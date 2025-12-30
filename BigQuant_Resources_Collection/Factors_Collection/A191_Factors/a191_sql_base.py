def get_a191_sql_base(sql_alpha):

    sql_base = f"""--sql
    WITH
    data_base AS (
        WITH
        data_a AS (
            SELECT

                date,
                instrument,
                open,
                close,
                high,
                low,
                amount,
                volume,

                close / m_lag(close, 1) - 1  AS ret,
                amount / volume              AS vwap,

                IF(open<=m_lag(open, 1), 0, greatest(high-open, open-m_lag(open, 1))) AS DTM, 
                IF(open>=m_lag(open, 1), 0, greatest(open-low,  open-m_lag(open, 1))) AS DBM,

                m_lag(low, 1) - low                                                               AS LD, 
                high - m_lag(high, 1)                                                             AS HD, 
                greatest(greatest(high-low, ABS(high-m_lag(close, 1))), ABS(low-m_lag(close, 1))) AS TR, 
            
            FROM cn_stock_bar1d
            WHERE instrument NOT LIKE '%BJ%'

        ),
        data_b AS (
            WITH 
            data1 AS (
                SELECT
                    date,
                    instrument,
                    change_ratio,
                    float_market_cap,
                    1 / pb AS bp_ratio,
                FROM cn_stock_prefactors
                WHERE instrument NOT LIKE '%BJ%'
            ),
            data2 AS ( 
                SELECT DISTINCT
                    date, 
                    c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS MKT
                FROM data1 
            ), 
            data3 AS (
                WITH 
                data3_0 AS (
                    SELECT
                        date,
                        instrument,
                        change_ratio,
                        float_market_cap,
                        c_pct_rank(float_market_cap) AS rank_sb,
                        c_pct_rank(bp_ratio)         AS rank_lmh,
                        CASE
                            WHEN rank_sb  < 0.5 THEN 1
                            ELSE 2
                        END AS group_sb,
                        CASE
                            WHEN rank_lmh < 0.3 THEN 1
                            WHEN rank_lmh > 0.7 THEN 3
                            ELSE 2
                        END AS group_lmh,
                    FROM data1
                ),
                data3_sl AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SL
                    FROM data3_0
                    WHERE group_sb = 1 AND group_lmh = 1
                ),
                data3_sm AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SM
                    FROM data3_0
                    WHERE group_sb = 1 AND group_lmh = 2
                ),
                data3_sh AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SH
                    FROM data3_0
                    WHERE group_sb = 1 AND group_lmh = 3
                ),
                data3_bl AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BL
                    FROM data3_0
                    WHERE group_sb = 2 AND group_lmh = 1
                ),
                data3_bm AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BM
                    FROM data3_0
                    WHERE group_sb = 2 AND group_lmh = 2
                ),
                data3_bh AS (
                    SELECT DISTINCT
                        date,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BH
                    FROM data3_0
                    WHERE group_sb = 2 AND group_lmh = 3
                ),
                data3_merge AS (
                    SELECT 
                        date,
                        (1/3) * (SL + SM + SH) - (1/3) * (BL + BM + BH) AS SMB,
                        (1/2) * (SH + BH)      - (1/2) * (SL + BL)      AS HML,
                    FROM data3_sl
                    JOIN data3_sm USING (date)
                    JOIN data3_sh USING (date)
                    JOIN data3_bl USING (date)
                    JOIN data3_bm USING (date)
                    JOIN data3_bh USING (date)
                )
                SELECT * 
                FROM data3_merge
            ),
            data4 AS ( 
                SELECT 
                    date, 
                    MKT,
                    SMB, 
                    HML, 
                FROM data2 JOIN data3 USING (date)
                QUALIFY COLUMNS(*) IS NOT NULL
            )
            SELECT *
            FROM data4
            ORDER BY date
        ),
        data_c AS (
            SELECT 
                date,
                close AS bm_close,
                open  AS bm_open,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000001.SH'
        )
        SELECT * 
        FROM data_a JOIN data_b USING (date) JOIN data_c USING (date)
        QUALIFY COLUMNS(*) IS NOT NULL
    ),
    data_factor AS (
        {sql_alpha}
    ),
    data_security AS (
        SELECT
            date,
            instrument,
        FROM cn_stock_instruments
    ),
    data_merge AS (
        SELECT *
        FROM data_security LEFT JOIN data_factor USING (date, instrument)
    )
    SELECT *
    FROM data_merge
    ORDER BY date, instrument
    """

    return sql_base
