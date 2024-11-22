import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.FACT.A191.A191 import *

class CNDT_FACT_A191_STAD1(A191):
    
    def __init__(self, sd, ed):
        super().__init__(sd, ed)

    def get_alpha_sql(self, sql_alpha):
        sql_combine = f"""
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
                    close / m_lag(close, 1) - 1 AS ret,
                    amount / volume AS vwap,
                    industry_level1_code,
                    industry_level2_code,
                    industry_level3_code,
                    IF(open<=m_lag(open, 1), 0, greatest(high-open, open-m_lag(open, 1))) AS DTM, 
                    IF(open>=m_lag(open, 1), 0, greatest(open-low,  open-m_lag(open, 1))) AS DBM,
                    m_lag(low, 1) - low   AS LD, 
                    high - m_lag(high, 1) AS HD, 
                    greatest(greatest(high-low, ABS(high-m_lag(close, 1))), ABS(low-m_lag(close, 1))) AS TR, 
                FROM cn_stock_bar1d JOIN cn_stock_industry_component USING (date, instrument)
                WHERE instrument NOT LIKE '%BJ%'
                AND industry = 'sw2021'
            ),
            data_b AS (
                WITH 
                data_be AS (
                    SELECT
                        date,
                        instrument,
                        close / m_lag(close, 1) - 1 AS change_ratio,
                        float_market_cap,
                        total_owner_equity_lf,
                    FROM cn_stock_bar1d JOIN cn_stock_valuation USING (date, instrument) JOIN cn_stock_factors_financial_items USING (date, instrument)
                ),
                data1 AS ( 
                    SELECT 
                        date, 
                        instrument, 
                        change_ratio, 
                    FROM data_be 
                ), 
                data2 AS ( 
                    SELECT 
                        date, 
                        instrument,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS MKT
                    FROM data_be 
                ), 
                data3 AS (
                    WITH 
                    data3_0 AS (
                        SELECT
                            date,
                            instrument,
                            change_ratio,
                            float_market_cap,
                            c_pct_rank(float_market_cap)                          AS rank_sb,
                            c_pct_rank(float_market_cap / total_owner_equity_lf)  AS rank_lmh,
                            CASE
                                WHEN rank_sb  < 0.5 THEN 1
                                ELSE 2
                            END AS group_sb,
                            CASE
                                WHEN rank_lmh < 0.3 THEN 1
                                WHEN rank_lmh > 0.7 THEN 3
                                ELSE 2
                            END AS group_lmh,
                        FROM data_be
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
                            data3_0.date,
                            data3_0.instrument,
                            (1/3) * (SL + SM + SH) - (1/3) * (BL + BM + BH) AS SMB,
                            (1/2) * (SH + BH)      - (1/2) * (SL + BL)      AS HML,
                        FROM data3_0
                        JOIN data3_sl USING (date)
                        JOIN data3_sm USING (date)
                        JOIN data3_sh USING (date)
                        JOIN data3_bl USING (date)
                        JOIN data3_bm USING (date)
                        JOIN data3_bh USING (date)
                    )
                    SELECT * FROM data3_merge
                )
                SELECT 
                    date, 
                    instrument, 
                    MKT,
                    SMB, 
                    HML, 
                FROM data1 JOIN data2 USING (date, instrument) JOIN data3 USING (date, instrument)
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
            FROM data_a JOIN data_b USING (date, instrument) JOIN data_c USING (date)
            QUALIFY COLUMNS(*) IS NOT NULL
        ),
        data_alpha AS (
            {sql_alpha}
        ),
        data_stock AS (
            SELECT
                date,
                instrument,
            FROM cn_stock_instruments
        ),
        data_merge AS (
            SELECT *
            FROM data_stock LEFT JOIN data_alpha USING (date, instrument)
        )
        SELECT *
        FROM data_merge
        ORDER BY date, instrument
        """
        return sql_combine

    def get_alpha_df(self, sql_alpha, lag):
        return self.get_dai_df(self.get_alpha_sql(sql_alpha), self.sd, self.ed, lag_day = lag)
