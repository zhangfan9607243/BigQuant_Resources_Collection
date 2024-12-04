import dai
import pandas as pd
import numpy as np

import sys
sys.path.append("/home/aiuser/work/userlib/BigQuant_Resources_Collection/BigQuant_Resources_Collection/00_General_Resources/General_Tool")
from cn_general_tool import *

# Stock
def get_factor_df_stock(sql_alpha, sd, ed, lag):

    sql = f"""
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
            data0 AS (
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
                FROM data0 
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
                    FROM data0
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
    return get_dai_df(sql, sd, ed, lag_day=lag)

# Future
def get_factor_df_future(sql_alpha, sd, ed, lag):

    sql = f"""
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
            
            FROM cn_future_bar1d
            WHERE instrument NOT LIKE '%BJ%'

        ),
        data_b AS (
            WITH 
            data0 AS (
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
                FROM data0 
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
                    FROM data0
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
        FROM cn_future_instruments
    ),
    data_merge AS (
        SELECT *
        FROM data_security LEFT JOIN data_factor USING (date, instrument)
    )
    SELECT *
    FROM data_merge
    ORDER BY date, instrument
    """
    return get_dai_df(sql, sd, ed, lag_day=lag)

# Fund
def get_factor_df_fund(sql_alpha, sd, ed, lag):

    sql = f"""
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
            
            FROM cn_fund_bar1d
            WHERE instrument NOT LIKE '%BJ%'

        ),
        data_b AS (
            WITH 
            data0 AS (
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
                FROM data0 
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
                    FROM data0
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
        FROM cn_fund_instruments
    ),
    data_merge AS (
        SELECT *
        FROM data_security LEFT JOIN data_factor USING (date, instrument)
    )
    SELECT *
    FROM data_merge
    ORDER BY date, instrument
    """
    return get_dai_df(sql, sd, ed, lag_day=lag)

# CBond
def get_factor_df_cbond(sql_alpha, sd, ed, lag):

    sql = f"""
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
            
            FROM cn_cbond_bar1d
            WHERE instrument NOT LIKE '%BJ%'

        ),
        data_b AS (
            WITH 
            data0 AS (
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
                FROM data0 
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
                    FROM data0
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
        FROM cn_cbond_instruments
    ),
    data_merge AS (
        SELECT *
        FROM data_security LEFT JOIN data_factor USING (date, instrument)
    )
    SELECT *
    FROM data_merge
    ORDER BY date, instrument
    """
    return get_dai_df(sql, sd, ed, lag_day=lag)

# Factor Formula
def alpha_a191_f0001(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_corr(c_pct_rank(m_delta(log(volume), 1)), c_pct_rank(((close -open) / open)), 6)) 
        AS alpha_a191_f0001
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0002(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_delta((((close -low) -(high -close)) / (high -low)), 1)) 
        AS alpha_a191_f0002
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0003(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum((IF(close=m_lag(close,1),0,close-(IF(close>m_lag(close,1),least(low,m_lag(close,1)),greatest(high,m_lag(close,1)))))),6) 
        AS alpha_a191_f0003
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0004(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF((((m_sum(close, 8) / 8) + m_stddev(close, 8)) < (m_sum(close, 2) / 2)), (-1 * 1), (IF(((m_sum(close, 2) / 2) < ((m_sum(close, 8) / 8) - m_stddev(close, 8))), 1, (IF(((1 < (volume / m_avg(volume,20))) OR ((volume / m_avg(volume,20)) == 1)), 1, (-1 * 1))))))) 
        AS alpha_a191_f0004
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0005(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_max(m_corr(m_rank(volume, 5), m_rank(high, 5), 5), 3)) 
        AS alpha_a191_f0005
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0006(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(sign(m_delta((((open * 0.85) + (high * 0.15))), 4)))* -1) 
        AS alpha_a191_f0006
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0007(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank(m_max((vwap -close), 3)) + c_pct_rank(m_min((vwap -close), 3))) * c_pct_rank(m_delta(volume, 3))) 
        AS alpha_a191_f0007
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0008(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        c_pct_rank(m_delta(((((high + low) / 2) * 0.2) + (vwap * 0.8)), 4) * -1) 
        AS alpha_a191_f0008
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0009(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(((high+low)/2-(m_lag(high,1)+m_lag(low,1))/2)*(high-low)/volume,7,2) 
        AS alpha_a191_f0009
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0010(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_max(IF(ret < 0, m_stddev(ret, 20), close)^2, 5))) 
        AS alpha_a191_f0010
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0011(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(((close-low)-(high-close))/(high-low)*volume,6) 
        AS alpha_a191_f0011
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0012(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank((open -(m_sum(vwap, 10) / 10)))) * (-1 * (c_pct_rank(abs((close -vwap))))) 
        AS alpha_a191_f0012
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0013(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (((high * low)^0.5) -vwap) 
        AS alpha_a191_f0013
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0014(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        close-m_lag(close,5) 
        AS alpha_a191_f0014
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0015(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        open/m_lag(close,1)-1 
        AS alpha_a191_f0015
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0016(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_max(c_pct_rank(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 5)), 5)) 
        AS alpha_a191_f0016
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0017(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        c_pct_rank((vwap - m_max(vwap, 15)))^m_delta(close, 5) 
        AS alpha_a191_f0017
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0018(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        close/m_lag(close,5) 
        AS alpha_a191_f0018
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0019(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF(close < m_lag(close, 5), (close - m_lag(close, 5)) / m_lag(close, 5), IF(close = m_lag(close, 5), 0, (close - m_lag(close, 5)) / close))) 
        AS alpha_a191_f0019
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0020(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_lag(close,6))/m_lag(close,6)*100 
        AS alpha_a191_f0020
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0021(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_regr_slope(m_avg(close,6), row_number(6), 6) 
        AS alpha_a191_f0021
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0022(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(((close-m_avg(close,6))/m_avg(close,6)-m_lag((close-m_avg(close,6))/m_avg(close,6),3)),12,1) 
        AS alpha_a191_f0022
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0023(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) / (m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) + m_ta_ewm(IF(close <= m_lag(close, 1), m_stddev(close, 20), 0), 20, 1))) * 100 
        AS alpha_a191_f0023
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0024(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(close-m_lag(close,5),5,1) 
        AS alpha_a191_f0024
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0025(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((-1 * c_pct_rank((m_delta(close, 7) * (1 -c_pct_rank(m_decay_linear((volume/m_avg(volume,20)), 9)))))) * (1 +c_pct_rank(m_sum(ret, 250)))) 
        AS alpha_a191_f0025
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=300)

def alpha_a191_f0026(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((((m_sum(close, 7) / 7) -close)) + ((m_corr(vwap, m_lag(close, 5), 230)))) AS alpha_a191_f0026
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=270)

def alpha_a191_f0027(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_wma((close-m_lag(close,3))/m_lag(close,3)*100+(close-m_lag(close,6))/m_lag(close,6)*100,12) 
        AS alpha_a191_f0027
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0028(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        3*m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1)-2*m_ta_ewm(m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_max(low,9))*100,3,1),3,1) 
        AS alpha_a191_f0028
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0029(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_lag(close,6))/m_lag(close,6)*volume 
        AS alpha_a191_f0029
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0030(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_wma((m_ols3d_last_resid(close/m_lag(close,1)-1,MKT,SMB,HML,60))^2,20) 
        AS alpha_a191_f0030
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0031(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_avg(close,12))/m_avg(close,12)*100 
        AS alpha_a191_f0031
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0032(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_sum(c_pct_rank(m_corr(c_pct_rank(high), c_pct_rank(volume), 3)), 3)) 
        AS alpha_a191_f0032
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0033(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((((-1 * m_min(low, 5)) + m_lag(m_min(low, 5), 5)) * c_pct_rank(((m_sum(ret, 240) -m_sum(ret, 20)) / 220))) *m_rank(volume, 5)) 
        AS alpha_a191_f0033
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=270)

def alpha_a191_f0034(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(close,12)/close AS alpha_a191_f0034
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0035(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (least(c_pct_rank(m_decay_linear(m_delta(open, 1), 15)), c_pct_rank(m_decay_linear(m_corr((volume), ((open * 0.65) +(open *0.35)), 17),7))) * -1) AS alpha_a191_f0035
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0036(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        c_pct_rank(m_sum(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 6), 2)) 
        AS alpha_a191_f0036
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0037(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * c_pct_rank(((m_sum(open, 5) * m_sum(ret, 5)) - m_lag((m_sum(open,5) * m_sum(ret, 5)), 10)))) 
        AS alpha_a191_f0037
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0038(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF(((m_sum(high, 20) / 20) < high), (-1 * m_delta(high, 2)), 0)) 
        AS alpha_a191_f0038
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0039(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank(m_decay_linear(m_delta((close), 2),8)) -c_pct_rank(m_decay_linear(m_corr(((vwap * 0.3) + (open * 0.7)),m_sum(m_avg(volume,180), 37), 14), 12))) * -1) 
        AS alpha_a191_f0039
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=270)

def alpha_a191_f0040(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_sum(IF(close > m_lag(close, 1), volume, 0), 26) / m_sum(IF(close <= m_lag(close, 1), volume, 0), 26)) * 100 
        AS alpha_a191_f0040
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0041(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_max(m_delta((vwap), 3), 5))* -1) 
        AS alpha_a191_f0041
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0042(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((-1 * c_pct_rank(m_stddev(high, 10))) * m_corr(high, volume, 10)) 
        AS alpha_a191_f0042
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0043(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close > m_lag(close, 1), volume, IF(close < m_lag(close, 1), -volume, 0)), 6) 
        AS alpha_a191_f0043
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0044(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_rank(m_decay_linear(m_corr(((low )), m_avg(volume,10), 7), 6),4) + m_rank(m_decay_linear(m_delta((vwap),3), 10), 15))  
        AS alpha_a191_f0044
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0045(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_delta((((close * 0.6) + (open *0.4))), 1)) * c_pct_rank(m_corr(vwap, m_avg(volume,150), 15))) 
        AS alpha_a191_f0045
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=180)

def alpha_a191_f0046(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_avg(close,3)+m_avg(close,6)+m_avg(close,12)+m_avg(close,24))/(4*close) 
        AS alpha_a191_f0046
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0047(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,9,1) 
        AS alpha_a191_f0047
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0048(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1*((c_pct_rank(((sign((close -m_lag(close, 1))) + sign((m_lag(close, 1) -m_lag(close, 2)))) +sign((m_lag(close, 2) -m_lag(close, 3)))))) * m_sum(volume, 5)) / m_sum(volume, 20)) 
        AS alpha_a191_f0048
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0049(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12)) 
        AS alpha_a191_f0049
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0050(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12))) - (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12))) 
        AS alpha_a191_f0050
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0051(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12)) AS alpha_a191_f0051
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0052(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(greatest(0,high-m_lag((high+low+close)/3,1)),26)/m_sum(greatest(0,m_lag((high+low+close)/3,1)-low),26)*100 
        AS alpha_a191_f0052
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0053(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close > m_lag(close, 1), 1, 0), 12) / 12 * 100 
        AS alpha_a191_f0053
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0054(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * c_pct_rank((m_stddev(abs(close -open), 10) + (close - open)) + m_corr(close, open, 10))) 
        AS alpha_a191_f0054
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0055(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(16*(close-m_lag(close,1)+(close-open)/2+m_lag(close,1)-m_lag(open,1))/(IF((abs(high-m_lag(close,1))>abs(low-m_lag(close,1)) AND abs(high-m_lag(close,1))>abs(high-m_lag(low,1))),abs(high-m_lag(close,1))+abs(low-m_lag(close,1))/2+abs(m_lag(close,1)-m_lag(open,1))/4,IF((abs(low-m_lag(close,1))>abs(high-m_lag(low,1)) AND abs(low-m_lag(close,1))>abs(high-m_lag(close,1))),abs(low-m_lag(close,1))+abs(high-m_lag(close,1))/2+abs(m_lag(close,1)-m_lag(open,1))/4,abs(high-m_lag(low,1))+abs(m_lag(close,1)-m_lag(open,1))/4)))*greatest(abs(high-m_lag(close,1)),abs(low-m_lag(close,1))),20) 
        AS alpha_a191_f0055
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0056(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF((c_pct_rank((open - m_min(open, 12))) < c_pct_rank((c_pct_rank(m_corr(m_sum(((high + low) / 2), 19), m_sum(m_avg(volume,40), 19), 13))^5))), 1, 0) * -1 AS alpha_a191_f0056
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0057(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1) 
        AS alpha_a191_f0057
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0058(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close > m_lag(close, 1), 1, 0), 20) / 20 * 100 
        AS alpha_a191_f0058
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0059(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum((IF(close=m_lag(close,1),0,close-(IF(close>m_lag(close,1),least(low,m_lag(close,1)),greatest(high,m_lag(close,1)))))),20) 
        AS alpha_a191_f0059
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0060(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(((close-low)-(high-close))/(high-low)*volume,20) 
        AS alpha_a191_f0060
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0061(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 1), 12)),c_pct_rank(m_decay_linear(c_pct_rank(m_corr((low),m_avg(volume,80), 8)), 17))) * -1) 
        AS alpha_a191_f0061
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0062(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_corr(high, c_pct_rank(volume), 5)) 
        AS alpha_a191_f0062
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0063(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(greatest(close-m_lag(close,1),0),6,1)/m_ta_ewm(abs(close-m_lag(close,1)),6,1)*100 
        AS alpha_a191_f0063
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0064(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (greatest(c_pct_rank(m_decay_linear(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 4), 4)),c_pct_rank(m_decay_linear(greatest(m_corr(c_pct_rank(close), c_pct_rank(m_avg(volume,60)), 4), 13), 14))) * -1) 
        AS alpha_a191_f0064
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0065(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(close,6)/close 
            AS alpha_a191_f0065
        FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0066(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_avg(close,6))/m_avg(close,6)*100 
        AS alpha_a191_f0066
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0067(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(greatest(close-m_lag(close,1),0),24,1)/m_ta_ewm(abs(close-m_lag(close,1)),24,1)*100 
        AS alpha_a191_f0067
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0068(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(((high+low)/2-(m_lag(high,1)+m_lag(low,1))/2)*(high-low)/volume,15,2) 
        AS alpha_a191_f0068
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0069(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF(m_sum(DTM,20)>m_sum(DBM,20),(m_sum(DTM,20)-m_sum(DBM,20))/m_sum(DTM,20),IF(m_sum(DTM,20)=m_sum(DBM,20),0,(m_sum(DTM,20)-m_sum(DBM,20))/m_sum(DBM,20)))) 
        AS alpha_a191_f0069
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0070(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_stddev(amount,6) 
        AS alpha_a191_f0070
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0071(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_avg(close,24))/m_avg(close,24)*100 
        AS alpha_a191_f0071
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0072(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,15,1) 
        AS alpha_a191_f0072
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0073(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((m_rank(m_decay_linear(m_decay_linear(m_corr((close), volume, 10), 16), 4), 5) - c_pct_rank(m_decay_linear(m_corr(vwap, m_avg(volume,30), 4),3))) * -1) 
        AS alpha_a191_f0073
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0074(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(m_sum(((low * 0.35) + (vwap * 0.65)), 20), m_sum(m_avg(volume,40), 20), 7)) + c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 6))) 
        AS alpha_a191_f0074
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0075(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close > open AND bm_close < bm_open, 1, 0), 50) / m_sum(IF(bm_close < bm_open, 1, 0), 50) 
        AS alpha_a191_f0075
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0076(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_stddev(abs((close/m_lag(close,1)-1))/volume,20)/m_avg(abs((close/m_lag(close,1)-1))/volume,20) 
        AS alpha_a191_f0076
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0077(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        least(c_pct_rank(m_decay_linear(((((high + low) / 2) + high) -(vwap + high)), 20)),c_pct_rank(m_decay_linear(m_corr(((high + low) / 2), m_avg(volume,40), 3), 6))) 
        AS alpha_a191_f0077
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0078(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((high+low+close)/3-m_avg((high+low+close)/3,12))/(0.015*m_avg(abs(close-m_avg((high+low+close)/3,12)),12)) 
        AS alpha_a191_f0078
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0079(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(greatest(close-m_lag(close,1),0),12,1)/m_ta_ewm(abs(close-m_lag(close,1)),12,1)*100 
        AS alpha_a191_f0079
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0080(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (volume-m_lag(volume,5))/m_lag(volume,5)*100 
        AS alpha_a191_f0080
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0081(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(volume,21,2) 
        AS alpha_a191_f0081
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0082(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,20,1) 
        AS alpha_a191_f0082
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0083(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * c_pct_rank(m_covar_samp(c_pct_rank(high), c_pct_rank(volume), 5))) 
        AS alpha_a191_f0083
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0084(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close>m_lag(close,1),volume,IF(close<m_lag(close,1),-volume,0)),20) 
        AS alpha_a191_f0084
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0085(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_rank((volume / m_avg(volume,20)), 20) * m_rank((-1 * m_delta(close, 7)), 8)) 
        AS alpha_a191_f0085
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0086(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF((0.25 < (((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10))), (-1 * 1), (IF((((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)) < 0, 1, ((-1 * 1) * (close - m_lag(close, 1))))))) 
        AS alpha_a191_f0086
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0087(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank(m_decay_linear(m_delta(vwap, 4), 7)) + m_pct_rank(m_decay_linear(((((low * 0.9) + (low * 0.1)) -vwap) /(open -((high + low) / 2))), 11), 7)) * -1) 
        AS alpha_a191_f0087
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0088(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_lag(close,20))/m_lag(close,20)*100 
        AS alpha_a191_f0088
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0089(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        2*(m_ta_ewm(close,13,2)-m_ta_ewm(close,27,2)-m_ta_ewm(m_ta_ewm(close,13,2)-m_ta_ewm(close,27,2),10,2)) 
        AS alpha_a191_f0089
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0090(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 5)) * -1) 
        AS alpha_a191_f0090
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0091(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank((close - m_max(close, 5)))*c_pct_rank(m_corr((m_avg(volume,40)), low, 5))) * -1) 
        AS alpha_a191_f0091
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0092(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (greatest(c_pct_rank(m_decay_linear(m_delta(((close * 0.35) + (vwap *0.65)), 2), 3)),m_rank(m_decay_linear(abs(m_corr((m_avg(volume,180)), close, 13)), 5), 15)) * -1) 
        AS alpha_a191_f0092
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=240)

def alpha_a191_f0093(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(open>=m_lag(open,1),0,greatest((open-low),(open-m_lag(open,1)))),20) 
        AS alpha_a191_f0093
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0094(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close > m_lag(close, 1), volume, IF(close < m_lag(close, 1), -volume, 0)), 30) 
        AS alpha_a191_f0094
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0095(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_stddev(amount,20) 
        AS alpha_a191_f0095
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0096(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1),3,1) 
        AS alpha_a191_f0096
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0097(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_stddev(volume,10) 
        AS alpha_a191_f0097
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0098(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (IF((((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) < 0.05) OR ((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) == 0.05)), (-1 * (close - m_min(close, 100))), (-1 * m_delta(close, 3)))) 
        AS alpha_a191_f0098
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=300)

def alpha_a191_f0099(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * c_pct_rank(m_covar_samp(c_pct_rank(close), c_pct_rank(volume), 5))) 
        AS alpha_a191_f0099
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0100(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_stddev(volume,20)  
        AS alpha_a191_f0100
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0101(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF(((c_pct_rank(m_corr(close, m_sum(m_avg(volume,30), 37), 15)) < c_pct_rank(m_corr(c_pct_rank(((high * 0.1) + (vwap * 0.9))),c_pct_rank(volume), 11)))),1,0) * -1 
        AS alpha_a191_f0101
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0102(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(greatest(volume-m_lag(volume,1),0),6,1)/m_ta_ewm(abs(volume-m_lag(volume,1)),6,1)*100 
        AS alpha_a191_f0102
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0103(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((20-m_imin(low,20))/20)*100 
        AS alpha_a191_f0103
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0104(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * (m_delta(m_corr(high, volume, 5), 5) * c_pct_rank(m_stddev(close, 20)))) 
        AS alpha_a191_f0104
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0105(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_corr(c_pct_rank(open), c_pct_rank(volume), 10)) 
        AS alpha_a191_f0105
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0106(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        close-m_lag(close,20) 
        AS alpha_a191_f0106
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0107(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (((-1 * c_pct_rank((open -m_lag(high, 1)))) * c_pct_rank((open -m_lag(close, 1)))) *c_pct_rank((open -m_lag(low, 1)))) 
        AS alpha_a191_f0107
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0108(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank((high - m_min(high, 2)))^c_pct_rank(m_corr((vwap), (m_avg(volume,120)), 6))) * -1) 
        AS alpha_a191_f0108
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=150)

def alpha_a191_f0109(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(high-low,10,2)/m_ta_ewm(m_ta_ewm(high-low,10,2),10,2) 
        AS alpha_a191_f0109
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0110(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(greatest(0,high-m_lag(close,1)),20)/m_sum(greatest(0,m_lag(close,1)-low),20)*100 
        AS alpha_a191_f0110
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0111(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(volume*((close-low)-(high-close))/(high-low),11,2)-m_ta_ewm(volume*((close-low)-(high-close))/(high-low),4,2) 
        AS alpha_a191_f0111
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0112(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((m_sum(IF(close-m_lag(close,1)>0,close-m_lag(close,1),0),12)-m_sum(IF(close-m_lag(close,1)<0,abs(close-m_lag(close,1)),0),12)) / (m_sum(IF(close-m_lag(close,1)>0,close-m_lag(close,1),0),12)+m_sum(IF(close-m_lag(close,1)<0,abs(close-m_lag(close,1)),0),12)))*100 
        AS alpha_a191_f0112
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0113(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * ((c_pct_rank((m_sum(m_lag(close, 5), 20) / 20)) * m_corr(close, volume, 2)) * c_pct_rank(m_corr(m_sum(close, 5),m_sum(close, 20), 2)))) 
        AS alpha_a191_f0113
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0114(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank(m_lag(((high -low) / (m_sum(close, 5) / 5)), 2)) * c_pct_rank(c_pct_rank(volume))) / (((high -low) /(m_sum(close, 5) / 5)) / (vwap -close))) 
        AS alpha_a191_f0114
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0115(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(((high * 0.9) + (close * 0.1)), m_avg(volume,30), 10))^c_pct_rank(m_corr(m_rank(((high + low) /2), 4), m_rank(volume, 10), 7))) 
        AS alpha_a191_f0115
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0116(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_regr_slope(close,row_number(20),20) 
        AS alpha_a191_f0116
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0117(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((m_rank(volume, 32) * (1 -m_rank(((close + high) -low), 16))) * (1 -m_rank(ret, 32))) 
        AS alpha_a191_f0117
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0118(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(high-open,20)/m_sum(open-low,20)*100 
        AS alpha_a191_f0118
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0119(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_decay_linear(m_corr(vwap, m_sum(m_avg(volume,5), 26), 5), 7)) -c_pct_rank(m_decay_linear(m_rank(m_min(m_corr(c_pct_rank(open), c_pct_rank(m_avg(volume,15)), 21), 9), 7), 8))) 
        AS alpha_a191_f0119
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0120(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank((vwap -close)) / c_pct_rank((vwap + close))) 
        AS alpha_a191_f0120
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0121(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank((vwap - m_min(vwap, 12)))^m_rank(m_corr(m_rank(vwap, 20), m_rank(m_avg(volume,60), 2), 18), 3)) * -1) 
        AS alpha_a191_f0121
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0122(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2)-m_lag(m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2),1))/m_lag(m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2),1) 
        AS alpha_a191_f0122
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0123(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF(((c_pct_rank(m_corr(m_sum(((high + low) / 2), 20), m_sum(m_avg(volume,60), 20), 9))<c_pct_rank(m_corr(low, volume,6)))), 1, 0) * -1 
        AS alpha_a191_f0123
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0124(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close -vwap) / m_decay_linear(c_pct_rank(m_max(close, 30)),2) 
        AS alpha_a191_f0124
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0125(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_decay_linear(m_corr((vwap), m_avg(volume,80),17), 20)) / c_pct_rank(m_decay_linear(m_delta(((close * 0.5)+ (vwap * 0.5)), 3), 16))) 
        AS alpha_a191_f0125
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0126(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close+high+low)/3 
        AS alpha_a191_f0126
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0127(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_avg((100*(close-m_max(close,12))/(m_max(close,12)))^2, 12))^(1/2) 
        AS alpha_a191_f0127
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0128(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        100 - (100 / (1 + m_sum(IF(((high+low+close)/3 > m_lag((high+low+close)/3,1)), ((high+low+close)/3 * volume), 0), 14) / m_sum(IF(((high+low+close)/3 < m_lag((high+low+close)/3,1)), ((high+low+close)/3 * volume), 0), 14))) 
        AS alpha_a191_f0128
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0129(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close - m_lag(close, 1) < 0, abs(close - m_lag(close, 1)), 0), 12) 
        AS alpha_a191_f0129
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0130(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(close, m_avg(volume, 60), 10)) - c_pct_rank(m_corr(close, m_avg(volume, 10), 6))) 
        AS alpha_a191_f0130
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0131(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_delta(vwap, 1))^m_rank(m_corr(close,m_avg(volume,50), 18), 18)) 
        AS alpha_a191_f0131
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0132(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(amount, 20) 
        AS alpha_a191_f0132
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0133(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((20 - m_imax(high, 20)) / 20) * 100 - ((20 - m_imin(low, 20)) / 20) * 100 
        AS alpha_a191_f0133
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0134(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close - m_lag(close, 12)) / m_lag(close, 12) * volume 
        AS alpha_a191_f0134
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0135(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(m_lag(close / m_lag(close, 20), 1), 20, 1) 
        AS alpha_a191_f0135
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0136(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((-1 * c_pct_rank(m_delta(ret, 3))) * m_corr(open, volume, 10)) 
        AS alpha_a191_f0136
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0137(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        16 * (((close - m_lag(close, 1) + (close - open) / 2 + m_lag(close, 1) - m_lag(open, 1)) / (IF((abs(high - m_lag(close, 1)) > abs(low - m_lag(close, 1)) AND abs(high - m_lag(close, 1)) > abs(high - m_lag(low, 1))), (abs(high - m_lag(close, 1)) + abs(low - m_lag(close, 1)) / 2 + abs(m_lag(close, 1) - m_lag(open, 1)) / 4), IF((abs(low - m_lag(close, 1)) > abs(high - m_lag(low, 1)) AND abs(low - m_lag(close, 1)) > abs(high - m_lag(close, 1))), (abs(low - m_lag(close, 1)) + abs(high - m_lag(close, 1)) / 2 + abs(m_lag(close, 1) - m_lag(open, 1)) / 4), (abs(high - m_lag(low, 1)) + abs(m_lag(close, 1) - m_lag(open, 1)) / 4))))) * greatest(abs(high - m_lag(close, 1)), abs(low - m_lag(close, 1)))) 
        AS alpha_a191_f0137
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0138(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((c_pct_rank(m_decay_linear(m_delta((((low * 0.7) + (vwap * 0.3))), 3), 20)) - m_rank(m_decay_linear(m_rank(m_corr(m_rank(low, 8), m_rank(m_avg(volume, 60), 17), 5), 19), 16), 7)) * -1)
        AS alpha_a191_f0138
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0139(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * m_corr(open, volume, 10)) 
        AS alpha_a191_f0139
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0140(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        least(c_pct_rank(m_decay_linear(((c_pct_rank(open) + c_pct_rank(low)) -(c_pct_rank(high) + c_pct_rank(close))), 8)),m_rank(m_decay_linear(m_corr(m_rank(close, 8), m_rank(m_avg(volume,60), 20), 8), 7), 3)) 
        AS alpha_a191_f0140
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=120)

def alpha_a191_f0141(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(c_pct_rank(high), c_pct_rank(m_avg(volume, 15)), 9)) * -1)
        AS alpha_a191_f0141
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0142(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (((-1 * c_pct_rank(m_rank(close, 10))) * c_pct_rank(m_delta(m_delta(close, 1), 1))) * c_pct_rank(m_rank((volume / m_avg(volume, 20)), 5))) 
        AS alpha_a191_f0142
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0143(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_product(IF(close / m_lag(close, 1) > 1, close / m_lag(close, 1), 1), 252) 
        AS alpha_a191_f0143
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=300)

def alpha_a191_f0144(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close < m_lag(close, 1), abs(close / m_lag(close, 1) - 1) / amount, 0), 20) / m_sum(IF(close < m_lag(close, 1), 1, 0), 20) 
        AS alpha_a191_f0144
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0145(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_avg(volume, 9) - m_avg(volume, 26)) / m_avg(volume, 12) * 100 
        AS alpha_a191_f0145
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0146(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2), 20) * ((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2)) / m_ta_ewm(((close - m_lag(close, 1)) / m_lag(close, 1) - ((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2))) ^ 2, 60, 2) 
        AS alpha_a191_f0146
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0147(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_regr_slope(m_avg(close, 12), row_number(12), 12) 
        AS alpha_a191_f0147
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0148(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF(((c_pct_rank(m_corr((open), m_sum(m_avg(volume, 60), 9), 6)) < c_pct_rank((open - m_min(open, 14))))), 1, 0) * -1 
        AS alpha_a191_f0148
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0149(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_regr_slope((close / m_lag(close, 1) - 1), (bm_close / m_lag(bm_close, 1) - 1), 252) 
        AS alpha_a191_f0149
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=300)

def alpha_a191_f0150(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close + high + low) / 3 * volume 
        AS alpha_a191_f0150
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0151(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(close - m_lag(close, 20), 20, 1) 
        AS alpha_a191_f0151
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0152(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(m_avg(m_lag(m_ta_ewm(m_lag(close / m_lag(close, 9), 1), 9, 1), 1), 12) - m_avg(m_lag(m_ta_ewm(m_lag(close / m_lag(close, 9), 1), 9, 1), 1), 26), 9, 1) 
        AS alpha_a191_f0152
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0153(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_avg(close, 3) + m_avg(close, 6) + m_avg(close, 12) + m_avg(close, 24)) / 4 
        AS alpha_a191_f0153
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0154(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF(((vwap - m_min(vwap, 16)) < (m_corr(vwap, m_avg(volume, 180), 18))), 1, 0) 
        AS alpha_a191_f0154
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=210)

def alpha_a191_f0155(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(volume, 13, 2) - m_ta_ewm(volume, 27, 2) - m_ta_ewm(m_ta_ewm(volume, 13, 2) - m_ta_ewm(volume, 27, 2), 10, 2) 
        AS alpha_a191_f0155
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0156(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 5), 3)), c_pct_rank(m_decay_linear(((m_delta(((open * 0.15) + (low * 0.85)), 2) / ((open * 0.15) + (low * 0.85))) * -1), 3))) * -1) 
        AS alpha_a191_f0156
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0157(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_min(m_product(c_pct_rank(c_pct_rank(log(m_sum(m_min(c_pct_rank(c_pct_rank((-1 * c_pct_rank(m_delta((close - 1), 5))))), 2), 1)))), 1), 5) + m_rank(m_lag((-1 * ret), 6), 5)) 
        AS alpha_a191_f0157
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0158(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((high - m_ta_ewm(close, 15, 2)) - (low - m_ta_ewm(close, 15, 2))) / close 
        AS alpha_a191_f0158
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0159(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((close - m_sum(least(low, m_lag(close, 1)), 6)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 6) * 12 * 24 + (close - m_sum(least(low, m_lag(close, 1)), 12)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 12) * 6 * 24 + (close - m_sum(least(low, m_lag(close, 1)), 24)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 24) * 6 * 24) * 100 / (6 * 12 + 6 * 24 + 12 * 24) 
        AS alpha_a191_f0159
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0160(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(IF(close <= m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) 
        AS alpha_a191_f0160
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0161(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(greatest(greatest((high - low), abs(m_lag(close, 1) - high)), abs(m_lag(close, 1) - low)), 12) 
        AS alpha_a191_f0161
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0162(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100 - m_min(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12)) / (m_max(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12) -  m_min(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12)) 
        AS alpha_a191_f0162
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0163(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        c_pct_rank(((((-1 * ret) * m_avg(volume, 20)) * vwap) * (high - close))) 
        AS alpha_a191_f0163
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0164(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm((IF((close > m_lag(close, 1)), 1 / (close - m_lag(close, 1)), 1) -  m_min(IF((close > m_lag(close, 1)), 1 / (close - m_lag(close, 1)), 1), 12)) / (high - low) * 100, 13, 2) 
        AS alpha_a191_f0164
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0165(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_max(c_sum(close - m_avg(close, 48)), 48) -  m_min(c_sum(close - m_avg(close, 48)), 48) / m_stddev(close, 48) 
        AS alpha_a191_f0165
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=180)

def alpha_a191_f0166(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        -20 * ((20 - 1) ^ 1.5 * m_sum(close / m_lag(close, 1) - 1 - m_avg(close / m_lag(close, 1) - 1, 20), 20)) / ((20 - 1) * (20 - 2) * (m_sum(m_avg(close / m_lag(close, 1), 20) ^ 2, 20)) ^ 1.5) 
        AS alpha_a191_f0166
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0167(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(close - m_lag(close, 1) > 0, close - m_lag(close, 1), 0), 12) 
        AS alpha_a191_f0167
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0168(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (-1 * volume / m_avg(volume, 20)) 
        AS alpha_a191_f0168
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0169(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(m_avg(m_lag(m_ta_ewm(close - m_lag(close, 1), 9, 1), 1), 12) - m_avg(m_lag(m_ta_ewm(close - m_lag(close, 1), 9, 1), 1), 26), 10, 1) 
        AS alpha_a191_f0169
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0170(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((((c_pct_rank((1 / close)) * volume) / m_avg(volume,20)) * ((high * c_pct_rank((high -close))) / (m_sum(high, 5) /5))) -c_pct_rank((vwap - m_lag(vwap, 5))))
        AS alpha_a191_f0170
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0171(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((-1 * ((low -close) * (open^5))) / ((close -high) * (close^5))) 
        AS alpha_a191_f0171
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0172(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(abs((m_sum(IF(LD > 0 AND LD > HD, LD, 0), 14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0), 14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0), 14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0), 14) * 100 / m_sum(TR, 14))) * 100, 6) 
        AS alpha_a191_f0172
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0173(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        3*m_ta_ewm(close,13,2)-2*m_ta_ewm(m_ta_ewm(close,13,2),13,2)+m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2) 
        AS alpha_a191_f0173
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0174(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) 
        AS alpha_a191_f0174
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0175(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(greatest(greatest((high-low),abs(m_lag(close,1)-high)),abs(m_lag(close,1)-low)),6) 
        AS alpha_a191_f0175
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0176(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_corr(c_pct_rank(((close -m_min(low, 12)) / (m_max(high, 12) - m_min(low,12)))),c_pct_rank(volume), 6) 
        AS alpha_a191_f0176
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0177(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((20-m_imax(high,20))/20)*100 
        AS alpha_a191_f0177
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0178(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (close-m_lag(close,1))/m_lag(close,1)*volume 
        AS alpha_a191_f0178
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0179(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(vwap, volume, 4)) * c_pct_rank(m_corr(c_pct_rank(low),c_pct_rank(m_avg(volume,50)), 12))) 
        AS alpha_a191_f0179
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0180(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        IF(m_avg(volume, 20) < volume, (-1 * m_rank(abs(m_delta(close, 7)), 60)) * sign(m_delta(close, 7)), -1 * volume) 
        AS alpha_a191_f0180
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0181(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(((close/m_lag(close,1)-1)-m_avg((close/m_lag(close,1)-1),20))-(bm_close-m_avg(bm_close,20))^2,20)/m_sum((bm_close-m_avg(bm_close,20))^3,20) 
        AS alpha_a191_f0181
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0182(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF((close > open AND bm_close > bm_open) OR (close < open AND bm_close < bm_open), 1, 0), 20) / 20 
        AS alpha_a191_f0182
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0183(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_max(c_sum(close-m_avg(close,24)),24)-m_min(c_sum(close-m_avg(close,24)),24)/m_stddev(close,24) 
        AS alpha_a191_f0183
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0184(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (c_pct_rank(m_corr(m_lag((open -close), 1), close, 200)) + c_pct_rank((open -close))) 
        AS alpha_a191_f0184
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=240)

def alpha_a191_f0185(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        c_pct_rank((-1 * ((1 -(open / close))^2))) 
        AS alpha_a191_f0185
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0186(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (m_lag(m_avg(abs(m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)),14) * 100, 6) + m_lag(m_avg(abs(m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)),14) * 100, 6)) / 2 
        AS alpha_a191_f0186
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=90)

def alpha_a191_f0187(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_sum(IF(open <= m_lag(open, 1), 0, greatest((high - open), (open - m_lag(open, 1)))), 20) 
        AS alpha_a191_f0187
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0188(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((high-low-m_ta_ewm(high-low,11,2))/m_ta_ewm(high-low,11,2))*100 
        AS alpha_a191_f0188
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0189(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        m_avg(abs(close-m_avg(close,6)),6) 
        AS alpha_a191_f0189
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=30)

def alpha_a191_f0190(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        (((m_sum(IF(close / m_lag(close,1) - 1 > ((close / m_lag(close, 19))^(1/20) - 1), 1, 0), 20) - 1) * (m_sum(IF(close / m_lag(close,1) - 1 < (close / m_lag(close, 19))^(1/20) - 1, (close / m_lag(close,1) - 1 - (close / m_lag(close, 19))^(1/20) - 1)^2, 0), 20))) / ((m_sum(IF(close / m_lag(close,1) - 1 < (close / m_lag(close, 19))^(1/20) - 1, 1, 0), 20)) * (m_sum(IF(close / m_lag(close,1) - 1 > (close / m_lag(close, 19))^(1/20) - 1, (close / m_lag(close,1) - 1 - (close / m_lag(close, 19))^(1/20) - 1)^2, 0), 20)))) 
        AS alpha_a191_f0190
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)

def alpha_a191_f0191(sd, ed, get_factor_df_function=get_factor_df_stock):
    sql_alpha = f"""
    SELECT
        date,
        instrument,
        ((m_corr(m_avg(volume,20), low, 5) + ((high + low) / 2)) -close) 
        AS alpha_a191_f0191
    FROM data_base
    """
    return get_factor_df_function(sql_alpha, sd, ed, lag=60)
