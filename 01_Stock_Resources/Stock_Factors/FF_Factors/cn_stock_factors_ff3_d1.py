import dai
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class CNStockFactorsFFD1:

    def __init__(self, sd, ed):
        self.sd = sd
        self.ed = ed
    
    def ff3_factors_d1(self):
        sql = """
        WITH 
        data_base AS (
            SELECT
                date,
                instrument,
                change_ratio,
                float_market_cap,
                1 / pb AS bp_ratio,
            FROM cn_stock_prefactors
            WHERE instrument NOT LIKE '%BJ%'
        ),
        data1 AS ( 
            SELECT 
                date, 
                instrument, 
                change_ratio, 
            FROM data_base 
        ), 
        data2 AS ( 
            SELECT DISTINCT
                date, 
                c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS MKT
            FROM data_base 
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
                FROM data_base
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
        data_merge AS ( 
            SELECT 
                date, 
                instrument, 
                change_ratio AS Return,
                MKT,
                SMB, 
                HML, 
            FROM data1 JOIN data2 USING (date) JOIN data3 USING (date)
            QUALIFY COLUMNS(*) IS NOT NULL
        )
        SELECT *
        FROM data_merge 
        ORDER BY date, instrument

        """
        return dai.query(sql, filters={'date': [self.sd, self.ed]}).df()
