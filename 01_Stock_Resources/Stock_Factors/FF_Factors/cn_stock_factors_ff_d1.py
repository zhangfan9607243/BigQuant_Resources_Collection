import dai
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression

class CNStockFactorsFFD1:

    def __init__(self, sd, ed):
        self.sd = sd
        self.ed = ed

    def ff3_analysis(self, df, n):

        results = []

        for instrument, group in df.groupby('instrument'):

            group = group.sort_values('date') 

            beta_ICP  = []
            beta_MKT  = []
            beta_SMB  = []
            beta_HML  = []
            alpha     = []
            regr_resd = []
            regr_pred = []
            regr_r2   = []


            if len(group) < n:
                group['beta_ICP']  = [None] * len(group)
                group['beta_MKT']  = [None] * len(group)
                group['beta_SMB']  = [None] * len(group)
                group['beta_HML']  = [None] * len(group)
                group['alpha']     = [None] * len(group)
                group['regr_resd'] = [None] * len(group)
                group['regr_pred'] = [None] * len(group)
                group['regr_r2']   = [None] * len(group)
            
            else:

                for i in range(len(group) - n + 1):

                    window = group.iloc[i:i+n]
                    y = window['Return'].values
                    X = window[['MKT', 'SMB', 'HML']].values

                    model = LinearRegression()
                    model.fit(X, y)

                    beta_ICP.append(model.intercept_)
                    beta_MKT.append(model.coef_[0])
                    beta_SMB.append(model.coef_[1])
                    beta_HML.append(model.coef_[2])
                    alpha.append(model.intercept_ + y[-1] - model.predict(X)[-1])
                    regr_pred.append(model.predict(X)[-1]) 
                    regr_resd.append(y[-1] - model.predict(X)[-1]) 
                    regr_r2.append(model.score(X, y))

                beta_ICP  = [None] * (n - 1) + beta_ICP
                beta_MKT  = [None] * (n - 1) + beta_MKT
                beta_SMB  = [None] * (n - 1) + beta_SMB
                beta_HML  = [None] * (n - 1) + beta_HML
                alpha     = [None] * (n - 1) + alpha
                regr_resd = [None] * (n - 1) + regr_resd
                regr_pred = [None] * (n - 1) + regr_pred
                regr_r2   = [None] * (n - 1) + regr_r2

                group['beta_ICP']  = beta_ICP
                group['beta_MKT']  = beta_MKT
                group['beta_SMB']  = beta_SMB
                group['beta_HML']  = beta_HML
                group['alpha']     = alpha
                group['regr_resd'] = regr_resd
                group['regr_pred'] = regr_pred
                group['regr_r2']   = regr_r2

            results.append(group)

        result_df = pd.concat(results).sort_index()
        return result_df

    def ff3_get_sql_str(self, sql_base):
        sql = f"""
        WITH 
        data_base AS (
            {sql_base}
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
        return sql

    def ff3_factors_d1(self):
        sql_base = """
        SELECT
            date,
            instrument,
            change_ratio,
            float_market_cap,
            1 / pb AS bp_ratio,
        FROM cn_stock_prefactors
        WHERE instrument NOT LIKE '%BJ%'
        """
        return dai.query(self.ff3_get_sql_str(sql_base), filters={'date': [self.sd, self.ed]}).df()

    def ff3_factors_w1(self):
        sql_base = """
        WITH 
        data_orgn AS (
            SELECT
                date,
                instrument,
                close,
                float_market_cap,
                total_market_cap,
                total_market_cap / pb AS total_book_value
            FROM cn_stock_prefactors
        ),
        data_freq AS (
            WITH 
            data1 AS (
                SELECT
                    DATE_TRUNC('week', date) AS period,
                    instrument,
                    LAST_VALUE(close)            OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                    LAST_VALUE(float_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS float_market_cap,
                    LAST_VALUE(total_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_market_cap,
                    LAST_VALUE(total_book_value) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_book_value,
                    ROW_NUMBER()                 OVER (PARTITION BY period, instrument ORDER BY date DESC) AS rn
                FROM data_orgn
            ),
            data2 AS (
                SELECT
                    period,
                    instrument,
                    close,
                    LAG(close) OVER (PARTITION BY instrument ORDER BY period) AS close_m_lag_1,
                    float_market_cap,
                    total_market_cap,
                    total_book_value
                FROM data1
                WHERE rn = 1 
            )
            SELECT 
                period AS date,
                instrument,
                close / close_m_lag_1 - 1 AS change_ratio,
                float_market_cap,
                total_book_value / total_market_cap AS bp_ratio
            FROM data2
        )
        SELECT
            date,
            instrument,
            change_ratio,
            float_market_cap,
            bp_ratio,
        FROM data_freq
        WHERE instrument NOT LIKE '%BJ%'
        ORDER BY date, instrument
        """
        return dai.query(self.ff3_get_sql_str(sql_base), filters={'date': [self.sd, self.ed]}).df()

    def ff3_factors_m1(self):
        sql_base = """
        WITH 
        data_orgn AS (
            SELECT
                date,
                instrument,
                close,
                float_market_cap,
                total_market_cap,
                total_market_cap / pb AS total_book_value
            FROM cn_stock_prefactors
        ),
        data_freq AS (
            WITH 
            data1 AS (
                SELECT
                    DATE_TRUNC('month', date) AS period,
                    instrument,
                    LAST_VALUE(close)            OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                    LAST_VALUE(float_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS float_market_cap,
                    LAST_VALUE(total_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_market_cap,
                    LAST_VALUE(total_book_value) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_book_value,
                    ROW_NUMBER()                 OVER (PARTITION BY period, instrument ORDER BY date DESC) AS rn
                FROM data_orgn
            ),
            data2 AS (
                SELECT
                    period,
                    instrument,
                    close,
                    LAG(close) OVER (PARTITION BY instrument ORDER BY period) AS close_m_lag_1,
                    float_market_cap,
                    total_market_cap,
                    total_book_value
                FROM data1
                WHERE rn = 1 
            )
            SELECT 
                period AS date,
                instrument,
                close / close_m_lag_1 - 1 AS change_ratio,
                float_market_cap,
                total_book_value / total_market_cap AS bp_ratio
            FROM data2
        )
        SELECT
            date,
            instrument,
            change_ratio,
            float_market_cap,
            bp_ratio,
        FROM data_freq
        WHERE instrument NOT LIKE '%BJ%'
        ORDER BY date, instrument
        """
        return dai.query(self.ff3_get_sql_str(sql_base), filters={'date': [self.sd, self.ed]}).df()

    def ff3_factors_q1(self):
        sql_base = """
        WITH 
        data_orgn AS (
            SELECT
                date,
                instrument,
                close,
                float_market_cap,
                total_market_cap,
                total_market_cap / pb AS total_book_value
            FROM cn_stock_prefactors
        ),
        data_freq AS (
            WITH 
            data1 AS (
                SELECT
                    DATE_TRUNC('quarter', date) AS period,
                    instrument,
                    LAST_VALUE(close)            OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                    LAST_VALUE(float_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS float_market_cap,
                    LAST_VALUE(total_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_market_cap,
                    LAST_VALUE(total_book_value) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_book_value,
                    ROW_NUMBER()                 OVER (PARTITION BY period, instrument ORDER BY date DESC) AS rn
                FROM data_orgn
            ),
            data2 AS (
                SELECT
                    period,
                    instrument,
                    close,
                    LAG(close) OVER (PARTITION BY instrument ORDER BY period) AS close_m_lag_1,
                    float_market_cap,
                    total_market_cap,
                    total_book_value
                FROM data1
                WHERE rn = 1 
            )
            SELECT 
                period AS date,
                instrument,
                close / close_m_lag_1 - 1 AS change_ratio,
                float_market_cap,
                total_book_value / total_market_cap AS bp_ratio
            FROM data2
        )
        SELECT
            date,
            instrument,
            change_ratio,
            float_market_cap,
            bp_ratio,
        FROM data_freq
        WHERE instrument NOT LIKE '%BJ%'
        ORDER BY date, instrument
        """
        return dai.query(self.ff3_get_sql_str(sql_base), filters={'date': [self.sd, self.ed]}).df()

    def ff3_factors_y1(self):
        sql_base = """
        WITH 
        data_orgn AS (
            SELECT
                date,
                instrument,
                close,
                float_market_cap,
                total_market_cap,
                total_market_cap / pb AS total_book_value
            FROM cn_stock_prefactors
        ),
        data_freq AS (
            WITH 
            data1 AS (
                SELECT
                    DATE_TRUNC('year', date) AS period,
                    instrument,
                    LAST_VALUE(close)            OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                    LAST_VALUE(float_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS float_market_cap,
                    LAST_VALUE(total_market_cap) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_market_cap,
                    LAST_VALUE(total_book_value) OVER (PARTITION BY period, instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_book_value,
                    ROW_NUMBER()                 OVER (PARTITION BY period, instrument ORDER BY date DESC) AS rn
                FROM data_orgn
            ),
            data2 AS (
                SELECT
                    period,
                    instrument,
                    close,
                    LAG(close) OVER (PARTITION BY instrument ORDER BY period) AS close_m_lag_1,
                    float_market_cap,
                    total_market_cap,
                    total_book_value
                FROM data1
                WHERE rn = 1 
            )
            SELECT 
                period AS date,
                instrument,
                close / close_m_lag_1 - 1 AS change_ratio,
                float_market_cap,
                total_book_value / total_market_cap AS bp_ratio
            FROM data2
        )
        SELECT
            date,
            instrument,
            change_ratio,
            float_market_cap,
            bp_ratio,
        FROM data_freq
        WHERE instrument NOT LIKE '%BJ%'
        ORDER BY date, instrument
        """
        return dai.query(self.ff3_get_sql_str(sql_base), filters={'date': [self.sd, self.ed]}).df()
