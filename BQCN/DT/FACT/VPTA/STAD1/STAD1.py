import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.FACT.VPTA.VPTA import *

class CNDT_FACT_VPTA_STAD1(VPTA):
    
    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)

    def get_alpha_sql(self, sql_alpha):
        sql_combine = f"""
        WITH 
        data_base AS (
            SELECT
                date,
                instrument,
                open,
                close,
                high,
                low,
                volume,
                amount,
            FROM cn_stock_bar1d
        ),
        data_alpha AS (
            {sql_alpha}
        ),
        data_security AS (
            SELECT
                date,
                instrument,
            FROM cn_stock_bar1d
        ),
        data_merge AS (
            SELECT *
            FROM data_security LEFT JOIN data_alpha USING (date, instrument)
        )
        SELECT *
        FROM data_merge
        ORDER BY date, instrument
        """
        return sql_combine

    def get_alpha_df(self, sql_alpha, lag):
        return self.get_dai_df(self.get_alpha_sql(sql_alpha), self.sd, self.ed, lag_day = lag)

    # 1. Moving Statistics
    # 1.1 Simple Statistics
    # 1.2 Regression

    # 2. Volume-Price Indicators
    # 2.1 Moving Indicators
    # 2.2 K Stick Indicators
    
    # 3. Technical Factors
    # 3.1 Price Indicators
    # 3.2 Volume Indicators
    # 3.3 Price-Volume Combined Indicators
    
    # 3.4 Security-Index Indicators
    # RPS
    def alpha_vpta_RPS(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT 
                date, 
                instrument, 
                (close - m_lag(close, {n})) / m_lag(close, {n}) AS stock_return
            FROM cn_stock_bar1d 
        ),
        data_index AS (
            SELECT
                date,
                (close - m_lag(close, {n})) / m_lag(close, {n}) AS index_return
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        ),
        data_merge AS (
            SELECT
                date,
                instrument,
                (stock_return / index_return) * 100 AS RPS,
            FROM data_stock JOIN data_index USING (date)
        )
        SELECT
            date,
            instrument,
            RPS AS alpha_vpta_RPS_RPS_{n},
        FROM data_merge
        """
        return self.get_alpha_df(sql_alpha, lag=n+15)
    
    # MSI
    def alpha_vpta_MSI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_avg(IF((stock_close >= stock_open AND index_close >= index_open) OR (stock_close < stock_open AND index_close < index_open), 1, 0), {n}) AS alpha_vpta_MSI_MSI_{n}
        FROM data_stock JOIN data_index USING (date)
        """
        
        return self.get_alpha_df(sql_alpha, lag=n+15)
    
    # PSI
    def alpha_vpta_PSI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_sum(IF(stock_close >= stock_open AND index_close < index_open, 1, 0), {n}) / m_sum(IF(index_close < index_open, 1, 0), {n}) AS alpha_vpta_PSI_PSI_{n},
        FROM data_stock JOIN data_index USING (date)
        """
        
        return self.get_alpha_df(sql_alpha, lag=n+15)
    
    # PWI
    def alpha_vpta_PWI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_sum(IF(stock_close >= stock_open AND index_close < index_open, 1, 0), {n}) / m_sum(IF(index_close > index_open, 1, 0), {n}) AS alpha_vpta_PWI_PWI_{n},
        FROM data_stock JOIN data_index USING (date)
        """
        return self.get_alpha_df(sql_alpha, lag=n+15)
