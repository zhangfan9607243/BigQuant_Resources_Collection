import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.FACT.A101.A101 import *

class CNDT_FACT_A101_STAD1(A101):
    
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
                amount,
                volume,
                close / m_lag(close, 1) - 1 AS returns,
                amount / volume AS vwap,
                m_nanavg(volume,   5) AS adv5,
                m_nanavg(volume,  10) AS adv10,
                m_nanavg(volume,  15) AS adv15,
                m_nanavg(volume,  20) AS adv20,
                m_nanavg(volume,  30) AS adv30,
                m_nanavg(volume,  40) AS adv40,
                m_nanavg(volume,  50) AS adv50,
                m_nanavg(volume,  60) AS adv60,
                m_nanavg(volume,  81) AS adv81,
                m_nanavg(volume, 120) AS adv120,
                m_nanavg(volume, 150) AS adv150,
                m_nanavg(volume, 180) AS adv180,
                float_market_cap,
                sw2021_level1 AS industry_level1_code,
                sw2021_level2 AS industry_level2_code,
                sw2021_level3 AS industry_level3_code,
            FROM cn_stock_prefactors 
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
