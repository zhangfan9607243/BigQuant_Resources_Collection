import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.FACT.VPTA.VPTA import *

class CNDT_FACT_VPTA_FNAD1(VPTA):
    
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
            FROM cn_fund_bar1d
        ),
        data_alpha AS (
            {sql_alpha}
        ),
        data_security AS (
            SELECT
                date,
                instrument,
            FROM cn_fund_bar1d
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
