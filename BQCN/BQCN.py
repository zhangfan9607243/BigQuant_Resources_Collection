import dai
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

class BQCN:
    
    def __init__(self, sd, ed, instruments = []):
        self.sd = sd
        self.ed = ed
        self.instruments = instruments
    
    def get_dai_df(self, sql, sd, ed, lag_day=0, lag_min=0, lag_sec=0, lag_mis=0):
    
        sql_lag_day = lag_day + (lag_min // 240 + 1) + (lag_sec // 14400 + 1) + (lag_mis // 14400000 + 1)

        def get_sd_sql(sd, lag):
            trading_days = dai.query(f"SELECT date FROM all_trading_days WHERE market_code = 'CN' AND date >= '1990-01-01' ORDER BY date").df()['date']
            sd_datetime = pd.to_datetime(sd)
            if sd_datetime in trading_days.values:
                sd_new =sd
            else:
                sd_new = trading_days[trading_days < sd_datetime].max().strftime('%Y-%m-%d')
            sd_index = trading_days[trading_days == sd_new].index[0]
            sd_sql = trading_days.iloc[max(0, sd_index - lag)].strftime('%Y-%m-%d')
            return sd_sql
        
        sd_sql = get_sd_sql(sd, sql_lag_day)
        
        if self.instruments == []:
            dai_df = dai.query(sql, filters={'date':[sd_sql, ed]}).df()
        else:
            dai_df = dai.query(sql, filters={'date':[sd_sql, ed], 'instrument': self.instruments}).df()

        dai_df = dai_df[(dai_df['date'] >= pd.to_datetime(sd)) & (dai_df['date'] <= pd.to_datetime(ed))] 
        return dai_df.reset_index(drop=True)
    