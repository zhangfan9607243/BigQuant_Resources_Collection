import dai
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

def get_sd_ed(sd, ed):

    if len(sd) == 10:
        timestamp = datetime.strptime(sd, '%Y-%m-%d')
        sd = timestamp.strftime('%Y-%m-%d 00:00:00.000')
    elif len(sd) == 23:
        timestamp = datetime.strptime(sd, '%Y-%m-%d %H:%M:%S.%f')
        sd = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        sd = "2024-01-01 00:00:00.000"
    
    if len(ed) == 10:
        timestamp = datetime.strptime(ed, '%Y-%m-%d')
        ed = timestamp.strftime('%Y-%m-%d 23:59:59.999')
    elif len(ed) == 23:
        timestamp = datetime.strptime(ed, '%Y-%m-%d %H:%M:%S.%f')
        ed = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        ed = "2024-06-30 23:59:59.999"
    
    return sd, ed

def get_dai_df(sql, sd, ed, lag_day=90, lag_min=0, lag_sec=0, lag_mis=0):
    
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
    
    if sql_lag_day < 9999:
        sd_sql = get_sd_sql(sd, sql_lag_day)
    else:
        sd_sql = '1990-01-01'
    
    dai_df = dai.query(sql, filters={'date':[sd_sql, ed]}).df()
    dai_df = dai_df[(dai_df['date'] >= pd.to_datetime(sd)) & (dai_df['date'] <= pd.to_datetime(ed))] 
    dai_df = dai_df.reset_index(drop=True)
    
    return dai_df
