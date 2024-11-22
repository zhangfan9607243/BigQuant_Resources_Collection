import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.DATA.DATA import *

class DATE(DATA):

    def __init__(self):
        pass

    def process_date_day(self, sql_date_day):
        
        df = dai.query(sql_date_day).df()

        df["dt_year"]      = df["date"].dt.year.astype(str)
        df["dt_quarter"]   = df["date"].dt.to_period("Q").apply(lambda x: f"{x.year}{x.quarter:02}")
        df["dt_month"]     = df["date"].dt.to_period("M").apply(lambda x: f"{x.year}{x.month:02}")
        df["dt_fortnight"] = df["date"].apply(lambda x: f"{x.year}{((x.dayofyear - 1) // 14) + 1:02}")
        df["dt_week"]      = df["date"].dt.strftime('%Y%U')
        df["dt_day"]       = df["date"].dt.strftime("%Y%m%d")
        
        df["is_year_start_trade"]      = (df["dt_year"] != df["dt_year"].shift(1)).astype(int)
        df["is_year_end_trade"]        = (df["dt_year"] != df["dt_year"].shift(-1)).astype(int)

        df["is_quarter_start_trade"]   = (df["dt_quarter"] != df["dt_quarter"].shift(1)).astype(int)
        df["is_quarter_end_trade"]     = (df["dt_quarter"] != df["dt_quarter"].shift(-1)).astype(int)

        df["is_month_start_trade"]     = (df["dt_month"] != df["dt_month"].shift(1)).astype(int)
        df["is_month_end_trade"]       = (df["dt_month"] != df["dt_month"].shift(-1)).astype(int)

        df["is_fortnight_start_trade"] = (df["dt_fortnight"] != df["dt_fortnight"].shift(1)).astype(int)
        df["is_fortnight_end_trade"]   = (df["dt_fortnight"] != df["dt_fortnight"].shift(-1)).astype(int)

        df["is_week_start_trade"]      = (df["dt_week"] != df["dt_week"].shift(1)).astype(int)
        df["is_week_end_trade"]        = (df["dt_week"] != df["dt_week"].shift(-1)).astype(int)

        df["is_year_start_order"]      = df["is_year_start_trade"].shift(-1, fill_value=0)
        df["is_year_end_order"]        = df["is_year_end_trade"].shift(-1, fill_value=0)

        df["is_quarter_start_order"]   = df["is_quarter_start_trade"].shift(-1, fill_value=0)
        df["is_quarter_end_order"]     = df["is_quarter_end_trade"].shift(-1, fill_value=0)

        df["is_month_start_order"]     = df["is_month_start_trade"].shift(-1, fill_value=0)
        df["is_month_end_order"]       = df["is_month_end_trade"].shift(-1, fill_value=0)

        df["is_fortnight_start_order"] = df["is_fortnight_start_trade"].shift(-1, fill_value=0)
        df["is_fortnight_end_order"]   = df["is_fortnight_end_trade"].shift(-1, fill_value=0)

        df["is_week_start_order"]      = df["is_week_start_trade"].shift(-1, fill_value=0)
        df["is_week_end_order"]        = df["is_week_end_trade"].shift(-1, fill_value=0)
        
        df['day_of_quarter_nature_trade']   = df['date'].apply(lambda x: (x - x.to_period('Q').start_time).days + 1)
        df['day_of_year_nature_trade']      = df['date'].apply(lambda x: (x - x.to_period('Y').start_time).days + 1)
        df['day_of_month_nature_trade']     = df['date'].apply(lambda x: x.day)
        df['day_of_fortnight_nature_trade'] = df['date'].apply(lambda x: ((x.dayofyear - 1) % 14) + 1)
        df['day_of_week_nature_trade']      = df['date'].dt.dayofweek + 1 

        df['day_of_quarter_nature_order']   = df['day_of_quarter_nature_trade'].shift(-1, fill_value=0)
        df['day_of_year_nature_order']      = df['day_of_year_nature_trade'].shift(-1, fill_value=0)
        df['day_of_month_nature_order']     = df['day_of_month_nature_trade'].shift(-1, fill_value=0)
        df['day_of_fortnight_nature_order'] = df['day_of_fortnight_nature_trade'].shift(-1, fill_value=0)

        df["day_of_year_trading_trade"]      = df.groupby("dt_year").cumcount() + 1
        df["day_of_quarter_trading_trade"]   = df.groupby("dt_quarter").cumcount() + 1
        df["day_of_month_trading_trade"]     = df.groupby("dt_month").cumcount() + 1
        df["day_of_fortnight_trading_trade"] = df.groupby("dt_fortnight").cumcount() + 1
        df["day_of_week_trading_trade"]      = df.groupby("dt_week").cumcount() + 1

        df["day_of_year_trading_order"]      = df["day_of_year_trading_trade"].shift(-1, fill_value=0)
        df["day_of_quarter_trading_order"]   = df["day_of_quarter_trading_trade"].shift(-1, fill_value=0)
        df["day_of_month_trading_order"]     = df["day_of_month_trading_trade"].shift(-1, fill_value=0)
        df["day_of_fortnight_trading_order"] = df["day_of_fortnight_trading_trade"].shift(-1, fill_value=0)
        df["day_of_week_trading_order"]      = df["day_of_week_trading_trade"].shift(-1, fill_value=0)
        
        return df
    
    # TODO
    def process_date_minute_stock(self, sql_date_minute):

        df = dai.query(sql_date_minute).df()

        df["dt_12_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour // 12) * 12:02}")
        df["dt_08_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour //  8) *  8:02}")
        df["dt_06_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour //  6) *  6:02}")
        df["dt_04_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour //  4) *  4:02}")
        df["dt_03_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour //  3) *  3:02}")
        df["dt_02_hour"]   = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d')}{(x.hour //  2) *  2:02}")
        df["dt_01_hour"]   = df["date"].dt.strftime("%Y%m%d%H")
        df["dt_30_minute"] = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d%H')}{(x.minute // 30) * 30:02}")
        df["dt_20_minute"] = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d%H')}{(x.minute // 20) * 20:02}")
        df["dt_15_minute"] = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d%H')}{(x.minute // 15) * 15:02}")
        df["dt_10_minute"] = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d%H')}{(x.minute // 10) * 10:02}")
        df["dt_05_minute"] = df["date"].apply(lambda x: f"{x.strftime('%Y%m%d%H')}{(x.minute //  5) *  5:02}")
        df["dt_01_minute"] = df["date"].dt.strftime("%Y%m%d%H%M")
        
        return df
    
    def cn_general_trading_calendar_day(self):

        sql_date_day = """
        WITH 
        data_base AS (
            SELECT date
            FROM all_trading_days
            WHERE market_code = 'CN'
            QUALIFY date >= '1990-01-01'
        )
        SELECT 
            date,
            ROW_NUMBER(date) AS date_index_day,
        FROM data_base
        ORDER BY date
        """

        df = self.process_date_day(sql_date_day)
        
        # Save to DAI
        ds = dai.DataSource("mldt_cn_trading_calendar")
        def update_df(df_new):
            df_new = df
            return df_new
        ds.apply_bdb(update_df, as_type=pd.DataFrame)

        dai.DataSource.write_bdb(
            data=df,
            id="mldt_cn_trading_calendar",
            unique_together=["date"],
            indexes=["date"],
        )

        return df
