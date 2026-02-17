import dai
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from bigmodule import M
from typing import Literal

def stock_strategy_buy_hold_df(sd, ed, instrument_list):
    sql = f"""
    WITH
    data_buy AS (
        WITH
        data_min_date AS (
            SELECT
                date,
                instrument,
                MIN(date) OVER (PARTITION BY instrument) AS _min_date,
            FROM cn_stock_bar1d
            WHERE instrument IN {str(tuple(instrument_list))}
            QUALIFY (date = _min_date)
            ORDER by date, instrument
        )
        SELECT
            date,
            instrument,
            1 / SUM(1) OVER (PARTITION BY 1) AS position
        FROM data_min_date
    ),
    data_sell AS (
        SELECT
            date,
            instrument,
            MAX(date) OVER (PARTITION BY instrument) AS _max_date,
            0 AS position,
        FROM cn_stock_bar1d
        WHERE instrument IN {str(tuple(instrument_list))}
        QUALIFY (date = _max_date)
        ORDER by date, instrument
    )
    SELECT * 
    FROM data_buy 
    UNION
    SELECT * 
    FROM data_sell
    ORDER BY date, instrument
    """
    df = dai.query(sql, filters={'date':[sd, ed]}).df()
    return df

def backtest_buy_hold(df, capital_base):

    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]

        if len(df_now) == 0:
            return
        
        for i, x in df_now.iterrows():
            position = 0.0 if pd.isnull(x.position) else float(x.position)
            context.order_target_percent(x.instrument, position)

    def BigTrader_Handle_Trade(context, trade):
        pass

    def BigTrader_Handle_Order(context, order):
        pass

    def BigTrader_After_Trading(context, data):
        pass

    BigTrader = M.bigtrader.v34(
        
        data = df,
        
        start_date = """""",
        end_date   = """""",
        
        initialize           = BigTrader_Initialize,
        before_trading_start = BigTrader_Before_Trading,
        handle_tick          = BigTrader_Handle_Tick,
        handle_data          = BigTrader_Handle_Data,
        handle_trade         = BigTrader_Handle_Trade,
        handle_order         = BigTrader_Handle_Order,
        after_trading        = BigTrader_After_Trading,
        
        capital_base = capital_base  + random.uniform(0, 10),
        frequency="""daily""",
        product_type="""自动""",
        rebalance_period_type="""交易日""",
        rebalance_period_days="""1""",
        rebalance_period_roll_forward=True,
        backtest_engine_mode="""标准模式""",
        before_start_days=0,
        volume_limit=1,
        order_price_field_buy="""open""",
        order_price_field_sell="""open""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()
