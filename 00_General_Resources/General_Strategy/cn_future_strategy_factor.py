import dai
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from bigmodule import M
from typing import Literal

import sys
sys.path.append("/home/aiuser/work/userlib/BigQuant_Resources_Collection/BigQuant_Resources_Collection/00_General_Resources/General_Tool")
from cn_general_tool import *

def cn_future_strategy_factor_trade_sql(
    sql_factor, 
    number_security_long=10,
    number_security_short=10, 
    position_allocation:Literal["equal", "by_score", "by_score_rank"]="equal", 
    rebalance_period:Literal["day","week","month","quarter","year"]="day",
    ):
    
    map_position_long = {
        "equal":"1/SUM(1)",
        "by_score":"score_long/SUM(score_long)",
        "by_score_rank":"(1/score_rank_long)/SUM(1/score_rank_long)",
    }
    str_position_long = map_position_long[position_allocation]

    map_position_short = {
        "equal":"1/SUM(1)",
        "by_score":"score_short/SUM(score_short)",
        "by_score_rank":"(1/score_rank_short)/SUM(1/score_rank_short)",
    }
    str_position_short = map_position_short[position_allocation]

    map_rebalance = {
        "day":"1=1",
        "week":"is_week_end_trade = 1",
        "month":"is_month_end_trade = 1",
        "quarter":"is_quarter_end_trade = 1",
        "year":"is_year_end_trade = 1",
    }
    str_rebalance = map_rebalance[rebalance_period]

    sql_merge = f"""
    WITH
    data_alpha AS (
        {sql_factor}
    ),
    data_filter AS (
        SELECT
            date,
            instrument,
            1  * factor AS score_long,
            -1 * factor AS score_short,
            c_rank(-1 * factor) AS score_rank_long,
            c_rank( 1 * factor) AS score_rank_short,
            IF(score_rank_long  <= {number_security_long},  1, 0) AS trade_long,
            IF(score_rank_short <= {number_security_short}, 1, 0) AS trade_short,
        FROM data_alpha
        QUALIFY trade_long = 1 OR trade_short = 1
    ),
    data_date AS (
        SELECT
            date,
            instrument,
            score_long, 
            score_short, 
            score_rank_long, 
            score_rank_short,
            trade_long,
            trade_short,
            IF(trade_long  = 1, {str_position_long}  OVER (PARTITION BY date, trade_long),  0) AS position_long,
            IF(trade_short = 1, {str_position_short} OVER (PARTITION BY date, trade_short), 0) AS position_short,
        FROM data_filter JOIN mldt_cn_trading_calendar USING (date)
        WHERE {str_rebalance}
    )
    SELECT *
    FROM data_date
    ORDER BY date, score_long
    """
    return sql_merge

def cn_future_strategy_factor_backtest(df, capital_base):

    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        context.data_date = dai.query("SELECT date FROM mldt_cn_trading_calendar WHERE is_month_end_trade = 1").df()

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]
        if len(df_now) == 0:
            return

        from bigtrader.constant import OrderType
        from bigtrader.constant import Direction

        holding_instruments = list(context.get_account_positions().keys())

        for ins in holding_instruments:
            position_long  = context.get_position(ins, Direction.LONG)
            position_short = context.get_position(ins, Direction.SHORT)
            price = data.current(ins,"open")
            if (position_long.current_qty  != 0):
                context.sell_close(ins, position_long.avail_qty, price, order_type=OrderType.MARKET)
            if (position_short.current_qty != 0):
                context.buy_close(ins, position_short.avail_qty, price, order_type=OrderType.MARKET)

        df_now_long  = df_now[df_now['trade_long']  == 1]
        df_now_short = df_now[df_now['trade_short'] == 1]

        cash = context.portfolio.cash

        for i, x in df_now_long.iterrows():
            ins = x.instrument
            price = data.current(ins, "open")
            position = 0.0 if pd.isnull(x.position_long)  else float(x.position_long)  / 2
            volume = 1
            # volume = max(int(cash * position / price), 0) 
            context.buy_open(ins, volume, price, order_type=OrderType.MARKET)

        for i, x in df_now_short.iterrows():
            ins = x.instrument
            price = data.current(ins, "open")
            position = 0.0 if pd.isnull(x.position_short) else float(x.position_short) / 2
            volume = 1
            # volume = max(int(cash * position / price), 0)
            context.sell_open(ins, volume, price, order_type=OrderType.MARKET)

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
        
        capital_base = capital_base + random.uniform(0, 10),
        frequency="""daily""",
        product_type="""期货""",
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
