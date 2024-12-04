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


def cn_fund_strategy_factor_trade_sql(
    sql_factor, 
    number_security=10, 
    position_allocation:Literal["equal", "by_score", "by_score_rank"]="equal", 
    rebalance_period:Literal["day","week","month","quarter","year"]="day",
    ):
    
    map_position = {
        "equal":"1/c_sum(1)",
        "by_score":"score/c_sum(score)",
        "by_score_rank":"(1/score_rank)/c_sum(1/score_rank)",
    }
    str_position = map_position[position_allocation]

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
            factor AS score,
            c_rank(-1 * factor) AS score_rank,
        FROM data_alpha
        QUALIFY score_rank <= {number_security}
    ),
    data_date AS (
        SELECT
            date,
            instrument,
            score, 
            score_rank, 
            {str_position} AS position, 
        FROM data_filter JOIN mldt_cn_trading_calendar USING (date)
        WHERE {str_rebalance} 
    )
    SELECT *
    FROM data_date
    ORDER BY date, score_rank
    """
    return sql_merge

def cn_fund_strategy_factor_backtest(df, capital_base):

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
        
        target_instruments  = list(df_now["instrument"])
        holding_instruments = list(context.get_account_positions().keys())

        for instrument in holding_instruments:
            if instrument not in target_instruments:
                context.order_target_percent(instrument, 0)
            
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
        order_price_field_buy="""close""",
        order_price_field_sell="""close""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()

def cn_stock_strategy_factor_market_neutral_backtest(df, capital_base):
    
    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        context.data_date = dai.query("SELECT date FROM mldt_cn_trading_calendar WHERE is_month_end_trade = 1").df()

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data_date[context.data_date["date"] == data.current_dt.strftime("%Y-%m-%d")]
        if len(df_now) == 0:
            return

        if True:
            from bigtrader.constant import OrderType
            from bigtrader.constant import Direction
            
            index_future = "IF8888.CFE"
            position_short = context.get_position(index_future, Direction.SHORT)
            price = data.current(index_future, "open")
            
            if (position_short.current_qty != 0):
                context.buy_close(index_future, position_short.avail_qty, price, order_type=OrderType.MARKET)
            
            context.sell_open(index_future, 1, price, order_type=OrderType.MARKET)


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
        order_price_field_sell="""close""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()
