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

def cn_cbond_strategy_signal_event_df(strategy_sql, sd, ed, instrument_list=[]):
    
    if instrument_list == []:
        str_ins = "1=1"
    else:
        str_ins = f"instrument IN {str(tuple(instrument_list))}"
    
    sql = f"""
    WITH 
    data_signal AS (
        {strategy_sql}
    )
    SELECT
        date,
        instrument,
        signal_buy,
        signal_sell,
    FROM data_signal
    WHERE (signal_buy = 1 OR signal_sell = 1)
    AND {str_ins}
    ORDER BY date, instrument
    """
    df = get_dai_df(sql, sd, ed)
    return df  


def cn_cbond_strategy_signal_event_backtest(df, capital_base, holding_days, is_trade_by_weight=True):

    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        context.holding_days = holding_days
        context.weight = 1 / len(df['instrument'].unique())

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]

        if len(df_now) == 0:
            return

        instruments_buy  = set(df_now[df_now["signal_buy"]  == 1]["instrument"])
        instruments_sell = set(df_now[df_now["signal_sell"] == 1]["instrument"])
        instruments_hold = set(context.get_account_positions().keys())

        for instrument in instruments_buy - instruments_hold:
            if is_trade_by_weight:
                context.order_target_percent(instrument, context.weight)
            else:
                context.order(instrument, 100)
        
        for instrument in instruments_sell:
            context.order_target_percent(instrument, 0)

        for instrument in instruments_hold:
            if (data.current_dt - context.get_position(instrument).last_sale_date).days >= context.holding_days:
                context.order_target_percent(instrument, 0)

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
