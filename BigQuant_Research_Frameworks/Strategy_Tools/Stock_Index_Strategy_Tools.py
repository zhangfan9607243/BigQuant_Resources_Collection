import dai
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from bigmodule import M
from typing import Literal

def backtest_index_strategy(df, index_code, capital_base, holding_days, is_trade_by_weight=True):

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

        index_base = set(context.get_account_positions().keys())
        
        market_bull_sig = df_now['market_bull_sig'].iloc[0]
        market_bear_sig = df_now['market_bear_sig'].iloc[0]

        # 卖出
        if market_bull_sig == 1 and index_code in index_base:
            context.order_target_percent(index_code, 0)

        # 买入目标持有列表中的股票
        if market_bear_sig == 1 and index_code not in index_base:
            context.order_target_percent(index_code, 1)

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
        order_price_field_buy="""open""",
        order_price_field_sell="""open""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()


def backtest_factor_strategy_with_index_timing(df, capital_base):

    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data[context.data["date"] == data.current_dt.strftime("%Y-%m-%d")]

        if df_now["market_bear_sig"].iloc[0] == 0 and df_now["instrument"].iloc[0] == "NA":
            return

        if df_now["market_bear_sig"].iloc[0] == 1:
            holding_instruments = list(context.get_account_positions().keys())
            for instrument in holding_instruments:
                context.order_target_percent(instrument, 0)
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
        order_price_field_buy="""open""",
        order_price_field_sell="""open""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()
