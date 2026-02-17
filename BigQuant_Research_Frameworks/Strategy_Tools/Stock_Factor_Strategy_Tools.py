import dai
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from bigmodule import M
from typing import Literal

# Factor Process
def process_factor_sql(sql_factor):
    sql = f"""
    WITH
    data_alpha AS (
        {sql_factor}
    ),
    data_alpha_origin AS (
        SELECT 
            date,
            instrument,
            factor,
        FROM data_alpha
        QUALIFY COLUMNS(*) IS NOT NULL AND factor != 'Infinity' AND factor != '-Infinity'
    ),
    data_alpha_filter AS (
        SELECT 
            date,
            instrument,
            factor,
        FROM data_alpha_origin JOIN cn_stock_prefactors USING (date, instrument)
        WHERE amount > 0
        AND is_risk_warning = 0
        AND name NOT LIKE '%ST%'
        AND name NOT LIKE '%退%'
        AND list_days > 365
        AND (instrument LIKE '%SH' OR instrument LIKE '%SZ')
        QUALIFY COLUMNS(*) IS NOT NULL
    ),
    data_alpha_process AS (
        SELECT 
            date,
            instrument,
            factor,
            clip(factor, c_avg(factor) - 3 * c_std(factor), c_avg(factor) + 3 * c_std(factor)) AS clipped_factor,
            c_normalize(clipped_factor) AS normalized_factor,
            c_neutralize(normalized_factor, sw2021_level1, LOG(total_market_cap)) AS neutralized_factor,
        FROM data_alpha_filter JOIN cn_stock_prefactors USING (date, instrument)
        ORDER BY date, instrument
    )
    SELECT 
        date, 
        instrument, 
        neutralized_factor AS factor 
    FROM data_alpha_process 
    ORDER BY date, factor DESC
    """
    return sql

# Synthesize SQL of Multiple Factos
def synthesize_multi_factor_sql(factos_dict):
    sql_list1 = []
    sql_list2 = []
    sql_list3 = []

    for i, (factor_name, factor_data) in enumerate(factos_dict.items(), start=1):
        sql_list1.append(f"""data_{factor_name} AS ({factor_data['sql']})""")
        sql_list2.append(f"c_normalize(data_{factor_name}.factor) * {factor_data['weight']}")
        if i == 1:
            sql_list3.append(f"FROM data_{factor_name}")
        else:
            sql_list3.append(f"JOIN data_{factor_name} USING (date, instrument)")

    sql_str1 = ", ".join(sql_list1) + ", "
    sql_str2 = " + ".join(sql_list2)
    sql_str3 = " ".join(sql_list3)

    sql_combine = f"""
    WITH
    {sql_str1}
    data_merge AS (
        SELECT
            date,
            instrument,
            {sql_str2} AS factor_new
        {sql_str3} 
    )
    SELECT
        date,
        instrument,
        factor_new AS factor
    FROM data_merge
    """
    return sql_combine

# Get the Trade SQL
def get_trade_sql(
    sql_factor, 
    number_security=10, 
    position_allocation:Literal["equal", "by_score", "by_score_rank", "by_market_cap"]="equal", 
    rebalance_period:Literal["day","week","month","quarter","year"]="day",
    stock_pool:Literal["all",'000001.SH','000985.CSI','000903.SH','399001.SZ','399330.SZ','000016.SH','000300.SH','000510.SH','000852.SH','000905.SH','399006.SZ','000688.SH','899050.BJ','000903.SH']="all"
    ):
    
    map_position = {
        "equal":"1/c_sum(1)",
        "by_score":"score/c_sum(score)",
        "by_score_rank":"(1/score_rank)/c_sum(1/score_rank)",
        "by_market_cap":"float_market_cap/c_sum(float_market_cap)",
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

    map_pool = {
        "all":      "1=1",
        "000300.SH":"is_hs300=1",   # 沪深300
        "000903.SH":"is_zz100=1",   # 中证100 & 中证A100
        "000905.SH":"is_zz500=1",   # 中证500
        "000510.SH":"is_zz500=1",   # 中证A500
        "000852.SH":"is_zz1000=1",  # 中证1000
        "000985.CSI":"1=1",         # 中证全指
        "000016.SH":"is_sh50=1",    # 上证50
        "000688.SH":"is_kc50=1",    # 科创50
        "000001.SH":"is_szzs=1",    # 上证指数
        "399330.SZ":"is_sz100=1",   # 深证100
        "399001.SZ":"is_szcz=1",    # 深证成指
        "399006.SZ":"is_cybz=1",    # 创业板指
        "899050.BJ":"is_bz50",      # 北证50
    }
    str_pool = map_pool[stock_pool]
    
    sql_merge = f"""
    WITH
    data_alpha AS (
        {sql_factor}
    ),
    data_pool AS (
        SELECT * 
        FROM data_alpha JOIN cn_stock_prefactors USING (date, instrument)
        WHERE {str_pool}
    ),
    data_filter AS (
        SELECT
            date,
            instrument,
            factor AS score,
            c_rank(-1 * factor) AS score_rank,
        FROM data_pool
        QUALIFY score_rank <= {number_security}
    ),
    data_date AS (
        SELECT
            date,
            instrument,
            score, 
            score_rank, 
            {str_position} AS position, 
        FROM data_filter JOIN cn_stock_valuation USING (date, instrument) JOIN mldt_cn_stock_calendar_daily USING (date)
        WHERE {str_rebalance} 
    )
    SELECT *
    FROM data_date
    ORDER BY date, score_rank
    """
    return sql_merge

def backtest_factor_strategy(df, capital_base):

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

def backtest_market_neutral_strategy(df, capital_base):
    
    def BigTrader_Initialize(context):
        from bigtrader.finance.commission import PerOrder
        context.set_commission(PerOrder(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        context.data_date = dai.query("SELECT date FROM mldt_cn_stock_calendar_daily WHERE is_month_end_trade = 1").df()

    def BigTrader_Before_Trading(context, data):
        pass

    def BigTrader_Handle_Tick(context, tick):
        pass

    def BigTrader_Handle_Data(context, data):

        df_now = context.data_date[context.data_date["date"] == data.current_dt.strftime("%Y-%m-%d")]
        if len(df_now) == 0:
            return

        from bigtrader.constant import OrderType
        from bigtrader.constant import Direction
        
        index_future = "IF8888.CFE"
        position_short = context.get_account_position(index_future, direction=Direction.SHORT)
        price = data.current(index_future, "open")
        if (position_short != 0):
            context.buy_close(index_future, 1, price, order_type=OrderType.MARKET)
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
        order_price_field_sell="""open""",
        benchmark="""沪深300指数""",
        
        plot_charts=True,
        debug=False,
        backtest_only=False,
        m_name="""BigTrader"""
    ) 

    return BigTrader.raw_perf.read()
