import dai
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class CNStockFactorsVPTAD1:
    
    def __init__(self, sd, ed):
        self.sd = sd
        self.ed = ed

    def sd_alpha(self, lag):
        if lag == 'UNLIMITED':
            return '1990-01-01'
        else:
            trading_days = dai.query("SELECT DISTINCT date FROM cn_stock_bar1d WHERE date >= '1991-01-01' ORDER BY date").df()['date']
            sd_datetime = pd.to_datetime(self.sd)
            if sd_datetime in trading_days.values:
                sd_new = self.sd 
            else:
                sd_new = trading_days[trading_days < sd_datetime].max().strftime('%Y-%m-%d')
            sd_alpha = dai.query(f"SELECT date, LAG(date, {lag}) OVER (ORDER BY date) AS sd FROM all_trading_days WHERE market_code = 'CN' QUALIFY date = '{sd_new}'").df()['sd'].iloc[0].strftime("%Y-%m-%d")
            return sd_alpha

    def generate_alpha_df(self, sql_alpha, lag):
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
                turn,
                price_limit_status,
                float_market_cap,
                total_market_cap,
            FROM cn_stock_bar1d 
            JOIN cn_stock_status    USING (date, instrument)
            JOIN cn_stock_valuation USING (date, instrument)
        ),
        data_alpha AS (
            {sql_alpha}
        ),
        data_stock AS (
            SELECT
                date,
                instrument,
            FROM cn_stock_instruments
        ),
        data_merge AS (
            SELECT *
            FROM data_stock LEFT JOIN data_alpha USING (date, instrument)
        )
        SELECT *
        FROM data_merge
        ORDER BY date, instrument
        """
        alpha_df = dai.query(sql_combine, filters={'date':[self.sd_alpha(lag), self.ed]}).df()
        alpha_df = alpha_df[(alpha_df['date'] >= pd.to_datetime(self.sd)) & (alpha_df['date'] <= pd.to_datetime(self.ed))] 
        return alpha_df.reset_index(drop=True)
    
    # 1. Moving Statistics
    # 1.1 Simple Statistics
    # N-period moving average
    def alpha_vpta_mavg(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg({col}, {n}) AS alpha_vpta_mavg_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving summation 
    def alpha_vpta_msum(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum({col}, {n}) AS alpha_vpta_msum_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving standard deviation 
    def alpha_vpta_mstd(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev({col}, {n}) AS alpha_vpta_mstd_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving variance 
    def alpha_vpta_mvar(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_var_pop({col}, {n}) AS alpha_vpta_mvar_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving skewness 
    def alpha_vpta_mskw(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_skewness({col}, {n}) AS alpha_vpta_mskw_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving kurtosis 
    def alpha_vpta_mkrt(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_kurtosis({col}, {n}) AS alpha_vpta_mkrt_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period consecutive rise 
    def alpha_vpta_mcov(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_covar_samp({col1}, {col2}, {n}) AS alpha_vpta_mcov_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period moving correlation 
    def alpha_vpta_mcor(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_corr({col1}, {col2}, {n}) AS alpha_vpta_mcor_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period maximum
    def alpha_vpta_mmax(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_max({col}, {n}) AS alpha_vpta_mmax_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period minimum
    def alpha_vpta_mmin(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_min({col}, {n}) AS alpha_vpta_mmin_{col}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # 1.2 Regression
    # 1.2.1 Regression: col vs window
    # N-period regression slope: col vs window
    def alpha_vpta_regr_slope_with_window(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY date ASC) AS _rn,
            m_regr_slope({col}, _rn, {n}) AS alpha_vpta_regr_slope_{col}_window_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression intercept: col vs window
    def alpha_vpta_regr_intercept_with_window(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY date ASC) AS _rn,
            m_regr_intercept({col}, _rn, {n}) AS alpha_vpta_regr_intercept_{col}_window_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression R2: col vs window
    def alpha_vpta_regr_r2_with_window(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY date ASC) AS _rn,
            m_regr_r2({col}, _rn, {n}) AS alpha_vpta_regr_r2_{col}_window_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression residual: col vs window
    def alpha_vpta_regr_resid_with_window(self, col, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY date ASC) AS _rn,
            {col} - m_regr_slope({col}, _rn, {n})*_rn - m_regr_intercept({col}, _rn, {n}) AS alpha_vpta_regr_resid_{col}_window_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # 1.2.2 Regression: col1 vs col2
    # N-period regression slope: col1 vs col2
    def alpha_vpta_regr_slope(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_slope({col1}, {col2}, {n}) AS alpha_vpta_regr_slope_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression intercept: col1 vs col2
    def alpha_vpta_regr_intercept(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_intercept({col1}, {col2}, {n}) AS alpha_vpta_regr_intercept_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression R2: col1 vs col2
    def alpha_vpta_regr_r2(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_r2({col1}, {col2}, {n}) AS alpha_vpta_regr_r2_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period regression residual: col1 vs col2
    def alpha_vpta_regr_resid(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            {col1} - m_regr_slope({col1}, {col2}, {n})*{col2} - m_regr_intercept({col1}, {col2}, {n}) AS alpha_vpta_regr_resid_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # 2. Volume-Price Indicators
    # 2.1 Moving Indicators
    # N-period return
    def alpha_vpta_return(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            close / m_lag(close,{n}) - 1 AS alpha_vpta_return_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period price_ratio
    def alpha_vpta_price_ratio(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_lag({col1}, {n}) / {col2} AS alpha_vpta_price_ratio_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period average price ratio
    def alpha_vpta_avg_price_ratio(self, col1, col2, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg({col1}, {n}) / {col2} AS alpha_vpta_avg_price_ratio_{col1}_{col2}_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period volume ratio
    def alpha_vpta_volume_ratio(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_lag(volume, {n}) / volume AS alpha_vpta_volume_ratio_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period vwap
    def alpha_vpta_vwap(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(amount, {n}) / m_sum(volume, {n}) AS alpha_vpta_vwap_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period relative price change
    def alpha_vpta_ralative_price_change(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close - m_min(low, {n})) / (m_max(high, {n}) - m_min(low, {n}) + 1e-12) AS alpha_vpta_relative_price_change_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period highest price relative location
    def alpha_vpta_high_relative_location(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_imax(high, {n})/{n} AS alpha_vpta_high_relative_location_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period lowest price relative location
    def alpha_vpta_low_relative_location(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_imin(low, {n})/{n} AS alpha_vpta_low_relative_location_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period high-low relative distance
    def alpha_vpta_high_low_relative_distance(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_imax(high, {n})-m_imin(low, {n}))/{n} AS alpha_vpta_high_low_relative_distance_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period price-volume correlation
    def alpha_vpta_price_volume_correlation(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_corr(close, volume, {n}) AS alpha_vpta_price_volume_correlation_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period price-volume change correlation
    def alpha_vpta_price_volume_change_correlation(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_corr(close/m_lag(close, 1), log(volume/m_lag(volume, 1)+1), {n}) AS alpha_vpta_price_volume_change_correlation_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period up days ratio
    def alpha_vpta_up_days_ratio(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(IF(close>m_lag(close, 1), 1, 0), {n}) AS alpha_vpta_up_days_ratio_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period down days ratio
    def alpha_vpta_down_days_ratio(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(IF(close<m_lag(close, 1), 1, 0), {n}) AS alpha_vpta_down_days_ratio_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period up-down days ratio
    def alpha_vpta_up_down_days_ratio(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(IF(close>m_lag(close, 1), 1, 0), {n}) - m_avg(IF(close<m_lag(close, 1), 1, 0), {n}) AS alpha_vpta_up_down_days_ratio_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period relative close profit
    def alpha_vpta_relative_close_profit(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close-m_lag(close, 1)>0, close-m_lag(close, 1), 0), {n}) / (m_sum(abs(close-m_lag(close, 1)), {n}) + 1e-12) AS alpha_vpta_relative_close_profit_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period relative close loss
    def alpha_vpta_relative_close_loss(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(m_lag(close, 1)-close < 0, 0, m_lag(close, 1)-close), {n}) / (m_sum(abs(close-m_lag(close, 1)), {n}) + 1e-12) AS alpha_vpta_relative_close_loss_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # N-period relative close net gain
    def alpha_vpta_relative_close_net_gain(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(m_lag(close, 1)-close > 0, m_lag(close, 1)-close, 0), {n}) / (m_sum(abs(close-m_lag(close, 1)), {n})+1e-12) AS alpha_vpta_relative_close_net_gain_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period consecutive rise
    def alpha_vpta_consecutive_rise(self, n):
        sql_str = " AND ".join([f"m_lag(close, {i}) > m_lag(close, {i + 1})" for i in range(1, n)])
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(close > m_lag(close, 1) AND {sql_str if sql_str != '' else '1=1'}, 1, 0) AS alpha_vpta_consecutive_rise_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # N-period consecutive limit up 
    def alpha_vpta_consecutive_limit_up(self, n):
        sql_str = " AND ".join([f"m_lag(price_limit_status, {i}) = 3" for i in range(1, n+1)])
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(price_limit_status = 3 AND {sql_str if sql_str != '' else '1=1'}, 1, 0) AS alpha_vpta_consecutive_limit_up_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)

    # 2.2 K Stick Indicators
    # K length
    def alpha_vpta_k_length(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close - open)/open AS alpha_vpta_k_length
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # K relative length
    def alpha_vpta_k_relative_length(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-open)/(high-low+1e-12) AS alpha_vpta_k_relative_length
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # K upper shadow
    def alpha_vpta_k_upper_shadow(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (high-IF(open > close, open, close))/open AS alpha_vpta_k_upper_shadow
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # K upper shadow ratio
    def alpha_vpta_k_upper_shadow_ratio(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (high-IF(open > close, open, close))/(high-low+1e-12) AS alpha_vpta_k_upper_shadow_ratio
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # K lower shadow
    def alpha_vpta_k_lower_shadow(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(open > close, close, open)-low)/open AS alpha_vpta_k_lower_shadow
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # K lower shadow ratio
    def alpha_vpta_k_lower_shadow_ratio(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(open > close, close, open)-low)/(high-low+1e-12) AS alpha_vpta_k_lower_shadow_ratio
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # TRIN
    def alpha_vpta_TRIN(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (2*close-high-low)/open AS alpha_vpta_TRIN
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # TRIN ratio
    def alpha_vpta_TRIN_ratio(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (2*close-high-low)/(high-low+1e-12) AS alpha_vpta_TRIN_ratio
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # 3. Technical Factors
    # TODO: Signal Types
    """
    1. Cross Signal
    SIGN_CROS_GOLD: Gold Cross (Buy)
    SIGN_CROS_DEAD: Dead Cross (Sell)

    2. Breaking Signal
    SIGN_BREK_BRES: Breaking Resistance (Buy)
    SIGN_BREK_SUPT: Breaking Support (Sell)

    3. Reverse Signal
    SIGN_RVSE_OSEL: Over Sell /
    SIGN_RVSE_OBUY: Over Buy  / Top Divergence

    4. Divergence Signal
    SIGN_DIVG_BOTD: Bottom Divergence
    SIGN_DIVG_TOPD: Top Divergence

    5. Trend Signal (Momentum)
    SIGN_TRND_UPRS: Continuously Go Up
    SIGN_TRND_DOWN: Continuously Go Down

    6. Volatility Signal
    SIGN_VOLT_INSE: Volatility Increase
    SIGN_VOLT_DESE: Volatility Decrease

    7. Volume Signa;
    SIGN_VOLM_
    """

    # 3.1 Price Indicators
    # SMA
    def alpha_vpta_SMA(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(close, {n}) AS alpha_vpta_SMA_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # EMA
    def alpha_vpta_EMA(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ema(close, {n}) AS alpha_vpta_EMA_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # BBI
    def alpha_vpta_BBI(self, n1=3, n2=6, n3=12, n4=24):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg(close,{n1}) + m_avg(close,{n2}) + m_avg(close,{n3}) + m_avg(close,{n4}))/4 AS alpha_vpta_BBI_{n1}_{n2}_{n3}_{n4}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2,n3,n4])+15)
    
    # DMI
    def alpha_vpta_DMI(self, n1=14, n2=6):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                m_ta_ema(if(high-m_lag(high,1)>0, high-m_lag(high,1),0), {n1})  AS PDM,
                m_ta_ema(if(m_lag(low,1)-low>0,   m_lag(low,1)-low,0),   {n1})  AS NDM,
                m_ta_ema(greatest(abs(high-low), abs(high-m_lag(close,1)), abs(low-m_lag(close,1))), {n1}) AS TR,
                PDM / TR * 100 AS PDI,
                NDM / TR * 100 AS MDI,
                m_avg(abs(PDI-MDI)/(PDI+MDI), {n2}) AS ADX,
                (ADX+m_lag(ADX, {n2}))/2 AS ADXR
            FROM data_base
        )
        SELECT
            date,
            instrument,
            PDM  AS alpha_vpta_DMI_PDM_{n1}_{n2},
            NDM  AS alpha_vpta_DMI_NDM_{n1}_{n2},
            TR   AS alpha_vpta_DMI_TR_{n1}_{n2},
            PDI  AS alpha_vpta_DMI_PDI_{n1}_{n2},
            MDI  AS alpha_vpta_DMI_MDI_{n1}_{n2},
            ADX  AS alpha_vpta_DMI_ADX_{n1}_{n2},
            ADXR AS alpha_vpta_DMI_ADXR_{n1}_{n2},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # MACD
    def alpha_vpta_MACD(self, n_long=26, n_short=13, n_signal=9):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_ta_ema(close, {n_short}) - m_ta_ema(close, {n_long}) AS DIF,
                m_ta_ema(DIF, {n_signal})                              AS DEA,
                DIF - DEA                                              AS HIST,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            DIF  AS alpha_vpta_MACD_DIF_{n_long}_{n_short}_{n_signal},
            DEA  AS alpha_vpta_MACD_DEA_{n_long}_{n_short}_{n_signal},
            HIST AS alpha_vpta_MACD_HIST_{n_long}_{n_short}_{n_signal},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, max([n_long, n_short, n_signal])+15)
    
    # MTM
    def alpha_vpta_MTM(self, n1=6, n2=6):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_avg(close - m_lag(close, {n1}), {n2}) AS MTM
            FROM data_base
        )
        SELECT
            date,
            instrument,
            MTM AS alpha_vpta_MTM_MTM_{n1}_{n2},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)

    # PRICEOSC
    def alpha_vpta_PRICEOSC(self, n_short=13, n_long=26):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            100*(m_avg(close,{n_short}) - m_avg(close,{n_long}))/m_avg(close,{n_short}) AS alpha_vpta_PRICEOSC_{n_short}_{n_long}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n_short,n_long])+15)
    
    # SAR
    # TODO: SQL realization of SAR
    def alpha_vpta_SAR(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_sar(high, low) AS alpha_vpta_SAR
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # TRIX
    def alpha_vpta_TRIX(self, n1=12, n2=12):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_ta_ema(close, {n1}) AS EMA1,
                m_ta_ema(EMA1,  {n1}) AS EMA2,
                m_ta_ema(EMA1,  {n1}) AS EMA3,
                (EMA3 - m_lag(EMA3, 1)) / m_lag(EMA3, 1) AS TRIX,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            EMA1              AS alpha_vpta_TRIX_EMA1_{n1}_{n2},
            EMA2              AS alpha_vpta_TRIX_EMA2_{n1}_{n2},
            EMA3              AS alpha_vpta_TRIX_EMA3_{n1}_{n2},
            TRIX              AS alpha_vpta_TRIX_TRIX_{n1}_{n2},
            m_avg(TRIX, {n2}) AS alpha_vpta_TRIX_MATRIX_{n1}_{n2},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)

    # BIAS
    def alpha_vpta_BIAS(self, n1=3, n2=6):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg(close,{n1}) - m_avg(close,{n2}))                      AS alpha_vpta_BIAS_BIAS_{n1}_{n2},
            (m_avg(close,{n1}) - m_avg(close,{n2})) / m_avg(close,{n2})  AS alpha_vpta_BIAS_BIAS_RATE_{n1}_{n2},
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # CCI
    def alpha_vpta_CCI(self, n=12):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                (high+low+close) / 3     AS TP,
                m_avg(TP, {n})           AS MA,
                m_avg(abs(TP - MA), {n}) AS MD,
                (TP - MA) / (0.015*MD)   AS CCI,
            FROM data_base
        )
        SELECT
            date, 
            instrument, 
            TP  AS alpha_vpta_CCI_TP_{n},
            MA  AS alpha_vpta_CCI_MA_{n},
            MD  AS alpha_vpta_CCI_MD_{n},
            CCI AS alpha_vpta_CCI_CCI_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # DPO
    def alpha_vpta_DPO(self, n1=12, n2=7):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            close-m_lag(m_avg(close,{n1}),{n2})         AS alpha_vpta_DPO_DPO_{n1}_{n2},
            m_avg(close-m_lag(m_avg(close,{n1}),{n2}),{n1})  AS alpha_vpta_DPO_MADPO_{n1}_{n2},
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # KDJ
    def alpha_vpta_KDJ(self, n=9):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                (close-m_min(low,{n})) / (m_max(high,{n})-m_min(low,{n})) * 100 AS RSV,
                m_avg(RSV, {n}) AS K,
                m_avg(K,   {n}) AS D,
                3*K - 2*D       AS J,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            K AS alpha_vpta_KDJ_K_{n},
            D AS alpha_vpta_KDJ_D_{n},
            J AS alpha_vpta_KDJ_J_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # ROC
    def alpha_vpta_ROC(self, n1=12, n2=6):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close - m_lag(close, {n1})) / m_lag(close, {n1})               AS alpha_vpta_ROC_ROC_{n1}_{n2},
            m_avg((close - m_lag(close, {n1})) / m_lag(close, {n1}), {n2})  AS alpha_vpta_ROC_MAROC_{n1}_{n2},
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # RSI
    def alpha_vpta_RSI(self, n=6):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                (m_sum(IF(close > m_lag(close,1), close, 0), {n}) / {n}) / ((m_sum(IF(close < m_lag(close,1), close, 0), {n}) / {n}) + 0.0001) AS RS,
                100 - (100/(1+RS)) AS RSI
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            RS  AS alpha_vpta_RSI_RS_{n},
            RSI AS alpha_vpta_RSI_RSI_{n},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)

    # RPS
    def alpha_vpta_RPS(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT 
                date, 
                instrument, 
                (close - m_lag(close, {n})) / m_lag(close, {n}) AS stock_return
            FROM cn_stock_bar1d 
        ),
        data_index AS (
            SELECT
                date,
                (close - m_lag(close, {n})) / m_lag(close, {n}) AS index_return
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        ),
        data_merge AS (
            SELECT
                date,
                instrument,
                (stock_return / index_return) * 100 AS RPS,
            FROM data_stock JOIN data_index USING (date)
        )
        SELECT
            date,
            instrument,
            RPS AS alpha_vpta_RPS_RPS_{n},
        FROM data_merge
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # SI
    def alpha_vpta_SI(self):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                greatest(high-low, abs(high-m_lag(close,1)), abs(low-m_lag(close,1)))                                 AS TR,
                50 * (close - m_lag(close, 1) + 0.5*(close - m_lag(open, 1)) + 0.25*(m_lag(close,1) - m_lag(open,1))) AS SI,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            SI AS alpha_vpta_SI,
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, 15)
    
    # SRDM
    def alpha_vpta_SRDM(self, n=10):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                IF((high+low)<=(m_lag(high,1)+m_lag(low,1)),0,greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))) AS DMP,
                IF((high+low)>=(m_lag(high,1)+m_lag(low,1)),0,greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))) AS DMN,
                m_avg(DMP,{n}) AS ADMP,
                m_avg(DMN,{n}) AS ADMN,
                IF(ADMP > ADMN, (ADMP-ADMN)/ADMP, IF(ADMP=ADMN, 0, (ADMP-ADMN)/ADMN)) AS SRDM,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            DMP  AS alpha_vpta_SRDM_DMP_{n},
            DMN  AS alpha_vpta_SRDM_DMN_{n},
            ADMP AS alpha_vpta_SRDM_ADMP_{n},
            ADMN AS alpha_vpta_SRDM_ADMN_{n},
            SRDM AS alpha_vpta_SRDM_SRDM_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # WR
    def alpha_vpta_WR(self, n=14):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                (m_max(high, {n}) - close) / (m_max(high, {n}) - m_min(low, {n})) AS WR,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            WR AS alpha_vpta_WR_WR_{n},
            IF(WR > -50 AND m_lag(WR,1) < -50, 1, 0) AS alpha_vpta_WR_GOLD_CROSS_A_{n},
            IF(WR < -50 AND m_lag(WR,1) > -50, 1, 0) AS alpha_vpta_WR_DEAD_CROSS_A_{n},
            IF(WR < -80 AND m_lag(WR,1) > -80, 1, 0) AS alpha_vpta_WR_GOLD_CROSS_B_{n},
            IF(WR > -20 AND m_lag(WR,1) < -20, 1, 0) AS alpha_vpta_WR_DEAD_CROSS_B_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # APBR
    def alpha_vpta_APBR(self, n=26):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_sum(high - open,{n})/m_sum(open - low,{n}) AS AR,
                m_sum(high - m_lag(close,1),{n})/m_sum(m_lag(close,1) - low,{n}) AS BR,
                AR / BR AS ARBR,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            AR AS alpha_vpta_APBR_AR_{n},
            BR AS alpha_vpta_APBR_BR_{n},
            ARBR AS alpha_vpta_APBR_ARBR_{n},
            IF(ARBR > 1 AND m_lag(ARBR,1) < 1, 1, 0) AS alpha_vpta_APBR_GOLD_CROSS_{n},
            IF(ARBR < 1 AND m_lag(ARBR,1) > 1, 1, 0) AS alpha_vpta_APBR_DEAD_CROSS_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # CR
    def alpha_vpta_CR(self, n=26):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                m_sum(high - m_lag((2*close + high + low)/4,1),{n})/m_sum(m_lag((2*close + high + low)/4,1) - low,{n}) AS CR,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            CR AS alpha_vpta_CR_CR_{n},
            IF(CR>0 AND m_lag(CR,1)<0, 1, 0) AS alpha_vpta_CR_GOLD_CROSS_A_{n},
            IF(CR<0 AND m_lag(CR,1)>0, 1, 0) AS alpha_vpta_CR_DEAD_CROSS_A_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # PSY
    def alpha_vpta_PSY(self, n1=12, n2=6):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_avg(m_sum(if(close > m_lag(close,1),1,0),{n1}),{n2}) AS PSY
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            PSY AS alpha_vpta_PSY_PSY_{n1}_{n2},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    

    
    # WAD
    def alpha_vpta_WAD(self):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                IF(close>=m_lag(close,1),close-least(low,m_lag(close,1)),close-greatest(high,m_lag(close,1))) AS WAD_temp,
                SUM(WAD_temp) OVER (PARTITION BY instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS WAD
            FROM data_base
        )
        SELECT
            date,
            instrument,
            WAD AS alpha_vpta_WAD_WAD
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, 'UNLIMITED')

    # BBIBOLL
    def alpha_vpta_BBIBOLL(self, n=10):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                close,
                (m_avg(close,3)+m_avg(close,6)+m_avg(close,12)+m_avg(close,24))/4 AS BBIBOLL_MIDDLE,
                BBIBOLL_MIDDLE + 2*m_stddev(BBIBOLL_MIDDLE, {n}) AS BBIBOLL_UPPER,
                BBIBOLL_MIDDLE - 2*m_stddev(BBIBOLL_MIDDLE, {n}) AS BBIBOLL_LOWER,
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            BBIBOLL_MIDDLE AS alpha_vpta_BBIBOLL_MIDDLE_{n},
            BBIBOLL_UPPER  AS alpha_vpta_BBIBOLL_UPPER_{n},
            BBIBOLL_LOWER  AS alpha_vpta_BBIBOLL_LOWER_{n},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # BOLL
    def alpha_vpta_BOLL(self, n=26):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                close,
                m_avg(close, {n})                 AS MIDDLE,
                MIDDLE + 2 * m_stddev(close, {n}) AS UPPER,
                MIDDLE - 2 * m_stddev(close, {n}) AS LOWER,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            MIDDLE AS alpha_vpta_BOLL_MIDDLE_{n},
            UPPER  AS alpha_vpta_BOLL_UPPER_{n},
            LOWER  AS alpha_vpta_BOLL_LOWER_{n},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # CDP
    def alpha_vpta_CDP(self):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                close,
                (m_lag(high,1) + m_lag(low,1) + 2*m_lag(close,1)) / 4 AS CDP,
                CDP + (m_lag(high,1)-m_lag(low,1))                    AS AH,
                CDP - (m_lag(high,1)-m_lag(low,1))                    AS AL,
                2*CDP - m_lag(low,1)                                  AS NH,
                2*CDP - m_lag(high,1)                                 AS NL,
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            CDP AS alpha_vpta_CDP_CDP,
            AH  AS alpha_vpta_CDP_AH,
            AL  AS alpha_vpta_CDP_AL,
            NH  AS alpha_vpta_CDP_NH,
            NL  AS alpha_vpta_CDP_NL,
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, 15)
    
    # ENV
    def alpha_vpta_ENV(self, n=14, k=0.05):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(close, {n}) * (1+{k}) AS factor
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # MIKE
    def alpha_vpta_MIKE(self, n=12):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                close,
                (high+low+close)/3 AS TYP,
                TYP + (TYP - m_min(low,{n}))                  AS WR,
                TYP - (m_max(high,{n}) - TYP)                 AS WS,
                TYP + (m_max(high,{n}) - m_min(low,{n}))      AS MR,
                TYP - (m_max(high,{n}) - m_min(low,{n}))      AS MS,
                m_max(high,{n}) + 2 * (TYP - m_min(low,{n}))  AS SR,
                m_min(low,{n})  - 2 * (m_max(high,{n}) - TYP) AS SS,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            WR AS alpha_vpta_MIKE_WR_{n},
            WS AS alpha_vpta_MIKE_WS_{n},
            MR AS alpha_vpta_MIKE_MR_{n},
            MS AS alpha_vpta_MIKE_MS_{n},
            SR AS alpha_vpta_MIKE_SR_{n},
            SS AS alpha_vpta_MIKE_SS_{n},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    

    # TAPI
    def alpha_vpta_TAPI(self, n=6):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(amount / (close * total_market_cap / c_sum(total_market_cap)), {n}) AS alpha_vpta_TAPI_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # ADTM
    def alpha_vpta_ADTM(self, n=23):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                IF(open <= m_lag(open, 1), 0, GREATEST((high - open), (open - m_lag(open, 1)))) AS DTM,
                IF(open >= m_lag(open, 1), 0, GREATEST((open - low),  (open - m_lag(open, 1)))) AS DBM,
                m_sum(DTM, {n}) AS STM,
                m_sum(DBM, {n}) AS SBM,
                IF(STM > SBM, (STM - SBM) / STM, (STM - SBM) / SBM) AS ADTM
            FROM data_base
        )
        SELECT
            date,
            instrument,
            DTM  AS alpha_vpta_ADTM_DTM_{n},
            DBM  AS alpha_vpta_ADTM_DBM_{n},
            STM  AS alpha_vpta_ADTM_STM_{n},
            SBM  AS alpha_vpta_ADTM_SBM_{n},
            ADTM AS alpha_vpta_ADTM_ADTM_{n},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # RC
    def alpha_vpta_RC(self, n=50):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_avg(close / m_lag(close,{n}) - 1, {n}) AS RC
            FROM data_base
        )
        SELECT
            date,
            instrument,
            RC AS alpha_vpta_RC_RC_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # SRMI
    def alpha_vpta_SRMI(self, n=9):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                IF(close < m_lag(close, {n}), (close - m_lag(close, {n})) / m_lag(close, {n}), IF(close = m_lag(close, {n}), 0, (close - m_lag(close, {n})) / close)) AS SRMI
            FROM data_base
        )
        SELECT
            date,
            instrument,
            SRMI AS alpha_vpta_SRMI_SRMI_{n}
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # MSI
    def alpha_vpta_MSI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_avg(IF((stock_close >= stock_open AND index_close >= index_open) OR (stock_close < stock_open AND index_close < index_open), 1, 0), {n}) AS alpha_vpta_MSI_MSI_{n}
        FROM data_stock JOIN data_index USING (date)
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # PSI
    def alpha_vpta_PSI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_sum(IF(stock_close >= stock_open AND index_close < index_open, 1, 0), {n}) / m_sum(IF(index_close < index_open, 1, 0), {n}) AS alpha_vpta_PSI_PSI_{n},
        FROM data_stock JOIN data_index USING (date)
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # PWI
    def alpha_vpta_PWI(self, n=6):
        sql_alpha = f"""
        WITH
        data_stock AS (
            SELECT
                date,
                instrument,
                open  AS stock_open,
                close AS stock_close,
            FROM cn_stock_bar1d
        ),
        data_index AS (
            SELECT
                date,
                open  AS index_open,
                close AS index_close,
            FROM cn_stock_index_bar1d
            WHERE instrument = '000300.SH'
        )
        SELECT
            date,
            instrument,
            m_sum(IF(stock_close >= stock_open AND index_close < index_open, 1, 0), {n}) / m_sum(IF(index_close > index_open, 1, 0), {n}) AS alpha_vpta_PWI_PWI_{n},
        FROM data_stock JOIN data_index USING (date)
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # UDRF
    def alpha_vpta_UDRF(self, n1=125, n2=10):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(m_sum(IF(close >= m_lag(close, 1), 1, 0), {n1}) / m_sum(IF(close < m_lag(close, 1), 1, 0), {n1}), {n2}) AS alpha_vpta_UDRF_UDRF_{n1}_{n2}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # ATR
    def alpha_vpta_ATR(self, n=14):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(greatest(abs(high - low), abs(m_lag(close,1) - high), abs(m_lag(close,1) - low)), {n}) AS alpha_vpta_ATR_ATR_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # MASS
    def alpha_vpta_MASS(self, n1=9, n2=25):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            high - low AS _DIF,
            m_ta_ema(_DIF, {n1}) AS _AHL,
            m_ta_ema(m_ta_ema(_DIF, {n1}), {n1}) AS _BHL,
            m_ta_sum(m_ta_ema(_DIF, {n1}) / m_ta_ema(m_ta_ema(_DIF, {n1}), {n1}), {n2}) AS alpha_vpta_MASS_MASS_{n1}_{n2}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, max([n1,n2])+15)
    
    # VHF
    def alpha_vpta_VHF(self, n=28):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_max(close, {n}) - m_min(close, {n})) / (m_sum(abs(close - m_lag(close, 1)), {n})) AS alpha_vpta_VHF_VHF_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # CVLT
    def alpha_vpta_CVLT(self, n=10):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ema(high - low, {n}) AS _EC,
            (_EC - m_lag(_EC, {n})) / m_lag(_EC, {n}) * 100 AS alpha_vpta_CVLT_CVLT_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # 3.2 Volume Indicators
    # VR
    def alpha_vpta_VR(self, n=26):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                close,
                m_sum(IF(close >= m_lag(close,1),volume,0),{n}) / m_sum(IF(close < m_lag(close,1),volume,0),{n}) AS VR
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            VR AS alpha_vpta_VR_VR_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # VMA
    def alpha_vpta_VMA(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(volume, {n}) AS alpha_vpta_VMA_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # VSTD
    def alpha_vpta_VSTD(self, n):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(volume, {n}) AS alpha_vpta_VSTD_{n}
        FROM data_base
        """
        
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # VMACD
    def alpha_vpta_VMACD(self, n_short=13, n_long=26, n_signal=9):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                m_ta_ema(volume, {n_short}) - m_ta_ema(volume, {n_long}) AS DIF,
                m_ta_ema(DIF, {n_signal})                                AS DEA,
                DIF - DEA                                                AS HIST,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            DIF  AS alpha_vpta_VMACD_DIF_{n_short}_{n_long}_{n_signal},
            DEA  AS alpha_vpta_VMACD_DEA_{n_short}_{n_long}_{n_signal},
            HIST AS alpha_vpta_VMACD_HIST_{n_short}_{n_long}_{n_signal},
        FROM data1
        """
        
        return self.generate_alpha_df(sql_alpha, max([n_short, n_long, n_signal])+15)
    
    # VOSC
    def alpha_vpta_VOSC(self, n_short = 13, n_long=26):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            100 * ((m_avg(volume, {n_short}) - m_avg(volume, {n_long})) / m_avg(volume, {n_short})) AS alpha_vpta_VOSC_{n_short}_{n_long}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n_short, n_long])+15)
    
    # VROC
    def alpha_vpta_VROC(self, n1=12, n2=6):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (volume - m_lag(volume, {n1})) / m_lag(volume, {n1})               AS alpha_vpta_VROC_VROC_{n1}_{n2},
            m_avg((volume - m_lag(volume, {n1})) / m_lag(volume, {n1}), {n2})  AS alpha_vpta_VROC_VMAROC_{n1}_{n2},
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, max([n1, n2])+15)

    # VRSI
    def alpha_vpta_VRSI(self, n=6):
        sql_alpha = f"""
        WITH
        data1 AS (
            SELECT
                date,
                instrument,
                (m_sum(IF(volume > m_lag(volume,1), volume, 0), {n}) / {n}) / ((m_sum(IF(volume < m_lag(volume,1), volume, 0), {n}) / {n}) + 0.0001) AS RS,
                100 - (100/(1+RS)) AS RSI
            FROM data_base
        )
        SELECT 
            date,
            instrument,
            RS  AS alpha_vpta_VRSI_VRS_{n},
            RSI AS alpha_vpta_VRSI_VRSI_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # 3.3 Price-Volume Combined Indicators
    # MFI
    def alpha_vpta_MFI(self, n=14):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                (high + low + close) / 3             AS TP,
                IF(TP > m_lag(TP,1), TP * volume, 0) AS PMF,
                IF(TP < m_lag(TP,1), TP * volume, 0) AS NMF,
                m_sum(PMF, {n})                      AS PMF_Total,
                m_sum(NMF, {n})                      AS NMF_Total,
                PMF_Total / (NMF_Total + 0.0001)     AS MFR,
                100 - (100 / (1 + MFR))              AS MFI,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            PMF_Total AS alpha_vpta_MFI_PMF_{n},
            NMF_Total AS alpha_vpta_MFI_NMF_{n},
            MFR       AS alpha_vpta_MFI_MFR_{n},
            MFI       AS alpha_vpta_MFI_MFI_{n},
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    
    # OBV
    def alpha_vpta_OBV(self):
        sql_alpha = f"""
        WITH 
        data1 AS (
            SELECT
                date,
                instrument,
                IF(close > m_lag(close, 1), volume, -1 * volume) AS volume_change,
                SUM(volume_change) OVER(PARTITION BY instrument ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                  AS OBV,
                IF(close > m_lag(close, 1),((close-low)-(high-close))/(high-low)*(OBV+volume),((close-low)-(high-low))/(high-low)*(OBV-volume))  AS OB_ADJ,
            FROM data_base
        )
        SELECT
            date,
            instrument,
            OBV          AS alpha_vpta_OBV_OBV,
            OB_ADJ       AS alpha_vpta_OBV_OBV_ADJ,
        FROM data1
        """
        return self.generate_alpha_df(sql_alpha, 'UNLIMITED')
    
    # PVT
    def alpha_vpta_PVT(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            SUM((close-m_lag(close, 1))/m_lag(close, 1)*volume) OVER(PARTITION BY instrument ORDER BY date ROWS UNBOUNDED PRECEDING) AS alpha_vpta_PVT
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 'UNLIMITED')
    
    # SOBV
    def alpha_vpta_SOBV(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            SUM(IF(close>open,volume,-1*volume)) OVER(PARTITION BY instrument ORDER BY date ROWS UNBOUNDED PRECEDING) AS alpha_vpta_SOBV
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 'UNLIMITED')
    
    # WVAD
    def alpha_vpta_WVAD(self, n=6):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum((close - open)/(high - low)*volume, {n}) AS alpha_vpta_WVAD_{n}
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, n+15)
    