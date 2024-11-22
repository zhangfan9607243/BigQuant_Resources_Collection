import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.FACT.FACT import *

class A101(FACT):
    
    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
    
    def get_alpha_sql(self, sql_alpha):
        return sql_alpha
    
    def get_alpha_df(self, sql_alpha, lag):
        return self.get_dai_df(self.get_alpha_sql(sql_alpha), self.sd, self.ed)
    
    def alpha_a101_f0001(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_imax(power(IF(returns < 0, m_stddev(returns, 20), close), 2), 5)) - 0.5) 
            AS alpha_a101_f0001,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)
    
    def alpha_a101_f0002(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(c_pct_rank(m_delta(log(volume), 2)), c_pct_rank(((close - open) / open)), 6)) 
            AS alpha_a101_f0002,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0003(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(c_pct_rank(open), c_pct_rank(volume), 10)) 
            AS alpha_a101_f0003,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0004(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_rank(c_pct_rank(low), 9)) 
            AS alpha_a101_f0004,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0005(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank((open - (m_sum(vwap, 10) / 10))) * (-1 * abs(c_pct_rank((close - vwap))))) 
            AS alpha_a101_f0005,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0006(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(open, volume, 10)) 
            AS alpha_a101_f0006,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0007(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(adv20 < volume, ((-1 * m_rank(abs(m_delta(close, 7)), 60)) * sign(m_delta(close, 7))), (-1 * 1))) 
            AS alpha_a101_f0007,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0008(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(((m_sum(open, 5) * m_sum(returns, 5)) - m_lag((m_sum(open, 5) * m_sum(returns, 5)), 10)))) 
            AS alpha_a101_f0008,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0009(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(0 < m_min(m_delta(close, 1), 5), m_delta(close, 1), IF(m_max(m_delta(close, 1), 5) < 0, m_delta(close, 1), (-1 * m_delta(close, 1))))) 
            AS alpha_a101_f0009,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0010(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(IF(0 < m_min(m_delta(close, 1), 4), m_delta(close, 1), IF(m_max(m_delta(close, 1), 4) < 0, m_delta(close, 1), (-1 * m_delta(close, 1))))) 
            AS alpha_a101_f0010,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0011(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_max((vwap - close), 3)) + c_pct_rank(m_min((vwap - close), 3))) * c_pct_rank(m_delta(volume, 3))) 
            AS alpha_a101_f0011,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0012(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (sign(m_delta(volume, 1)) * (-1 * m_delta(close, 1))) 
            AS alpha_a101_f0012,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0013(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(m_covar_samp(c_pct_rank(close), c_pct_rank(volume), 5))) 
            AS alpha_a101_f0013,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0014(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank(m_delta(returns, 3))) * m_corr(open, volume, 10))  
            AS alpha_a101_f0014,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0015(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_sum(c_pct_rank(m_corr(c_pct_rank(high), c_pct_rank(volume), 3)), 3)) 
            AS alpha_a101_f0015,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0016(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(m_covar_samp(c_pct_rank(high), c_pct_rank(volume), 5))) 
            AS alpha_a101_f0016,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0017(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((-1 * c_pct_rank(m_rank(close, 10))) * c_pct_rank(m_delta(m_delta(close, 1), 1))) * c_pct_rank(m_rank((volume / adv20), 5))) 
            AS alpha_a101_f0017,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0018(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(((m_stddev(abs((close - open)), 5) + (close - open)) + m_corr(close, open, 10)))) 
            AS alpha_a101_f0018,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0019(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * sign(((close - m_lag(close, 7)) + m_delta(close, 7)))) * (1 + c_pct_rank((1 + m_sum(returns, 250))))) 
            AS alpha_a101_f0019,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=270)

    def alpha_a101_f0020(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((-1 * c_pct_rank((open - m_lag(high, 1)))) * c_pct_rank((open - m_lag(close, 1)))) * c_pct_rank((open - m_lag(low, 1)))) 
            AS alpha_a101_f0020,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0021(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((((m_sum(close, 8) / 8) + m_stddev(close, 8)) < (m_sum(close, 2) / 2)), (-1 * 1), IF(((m_sum(close, 2) / 2) < ((m_sum(close, 8) / 8) - m_stddev(close, 8))), 1, IF(((1 < (volume / adv20)) OR ((volume / adv20) = 1)), 1, (-1 * 1)))) 
            AS alpha_a101_f0021,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0022(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * (m_delta(m_corr(high, volume, 5), 5) * c_pct_rank(m_stddev(close, 20)))) 
            AS alpha_a101_f0022,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0023(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((m_sum(high, 20) / 20) < high), (-1 * m_delta(high, 2)), 0) 
            AS alpha_a101_f0023,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0024(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) < 0.05) OR ((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) == 0.05)), (-1 * (close - m_min(close, 100))), (-1 * m_delta(close, 3))) 
            AS alpha_a101_f0024,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0025(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(((((-1 * returns) * adv20) * vwap) * (high - close))) 
            AS alpha_a101_f0025,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0026(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_max(m_corr(m_rank(volume, 5), m_rank(high, 5), 5), 3)) 
            AS alpha_a101_f0026,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0027(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((0.5 < c_pct_rank((m_sum(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 6), 2) / 2.0))), (-1 * 1), 1) 
            AS alpha_a101_f0027,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0028(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_scale(((m_corr(adv20, low, 5) + ((high + low) / 2)) - close), 1) 
            AS alpha_a101_f0028,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0029(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_min(m_product(c_pct_rank(c_pct_rank(c_scale(log(m_sum(m_min(c_pct_rank(c_pct_rank((-1 * c_pct_rank(m_delta((close - 1), 5))))), 2), 1)), 1))), 1), 5) + m_rank(m_lag((-1 * returns), 6), 5)) 
            AS alpha_a101_f0029,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0030(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((1.0 - c_pct_rank(((sign((close - m_lag(close, 1))) + sign((m_lag(close, 1) - m_lag(close, 2)))) + sign((m_lag(close, 2) - m_lag(close, 3)))))) * m_sum(volume, 5)) / m_sum(volume, 20)) 
            AS alpha_a101_f0030,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0031(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(c_pct_rank(c_pct_rank(m_decay_linear((-1 * c_pct_rank(c_pct_rank(m_delta(close, 10)))), 10)))) + c_pct_rank((-1 * m_delta(close, 3)))) + sign(c_scale(m_corr(adv20, low, 12), 1))) 
            AS alpha_a101_f0031,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0032(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_scale(((m_sum(close, 7) / 7) - close), 1) + (20 * c_scale(m_corr(vwap, m_lag(close, 5), 230), 1))) 
            AS alpha_a101_f0032,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=270)

    def alpha_a101_f0033(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank((-1 * ((1 - (open / close))^1))) 
            AS alpha_a101_f0033,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0034(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(((1 - c_pct_rank((m_stddev(returns, 2) / m_stddev(returns, 5)))) + (1 - c_pct_rank(m_delta(close, 1))))) 
            AS alpha_a101_f0034,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0035(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_rank(volume, 32) * (1 - m_rank(((close + high) - low), 16))) * (1 - m_rank(returns, 32))) 
            AS alpha_a101_f0035,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0036(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((((2.21 * c_pct_rank(m_corr((close - open), m_lag(volume, 1), 15))) + (0.7 * c_pct_rank((open - close)))) + (0.73 * c_pct_rank(m_rank(m_lag((-1 * returns), 6), 5)))) + c_pct_rank(abs(m_corr(vwap, adv20, 6)))) + (0.6 * c_pct_rank((((m_sum(close, 200) / 200) - open) * (close - open))))) 
            AS alpha_a101_f0036,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=240)

    def alpha_a101_f0037(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(m_lag((open - close), 1), close, 200)) + c_pct_rank((open - close))) 
            AS alpha_a101_f0037,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=240)

    def alpha_a101_f0038(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank(m_rank(close, 10))) * c_pct_rank((close / open))) 
            AS alpha_a101_f0038,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0039(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank((m_delta(close, 7) * (1 - c_pct_rank(m_decay_linear((volume / adv20), 9)))))) * (1 + c_pct_rank(m_sum(returns, 250)))) 
            AS alpha_a101_f0039,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=300)

    def alpha_a101_f0040(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank(m_stddev(high, 10))) * m_corr(high, volume, 10)) 
            AS alpha_a101_f0040,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0041(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((high * low)^0.5) - vwap) 
            AS alpha_a101_f0041,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0042(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank((vwap - close)) / c_pct_rank((vwap + close))) 
            AS alpha_a101_f0042,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0043(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_rank((volume / adv20), 20) * m_rank((-1 * m_delta(close, 7)), 8)) 
            AS alpha_a101_f0043,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0044(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(high, c_pct_rank(volume), 5)) 
            AS alpha_a101_f0044,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0045(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * ((c_pct_rank((m_sum(m_lag(close, 5), 20) / 20)) * m_corr(close, volume, 2)) * c_pct_rank(m_corr(m_sum(close, 5), m_sum(close, 20), 2)))) 
            AS alpha_a101_f0045,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0046(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(0.25 < (((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)), -1, IF((((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)) < 0, 1, -1 * (close - m_lag(close, 1)))) 
            AS alpha_a101_f0046,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0047(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((((c_pct_rank((1 / close)) * volume) / adv20) * ((high * c_pct_rank((high - close))) / (m_sum(high, 5) / 5))) - c_pct_rank((vwap - m_lag(vwap, 5)))) 
            AS alpha_a101_f0047,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0048(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_indneutralize(((m_corr(m_delta(close, 1), m_delta(m_lag(close, 1), 1), 250) * m_delta(close, 1)) / close), industry_level3_code) / m_sum(((m_delta(close, 1) / m_lag(close, 1))^2), 250)) 
            AS alpha_a101_f0048,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=270)

    def alpha_a101_f0049(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)) < -0.1, 1, -1 * (close - m_lag(close, 1))) 
            AS alpha_a101_f0049,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0050(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_max(c_pct_rank(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 5)), 5)) 
            AS alpha_a101_f0050,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0051(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)) < -0.05, 1, -1 * (close - m_lag(close, 1))) 
            AS alpha_a101_f0051,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0052(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((((-1 * m_min(low, 5)) + m_lag(m_min(low, 5), 5)) * c_pct_rank(((m_sum(returns, 240) - m_sum(returns, 20)) / 220))) * m_rank(volume, 5)) 
            AS alpha_a101_f0052,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=300)

    def alpha_a101_f0053(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_delta((((close - low) - (high - close)) / (close - low)), 9)) 
            AS alpha_a101_f0053,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0054(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5))) 
            AS alpha_a101_f0054,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0055(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(c_pct_rank(((close - m_min(low, 12)) / (m_max(high, 12) - m_min(low, 12)))), c_pct_rank(volume), 6)) 
            AS alpha_a101_f0055,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0056(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (0 - (1 * (c_pct_rank((m_sum(returns, 10) / m_sum(m_sum(returns, 2), 3))) * c_pct_rank((returns * float_market_cap))))) 
            AS alpha_a101_f0056,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0057(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (0 - (1 * ((close - vwap) / m_decay_linear(c_pct_rank(m_imax(close, 30)), 2)))) 
            AS alpha_a101_f0057,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0058(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_rank(m_decay_linear(m_corr(c_indneutralize(vwap, industry_level1_code), volume, 3.92795), 7.89291), 5.50322)) 
            AS alpha_a101_f0058,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0059(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_rank(m_decay_linear(m_corr(c_indneutralize(((vwap * 0.728317) + (vwap * (1 - 0.728317))), industry_level2_code), volume, 4.25197), 16.2289), 8.19648)) 
            AS alpha_a101_f0059,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0060(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (0 - (1 * ((2 * c_scale(c_pct_rank(((((close - low) - (high - close)) / (high - low)) * volume)),1)) - c_scale(c_pct_rank(m_imax(close, 10)),1)))) 
            AS alpha_a101_f0060,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0061(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank((vwap - m_min(vwap, 16.1219))) < c_pct_rank(m_corr(vwap, adv180, 17.9282))), 1, 0)  
            AS alpha_a101_f0061,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0062(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(vwap, m_sum(adv20, 22.4101), 9.91009)) < c_pct_rank(((c_pct_rank(open) + c_pct_rank(open)) < (c_pct_rank(((high + low) / 2)) + c_pct_rank(high)))))), 1, 0) * -1 
            AS alpha_a101_f0062,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0063(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_decay_linear(m_delta(c_indneutralize(close, industry_level2_code), 2.25164), 8.22237)) - c_pct_rank(m_decay_linear(m_corr(((vwap * 0.318108) + (open * (1 - 0.318108))), m_sum(adv180, 37.2467), 13.557), 12.2883))) * -1) 
            AS alpha_a101_f0063,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=300)

    def alpha_a101_f0064(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank(m_corr(m_sum(((open * 0.178404) + (low * (1 - 0.178404))), 12.7054), m_sum(adv120, 12.7054), 16.6208)) < c_pct_rank(m_delta(((((high + low) / 2) * 0.178404) + (vwap * (1 - 0.178404))), 3.69741))), -1, 0) 
            AS alpha_a101_f0064,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=150)

    def alpha_a101_f0065(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(((open * 0.00817205) + (vwap * (1 - 0.00817205))), m_sum(adv60, 8.6911), 6.40374)) < c_pct_rank((open - m_min(open, 13.635))))), 1, 0) * -1 
            AS alpha_a101_f0065,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0066(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_decay_linear(m_delta(vwap, 3.51013), 7.23052)) + m_rank(m_decay_linear(((((low * 0.96633) + (low * (1 - 0.96633))) - vwap) / (open - ((high + low) / 2))), 11.4157), 6.72611)) * -1) 
            AS alpha_a101_f0066,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=150)

    def alpha_a101_f0067(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank((high - m_min(high, 2.14593)))^c_pct_rank(m_corr(c_indneutralize(vwap, industry_level1_code), c_indneutralize(adv20, industry_level3_code), 6.02936))) * -1) 
            AS alpha_a101_f0067,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0068(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((m_rank(m_corr(c_pct_rank(high), c_pct_rank(adv15), 8.91644), 13.9333) < c_pct_rank(m_delta(((close * 0.518371) + (low * (1 - 0.518371))), 1.06157)))), 1, 0) * -1  
            AS alpha_a101_f0068,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0069(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_max(m_delta(c_indneutralize(vwap, industry_level2_code), 2.72412), 4.79344))^m_rank(m_corr(((close * 0.490655) + (vwap * (1 - 0.490655))), adv20, 4.92416), 9.0615)) * -1) 
            AS alpha_a101_f0069,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0070(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_delta(vwap, 1.29456))^m_rank(m_corr(c_indneutralize(close, industry_level2_code), adv50, 17.8256), 17.9171)) * -1) 
            AS alpha_a101_f0070,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0071(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            greatest(m_rank(m_decay_linear(m_corr(m_rank(close, 3.43976), m_rank(adv180, 12.0647), 18.0175), 4.20501), 15.6948), m_rank(m_decay_linear((c_pct_rank(((low + open) - (vwap + vwap)))^2), 16.4662), 4.4388)) 
            AS alpha_a101_f0071,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0072(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_decay_linear(m_corr(((high + low) / 2), adv40, 8.93345), 10.1519)) / c_pct_rank(m_decay_linear(m_corr(m_rank(vwap, 3.72469), m_rank(volume, 18.5188), 6.86671), 2.95011))) 
            AS alpha_a101_f0072,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0073(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 4.72775), 2.91864)), m_pct_rank(m_decay_linear(((m_delta(((open * 0.147155) + (low * (1 - 0.147155))), 2.03608) / ((open * 0.147155) + (low * (1 - 0.147155)))) * -1), 3.33829), 16.7411)) * -1 
            AS alpha_a101_f0073,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0074(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(close, m_sum(adv30, 37.4843), 15.1365)) < c_pct_rank(m_corr(c_pct_rank(((high * 0.0261661) + (vwap * (1 - 0.0261661)))), c_pct_rank(volume), 11.4791)))), 1, 0) * -1 
            AS alpha_a101_f0074,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0075(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank(m_corr(vwap, volume, 4.24304)) < c_pct_rank(m_corr(c_pct_rank(low), c_pct_rank(adv50), 12.4413))), 1, 0) 
            AS alpha_a101_f0075,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0076(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 1.24383), 11.8259)), m_pct_rank(m_decay_linear(m_rank(m_corr(c_indneutralize(low, industry_level1_code), adv81, 8.14941), 19.569), 17.1543), 19.383)) * -1 
            AS alpha_a101_f0076,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0077(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            least(c_pct_rank(m_decay_linear(((((high + low) / 2) + high) - (vwap + high)), 20.0451)), c_pct_rank(m_decay_linear(m_corr(((high + low) / 2), adv40, 3.1614), 5.64125))) 
            AS alpha_a101_f0077,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0078(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(m_sum(((low * 0.352233) + (vwap * (1 - 0.352233))), 19.7428), m_sum(adv40, 19.7428), 6.83313))^c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 5.77492))) 
            AS alpha_a101_f0078,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0079(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank(m_delta(c_indneutralize(((close * 0.60733) + (open * (1 - 0.60733))), industry_level1_code), 1.23438)) < c_pct_rank(m_corr(m_rank(vwap, 3.60973), m_rank(adv150, 9.18637), 14.6644))), 1, 0) 
            AS alpha_a101_f0079,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, 210)

    def alpha_a101_f0080(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(Sign(m_delta(c_indneutralize(((open * 0.868128) + (high * (1 - 0.868128))), industry_level2_code), 4.04545)))^m_rank(m_corr(high, adv10, 5.11456), 5.53756)) * -1) 
            AS alpha_a101_f0080,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)

    def alpha_a101_f0081(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(log(m_product(c_pct_rank((c_pct_rank(m_corr(vwap, m_sum(adv10, 49.6054), 8.47743))^4)), 14.9655))) < c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 5.07914)))),1,0) * -1  
            AS alpha_a101_f0081,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0082(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (least(c_pct_rank(m_decay_linear(m_delta(open, 1.46063), 14.8717)), m_rank(m_decay_linear(m_corr(c_indneutralize(volume, industry_level1_code), ((open * 0.634196) + (open * (1 - 0.634196))), 17.4842), 6.92131), 13.4283)) * -1) 
            AS alpha_a101_f0082,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0083(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_lag(((high - low) / (m_sum(close, 5) / 5)), 2)) * c_pct_rank(c_pct_rank(volume))) / (((high - low) / (m_sum(close, 5) / 5)) / (vwap - close))) 
            AS alpha_a101_f0083,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0084(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            power(m_rank((vwap - m_max(vwap, 15.3217)), 20.7127), c_pct_rank(m_delta(close, 4.96796)))  
            AS alpha_a101_f0084,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0085(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(((high * 0.876703) + (close * (1 - 0.876703))), adv30, 9.61331))^c_pct_rank(m_corr(m_rank(((high + low) / 2), 3.70596), m_rank(volume, 10.1595), 7.11408))) 
            AS alpha_a101_f0085,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0086(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((m_pct_rank(m_corr(close, m_sum(adv20, 14.7444), 6.00049), 20.4195) < c_pct_rank(((open + close) - (vwap + open)))), 1, 0) * -1 
            AS alpha_a101_f0086,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0087(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            greatest(c_pct_rank(m_decay_linear(m_delta(((close * 0.369701) + (vwap * (1 - 0.369701))), 1.91233), 2.65461)),m_pct_rank(m_decay_linear(abs(m_corr(c_indneutralize(adv81, industry_level2_code), close, 13.4132)), 4.89768), 14.4535)) * -1 
            AS alpha_a101_f0087,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=180)

    def alpha_a101_f0088(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            least(c_pct_rank(m_decay_linear(((c_pct_rank(open) + c_pct_rank(low)) - (c_pct_rank(high) + c_pct_rank(close))), 8.06882)), m_rank(m_decay_linear(m_corr(m_rank(close, 8.44728), m_rank(adv60, 20.6966), 8.01266), 6.65053), 2.61957)) 
            AS alpha_a101_f0088,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=180)

    def alpha_a101_f0089(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_rank(m_decay_linear(m_corr(((low * 0.967285) + (low * (1 - 0.967285))), adv10, 6.94279), 5.51607), 3.79744) - m_rank(m_decay_linear(m_delta(c_indneutralize(vwap, industry_level2_code), 3.48158), 10.1466), 15.3012)) 
            AS alpha_a101_f0089,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0090(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank((close - m_max(close, 4.66719)))^m_rank(m_corr(c_indneutralize(adv40, industry_level3_code), low, 5.38375), 3.21856)) * -1) 
            AS alpha_a101_f0090,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0091(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_rank(m_decay_linear(m_decay_linear(m_corr(c_indneutralize(close, industry_level2_code), volume, 9.74928), 16.398), 3.83219), 4.8667) - c_pct_rank(m_decay_linear(m_corr(vwap, adv30, 4.01303), 2.6809))) * -1) 
            AS alpha_a101_f0091,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0092(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            least(m_rank(m_decay_linear(((((high + low) / 2) + close) < (low + open)), 14.7221), 18.8683), m_rank(m_decay_linear(m_corr(c_pct_rank(low), c_pct_rank(adv30), 7.58555), 6.94024), 6.80584)) 
            AS alpha_a101_f0092,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0093(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_rank(m_decay_linear(m_corr(c_indneutralize(vwap, industry_level2_code), adv81, 17.4193), 19.848), 7.54455) / c_pct_rank(m_decay_linear(m_delta(((close * 0.524434) + (vwap * (1 - 0.524434))), 2.77377), 16.2664))) 
            AS alpha_a101_f0093,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=60)

    def alpha_a101_f0094(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank((vwap - m_min(vwap, 11.5783))) AS _part1,
            IF(m_rank(m_corr(m_rank(vwap, 19.6462), m_rank(adv60, 4.02992), 18.0926), 2.70756) IS NULL, 4, m_rank(m_corr(m_rank(vwap, 19.6462), m_rank(adv60, 4.02992), 18.0926), 2.70756)) AS _part2,
            _part1 ^ _part2 * -1 
            AS alpha_a101_f0094,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0095(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank((open - m_min(open, 12.4105))) < m_rank((c_pct_rank(m_corr(m_sum(((high + low) / 2), 19.1351), m_sum(adv40, 19.1351), 12.8742))^5), 11.7584)), 1, 0) 
            AS alpha_a101_f0095,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0096(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            greatest(m_rank(m_decay_linear(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 3.83878), 4.16783), 8.38151), m_rank(m_decay_linear(m_imax(m_corr(m_rank(close, 7.45404), m_rank(adv60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143)) * -1 
            AS alpha_a101_f0096,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0097(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(m_rank(m_corr(m_rank(low, 7.87871), m_rank(adv60, 17.255), 4.97547), 18.5925) IS NULL, 20, m_rank(m_corr(m_rank(low, 7.87871), m_rank(adv60, 17.255), 4.97547), 18.5925)) AS _part1,
            ((c_pct_rank(m_decay_linear(m_delta(c_indneutralize(((low * 0.721001) + (vwap * (1 - 0.721001))), industry_level2_code), 3.3705), 20.4523)) - m_rank(m_decay_linear(_part1, 15.7152), 6.71659)) * -1)  
            AS alpha_a101_f0097,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=120)

    def alpha_a101_f0098(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_decay_linear(m_corr(vwap, m_sum(adv5, 26.4719), 4.58418), 7.18088)) - c_pct_rank(m_decay_linear(m_rank(m_imin(m_corr(c_pct_rank(open), c_pct_rank(adv15), 20.8187), 8.62571), 6.95668), 8.07206))) 
            AS alpha_a101_f0098,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0099(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(m_sum(((high + low) / 2), 19.8975), m_sum(adv60, 19.8975), 8.8136)) < c_pct_rank(m_corr(low, volume, 6.28259)))), 1, 0) * -1  
            AS alpha_a101_f0099,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0100(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (0 - (1 * (((1.5 * c_scale(c_indneutralize(c_indneutralize(c_pct_rank((((close - low) - (high - close)) / (high - low)) * volume) * 100, industry_level3_code),industry_level3_code), 1)) - c_scale(c_indneutralize((m_corr(close, c_pct_rank(adv20), 5) - c_pct_rank(m_imin(close, 30))), industry_level3_code),1)) * (volume / adv20)))) 
            AS alpha_a101_f0100,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=90)

    def alpha_a101_f0101(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((close - open) / ((high - low) + 0.001))  
            AS alpha_a101_f0101,
        FROM data_base
        """
        return self.get_alpha_df(sql_alpha, lag=30)
