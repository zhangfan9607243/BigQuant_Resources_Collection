import dai
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class CNStockFactorsA191D1:
    
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
            WITH
            data_a AS (
                SELECT
                    date,
                    instrument,
                    open,
                    close,
                    high,
                    low,
                    amount,
                    volume,
                    close / m_lag(close, 1) - 1 AS ret,
                    amount / volume AS vwap,
                    industry_level1_code,
                    industry_level2_code,
                    industry_level3_code,
                    IF(open<=m_lag(open, 1), 0, greatest(high-open, open-m_lag(open, 1))) AS DTM, 
                    IF(open>=m_lag(open, 1), 0, greatest(open-low,  open-m_lag(open, 1))) AS DBM,
                    m_lag(low, 1) - low   AS LD, 
                    high - m_lag(high, 1) AS HD, 
                    greatest(greatest(high-low, ABS(high-m_lag(close, 1))), ABS(low-m_lag(close, 1))) AS TR, 
                FROM cn_stock_bar1d JOIN cn_stock_industry_component USING (date, instrument)
                WHERE instrument NOT LIKE '%BJ%'
                AND industry = 'sw2021'
            ),
            data_b AS (
                WITH 
                data_be AS (
                    SELECT
                        date,
                        instrument,
                        close / m_lag(close, 1) - 1 AS change_ratio,
                        float_market_cap,
                        total_owner_equity_lf,
                    FROM cn_stock_bar1d JOIN cn_stock_valuation USING (date, instrument) JOIN cn_stock_factors_financial_items USING (date, instrument)
                ),
                data1 AS ( 
                    SELECT 
                        date, 
                        instrument, 
                        change_ratio, 
                    FROM data_be 
                ), 
                data2 AS ( 
                    SELECT 
                        date, 
                        instrument,
                        c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS MKT
                    FROM data_be 
                ), 
                data3 AS (
                    WITH 
                    data3_0 AS (
                        SELECT
                            date,
                            instrument,
                            change_ratio,
                            float_market_cap,
                            c_pct_rank(float_market_cap)                          AS rank_sb,
                            c_pct_rank(float_market_cap / total_owner_equity_lf)  AS rank_lmh,
                            CASE
                                WHEN rank_sb  < 0.5 THEN 1
                                ELSE 2
                            END AS group_sb,
                            CASE
                                WHEN rank_lmh < 0.3 THEN 1
                                WHEN rank_lmh > 0.7 THEN 3
                                ELSE 2
                            END AS group_lmh,
                        FROM data_be
                    ),
                    data3_sl AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SL
                        FROM data3_0
                        WHERE group_sb = 1 AND group_lmh = 1
                    ),
                    data3_sm AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SM
                        FROM data3_0
                        WHERE group_sb = 1 AND group_lmh = 2
                    ),
                    data3_sh AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS SH
                        FROM data3_0
                        WHERE group_sb = 1 AND group_lmh = 3
                    ),
                    data3_bl AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BL
                        FROM data3_0
                        WHERE group_sb = 2 AND group_lmh = 1
                    ),
                    data3_bm AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BM
                        FROM data3_0
                        WHERE group_sb = 2 AND group_lmh = 2
                    ),
                    data3_bh AS (
                        SELECT DISTINCT
                            date,
                            c_sum(float_market_cap * change_ratio) / c_sum(float_market_cap) AS BH
                        FROM data3_0
                        WHERE group_sb = 2 AND group_lmh = 3
                    ),
                    data3_merge AS (
                        SELECT 
                            data3_0.date,
                            data3_0.instrument,
                            (1/3) * (SL + SM + SH) - (1/3) * (BL + BM + BH) AS SMB,
                            (1/2) * (SH + BH)      - (1/2) * (SL + BL)      AS HML,
                        FROM data3_0
                        JOIN data3_sl USING (date)
                        JOIN data3_sm USING (date)
                        JOIN data3_sh USING (date)
                        JOIN data3_bl USING (date)
                        JOIN data3_bm USING (date)
                        JOIN data3_bh USING (date)
                    )
                    SELECT * FROM data3_merge
                )
                SELECT 
                    date, 
                    instrument, 
                    MKT,
                    SMB, 
                    HML, 
                FROM data1 JOIN data2 USING (date, instrument) JOIN data3 USING (date, instrument)
            ),
            data_c AS (
                SELECT 
                    date,
                    close AS bm_close,
                    open  AS bm_open,
                FROM cn_stock_index_bar1d
                WHERE instrument = '000001.SH'
            )
            SELECT * 
            FROM data_a JOIN data_b USING (date, instrument) JOIN data_c USING (date)
            QUALIFY COLUMNS(*) IS NOT NULL
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

    def alpha_a191_f0001(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(c_pct_rank(m_delta(log(volume), 1)), c_pct_rank(((close -open) / open)), 6)) 
            AS alpha_a191_f0001
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0002(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_delta((((close -low) -(high -close)) / (high -low)), 1)) 
            AS alpha_a191_f0002
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0003(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum((IF(close=m_lag(close,1),0,close-(IF(close>m_lag(close,1),least(low,m_lag(close,1)),greatest(high,m_lag(close,1)))))),6) 
            AS alpha_a191_f0003
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0004(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF((((m_sum(close, 8) / 8) + m_stddev(close, 8)) < (m_sum(close, 2) / 2)), (-1 * 1), (IF(((m_sum(close, 2) / 2) < ((m_sum(close, 8) / 8) - m_stddev(close, 8))), 1, (IF(((1 < (volume / m_avg(volume,20))) OR ((volume / m_avg(volume,20)) == 1)), 1, (-1 * 1))))))) 
            AS alpha_a191_f0004
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0005(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_max(m_corr(m_rank(volume, 5), m_rank(high, 5), 5), 3)) 
            AS alpha_a191_f0005
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0006(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(sign(m_delta((((open * 0.85) + (high * 0.15))), 4)))* -1) 
            AS alpha_a191_f0006
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0007(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_max((vwap -close), 3)) + c_pct_rank(m_min((vwap -close), 3))) * c_pct_rank(m_delta(volume, 3))) 
            AS alpha_a191_f0007
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0008(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(m_delta(((((high + low) / 2) * 0.2) + (vwap * 0.8)), 4) * -1) 
            AS alpha_a191_f0008
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0009(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(((high+low)/2-(m_lag(high,1)+m_lag(low,1))/2)*(high-low)/volume,7,2) 
            AS alpha_a191_f0009
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0010(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_max(IF(ret < 0, m_stddev(ret, 20), close)^2, 5))) 
            AS alpha_a191_f0010
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0011(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(((close-low)-(high-close))/(high-low)*volume,6) 
            AS alpha_a191_f0011
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0012(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank((open -(m_sum(vwap, 10) / 10)))) * (-1 * (c_pct_rank(abs((close -vwap))))) 
            AS alpha_a191_f0012
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0013(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((high * low)^0.5) -vwap) 
            AS alpha_a191_f0013
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0014(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            close-m_lag(close,5) 
            AS alpha_a191_f0014
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0015(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            open/m_lag(close,1)-1 
            AS alpha_a191_f0015
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0016(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_max(c_pct_rank(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 5)), 5)) 
            AS alpha_a191_f0016
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0017(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank((vwap - m_max(vwap, 15)))^m_delta(close, 5) 
            AS alpha_a191_f0017
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0018(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            close/m_lag(close,5) 
            AS alpha_a191_f0018
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0019(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(close < m_lag(close, 5), (close - m_lag(close, 5)) / m_lag(close, 5), IF(close = m_lag(close, 5), 0, (close - m_lag(close, 5)) / close))) 
            AS alpha_a191_f0019
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0020(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_lag(close,6))/m_lag(close,6)*100 
            AS alpha_a191_f0020
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0021(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_slope(m_avg(close,6), row_number(6), 6) 
            AS alpha_a191_f0021
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0022(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(((close-m_avg(close,6))/m_avg(close,6)-m_lag((close-m_avg(close,6))/m_avg(close,6),3)),12,1) 
            AS alpha_a191_f0022
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0023(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) / (m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) + m_ta_ewm(IF(close <= m_lag(close, 1), m_stddev(close, 20), 0), 20, 1))) * 100 
            AS alpha_a191_f0023
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0024(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(close-m_lag(close,5),5,1) 
            AS alpha_a191_f0024
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0025(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank((m_delta(close, 7) * (1 -c_pct_rank(m_decay_linear((volume/m_avg(volume,20)), 9)))))) * (1 +c_pct_rank(m_sum(ret, 250)))) 
            AS alpha_a191_f0025
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0026(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((((m_sum(close, 7) / 7) -close)) + ((m_corr(vwap, m_lag(close, 5), 230)))) AS alpha_a191_f0026
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0027(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_wma((close-m_lag(close,3))/m_lag(close,3)*100+(close-m_lag(close,6))/m_lag(close,6)*100,12) 
            AS alpha_a191_f0027
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0028(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            3*m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1)-2*m_ta_ewm(m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_max(low,9))*100,3,1),3,1) 
            AS alpha_a191_f0028
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0029(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_lag(close,6))/m_lag(close,6)*volume 
            AS alpha_a191_f0029
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0030(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_wma((m_ols3d_last_resid(close/m_lag(close,1)-1,MKT,SMB,HML,60))^2,20) 
            AS alpha_a191_f0030
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0031(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_avg(close,12))/m_avg(close,12)*100 
            AS alpha_a191_f0031
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0032(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_sum(c_pct_rank(m_corr(c_pct_rank(high), c_pct_rank(volume), 3)), 3)) 
            AS alpha_a191_f0032
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0033(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((((-1 * m_min(low, 5)) + m_lag(m_min(low, 5), 5)) * c_pct_rank(((m_sum(ret, 240) -m_sum(ret, 20)) / 220))) *m_rank(volume, 5)) 
            AS alpha_a191_f0033
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0034(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(close,12)/close AS alpha_a191_f0034
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0035(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (least(c_pct_rank(m_decay_linear(m_delta(open, 1), 15)), c_pct_rank(m_decay_linear(m_corr((volume), ((open * 0.65) +(open *0.35)), 17),7))) * -1) AS alpha_a191_f0035
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0036(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(m_sum(m_corr(c_pct_rank(volume), c_pct_rank(vwap), 6), 2)) 
            AS alpha_a191_f0036
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0037(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(((m_sum(open, 5) * m_sum(ret, 5)) - m_lag((m_sum(open,5) * m_sum(ret, 5)), 10)))) 
            AS alpha_a191_f0037
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0038(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(((m_sum(high, 20) / 20) < high), (-1 * m_delta(high, 2)), 0)) 
            AS alpha_a191_f0038
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0039(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_decay_linear(m_delta((close), 2),8)) -c_pct_rank(m_decay_linear(m_corr(((vwap * 0.3) + (open * 0.7)),m_sum(m_avg(volume,180), 37), 14), 12))) * -1) 
            AS alpha_a191_f0039
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0040(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_sum(IF(close > m_lag(close, 1), volume, 0), 26) / m_sum(IF(close <= m_lag(close, 1), volume, 0), 26)) * 100 
            AS alpha_a191_f0040
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0041(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_max(m_delta((vwap), 3), 5))* -1) 
            AS alpha_a191_f0041
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0042(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank(m_stddev(high, 10))) * m_corr(high, volume, 10)) 
            AS alpha_a191_f0042
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0043(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close > m_lag(close, 1), volume, IF(close < m_lag(close, 1), -volume, 0)), 6) 
            AS alpha_a191_f0043
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0044(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_rank(m_decay_linear(m_corr(((low )), m_avg(volume,10), 7), 6),4) + m_rank(m_decay_linear(m_delta((vwap),3), 10), 15))  
            AS alpha_a191_f0044
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0045(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_delta((((close * 0.6) + (open *0.4))), 1)) * c_pct_rank(m_corr(vwap, m_avg(volume,150), 15))) 
            AS alpha_a191_f0045
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 180)

    def alpha_a191_f0046(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg(close,3)+m_avg(close,6)+m_avg(close,12)+m_avg(close,24))/(4*close) 
            AS alpha_a191_f0046
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0047(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,9,1) 
            AS alpha_a191_f0047
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0048(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1*((c_pct_rank(((sign((close -m_lag(close, 1))) + sign((m_lag(close, 1) -m_lag(close, 2)))) +sign((m_lag(close, 2) -m_lag(close, 3)))))) * m_sum(volume, 5)) / m_sum(volume, 20)) 
            AS alpha_a191_f0048
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0049(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12)) 
            AS alpha_a191_f0049
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0050(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12))) - (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12))) 
            AS alpha_a191_f0050
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0051(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) / (m_sum(IF((high+low)<=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12) + m_sum(IF((high+low)>=(m_lag(high,1)+m_lag(low,1)), 0, greatest(abs(high-m_lag(high,1)),abs(low-m_lag(low,1)))), 12)) AS alpha_a191_f0051
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0052(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(greatest(0,high-m_lag((high+low+close)/3,1)),26)/m_sum(greatest(0,m_lag((high+low+close)/3,1)-low),26)*100 
            AS alpha_a191_f0052
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0053(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close > m_lag(close, 1), 1, 0), 12) / 12 * 100 
            AS alpha_a191_f0053
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0054(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank((m_stddev(abs(close -open), 10) + (close - open)) + m_corr(close, open, 10))) 
            AS alpha_a191_f0054
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0055(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(16*(close-m_lag(close,1)+(close-open)/2+m_lag(close,1)-m_lag(open,1))/(IF((abs(high-m_lag(close,1))>abs(low-m_lag(close,1)) AND abs(high-m_lag(close,1))>abs(high-m_lag(low,1))),abs(high-m_lag(close,1))+abs(low-m_lag(close,1))/2+abs(m_lag(close,1)-m_lag(open,1))/4,IF((abs(low-m_lag(close,1))>abs(high-m_lag(low,1)) AND abs(low-m_lag(close,1))>abs(high-m_lag(close,1))),abs(low-m_lag(close,1))+abs(high-m_lag(close,1))/2+abs(m_lag(close,1)-m_lag(open,1))/4,abs(high-m_lag(low,1))+abs(m_lag(close,1)-m_lag(open,1))/4)))*greatest(abs(high-m_lag(close,1)),abs(low-m_lag(close,1))),20) 
            AS alpha_a191_f0055
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0056(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF((c_pct_rank((open - m_min(open, 12))) < c_pct_rank((c_pct_rank(m_corr(m_sum(((high + low) / 2), 19), m_sum(m_avg(volume,40), 19), 13))^5))), 1, 0) * -1 AS alpha_a191_f0056
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0057(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1) 
            AS alpha_a191_f0057
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0058(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close > m_lag(close, 1), 1, 0), 20) / 20 * 100 
            AS alpha_a191_f0058
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0059(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum((IF(close=m_lag(close,1),0,close-(IF(close>m_lag(close,1),least(low,m_lag(close,1)),greatest(high,m_lag(close,1)))))),20) 
            AS alpha_a191_f0059
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0060(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(((close-low)-(high-close))/(high-low)*volume,20) 
            AS alpha_a191_f0060
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0061(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 1), 12)),c_pct_rank(m_decay_linear(c_pct_rank(m_corr((low),m_avg(volume,80), 8)), 17))) * -1) 
            AS alpha_a191_f0061
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0062(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(high, c_pct_rank(volume), 5)) 
            AS alpha_a191_f0062
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0063(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(greatest(close-m_lag(close,1),0),6,1)/m_ta_ewm(abs(close-m_lag(close,1)),6,1)*100 
            AS alpha_a191_f0063
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0064(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (greatest(c_pct_rank(m_decay_linear(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 4), 4)),c_pct_rank(m_decay_linear(greatest(m_corr(c_pct_rank(close), c_pct_rank(m_avg(volume,60)), 4), 13), 14))) * -1) 
            AS alpha_a191_f0064
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0065(self):
        sql_alpha = f"""
            SELECT
                date,
                instrument,
                m_avg(close,6)/close 
                AS alpha_a191_f0065
            FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0066(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_avg(close,6))/m_avg(close,6)*100 
            AS alpha_a191_f0066
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0067(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(greatest(close-m_lag(close,1),0),24,1)/m_ta_ewm(abs(close-m_lag(close,1)),24,1)*100 
            AS alpha_a191_f0067
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0068(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(((high+low)/2-(m_lag(high,1)+m_lag(low,1))/2)*(high-low)/volume,15,2) 
            AS alpha_a191_f0068
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0069(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF(m_sum(DTM,20)>m_sum(DBM,20),(m_sum(DTM,20)-m_sum(DBM,20))/m_sum(DTM,20),IF(m_sum(DTM,20)=m_sum(DBM,20),0,(m_sum(DTM,20)-m_sum(DBM,20))/m_sum(DBM,20)))) 
            AS alpha_a191_f0069
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0070(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(amount,6) 
            AS alpha_a191_f0070
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0071(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_avg(close,24))/m_avg(close,24)*100 
            AS alpha_a191_f0071
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0072(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,15,1) 
            AS alpha_a191_f0072
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0073(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_rank(m_decay_linear(m_decay_linear(m_corr((close), volume, 10), 16), 4), 5) - c_pct_rank(m_decay_linear(m_corr(vwap, m_avg(volume,30), 4),3))) * -1) 
            AS alpha_a191_f0073
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0074(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(m_sum(((low * 0.35) + (vwap * 0.65)), 20), m_sum(m_avg(volume,40), 20), 7)) + c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 6))) 
            AS alpha_a191_f0074
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 180)

    def alpha_a191_f0075(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close > open AND bm_close < bm_open, 1, 0), 50) / m_sum(IF(bm_close < bm_open, 1, 0), 50) 
            AS alpha_a191_f0075
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0076(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(abs((close/m_lag(close,1)-1))/volume,20)/m_avg(abs((close/m_lag(close,1)-1))/volume,20) 
            AS alpha_a191_f0076
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0077(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            least(c_pct_rank(m_decay_linear(((((high + low) / 2) + high) -(vwap + high)), 20)),c_pct_rank(m_decay_linear(m_corr(((high + low) / 2), m_avg(volume,40), 3), 6))) 
            AS alpha_a191_f0077
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0078(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((high+low+close)/3-m_avg((high+low+close)/3,12))/(0.015*m_avg(abs(close-m_avg((high+low+close)/3,12)),12)) 
            AS alpha_a191_f0078
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0079(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(greatest(close-m_lag(close,1),0),12,1)/m_ta_ewm(abs(close-m_lag(close,1)),12,1)*100 
            AS alpha_a191_f0079
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0080(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (volume-m_lag(volume,5))/m_lag(volume,5)*100 
            AS alpha_a191_f0080
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0081(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(volume,21,2) 
            AS alpha_a191_f0081
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0082(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm((m_max(high,6)-close)/(m_max(high,6)-m_min(low,6))*100,20,1) 
            AS alpha_a191_f0082
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0083(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(m_covar_samp(c_pct_rank(high), c_pct_rank(volume), 5))) 
            AS alpha_a191_f0083
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0084(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close>m_lag(close,1),volume,IF(close<m_lag(close,1),-volume,0)),20) 
            AS alpha_a191_f0084
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0085(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_rank((volume / m_avg(volume,20)), 20) * m_rank((-1 * m_delta(close, 7)), 8)) 
            AS alpha_a191_f0085
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0086(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF((0.25 < (((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10))), (-1 * 1), (IF((((m_lag(close, 20) - m_lag(close, 10)) / 10) - ((m_lag(close, 10) - close) / 10)) < 0, 1, ((-1 * 1) * (close - m_lag(close, 1))))))) 
            AS alpha_a191_f0086
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0087(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_decay_linear(m_delta(vwap, 4), 7)) + m_pct_rank(m_decay_linear(((((low * 0.9) + (low * 0.1)) -vwap) /(open -((high + low) / 2))), 11), 7)) * -1) 
            AS alpha_a191_f0087
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0088(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_lag(close,20))/m_lag(close,20)*100 
            AS alpha_a191_f0088
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0089(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            2*(m_ta_ewm(close,13,2)-m_ta_ewm(close,27,2)-m_ta_ewm(m_ta_ewm(close,13,2)-m_ta_ewm(close,27,2),10,2)) 
            AS alpha_a191_f0089
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0090(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(c_pct_rank(vwap), c_pct_rank(volume), 5)) * -1) 
            AS alpha_a191_f0090
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0091(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank((close - m_max(close, 5)))*c_pct_rank(m_corr((m_avg(volume,40)), low, 5))) * -1) 
            AS alpha_a191_f0091
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0092(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (greatest(c_pct_rank(m_decay_linear(m_delta(((close * 0.35) + (vwap *0.65)), 2), 3)),m_rank(m_decay_linear(abs(m_corr((m_avg(volume,180)), close, 13)), 5), 15)) * -1) 
            AS alpha_a191_f0092
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 240)

    def alpha_a191_f0093(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(open>=m_lag(open,1),0,greatest((open-low),(open-m_lag(open,1)))),20) 
            AS alpha_a191_f0093
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0094(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close > m_lag(close, 1), volume, IF(close < m_lag(close, 1), -volume, 0)), 30) 
            AS alpha_a191_f0094
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0095(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(amount,20) 
            AS alpha_a191_f0095
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0096(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(m_ta_ewm((close-m_min(low,9))/(m_max(high,9)-m_min(low,9))*100,3,1),3,1) 
            AS alpha_a191_f0096
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0097(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(volume,10) 
            AS alpha_a191_f0097
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0098(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (IF((((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) < 0.05) OR ((m_delta((m_sum(close, 100) / 100), 100) / m_lag(close, 100)) == 0.05)), (-1 * (close - m_min(close, 100))), (-1 * m_delta(close, 3)))) 
            AS alpha_a191_f0098
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 150)

    def alpha_a191_f0099(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * c_pct_rank(m_covar_samp(c_pct_rank(close), c_pct_rank(volume), 5))) 
            AS alpha_a191_f0099
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0100(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_stddev(volume,20)  
            AS alpha_a191_f0100
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0101(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(close, m_sum(m_avg(volume,30), 37), 15)) < c_pct_rank(m_corr(c_pct_rank(((high * 0.1) + (vwap * 0.9))),c_pct_rank(volume), 11)))),1,0) * -1 
            AS alpha_a191_f0101
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0102(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(greatest(volume-m_lag(volume,1),0),6,1)/m_ta_ewm(abs(volume-m_lag(volume,1)),6,1)*100 
            AS alpha_a191_f0102
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0103(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((20-m_imin(low,20))/20)*100 
            AS alpha_a191_f0103
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0104(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * (m_delta(m_corr(high, volume, 5), 5) * c_pct_rank(m_stddev(close, 20)))) 
            AS alpha_a191_f0104
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0105(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(c_pct_rank(open), c_pct_rank(volume), 10)) 
            AS alpha_a191_f0105
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0106(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            close-m_lag(close,20) 
            AS alpha_a191_f0106
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0107(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((-1 * c_pct_rank((open -m_lag(high, 1)))) * c_pct_rank((open -m_lag(close, 1)))) *c_pct_rank((open -m_lag(low, 1)))) 
            AS alpha_a191_f0107
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0108(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank((high - m_min(high, 2)))^c_pct_rank(m_corr((vwap), (m_avg(volume,120)), 6))) * -1) 
            AS alpha_a191_f0108
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 150)

    def alpha_a191_f0109(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(high-low,10,2)/m_ta_ewm(m_ta_ewm(high-low,10,2),10,2) 
            AS alpha_a191_f0109
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0110(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(greatest(0,high-m_lag(close,1)),20)/m_sum(greatest(0,m_lag(close,1)-low),20)*100 
            AS alpha_a191_f0110
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0111(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(volume*((close-low)-(high-close))/(high-low),11,2)-m_ta_ewm(volume*((close-low)-(high-close))/(high-low),4,2) 
            AS alpha_a191_f0111
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0112(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_sum(IF(close-m_lag(close,1)>0,close-m_lag(close,1),0),12)-m_sum(IF(close-m_lag(close,1)<0,abs(close-m_lag(close,1)),0),12)) / (m_sum(IF(close-m_lag(close,1)>0,close-m_lag(close,1),0),12)+m_sum(IF(close-m_lag(close,1)<0,abs(close-m_lag(close,1)),0),12)))*100 
            AS alpha_a191_f0112
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0113(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * ((c_pct_rank((m_sum(m_lag(close, 5), 20) / 20)) * m_corr(close, volume, 2)) * c_pct_rank(m_corr(m_sum(close, 5),m_sum(close, 20), 2)))) 
            AS alpha_a191_f0113
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0114(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_lag(((high -low) / (m_sum(close, 5) / 5)), 2)) * c_pct_rank(c_pct_rank(volume))) / (((high -low) /(m_sum(close, 5) / 5)) / (vwap -close))) 
            AS alpha_a191_f0114
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0115(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(((high * 0.9) + (close * 0.1)), m_avg(volume,30), 10))^c_pct_rank(m_corr(m_rank(((high + low) /2), 4), m_rank(volume, 10), 7))) 
            AS alpha_a191_f0115
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0116(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_slope(close,row_number(20),20) 
            AS alpha_a191_f0116
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0117(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_rank(volume, 32) * (1 -m_rank(((close + high) -low), 16))) * (1 -m_rank(ret, 32))) 
            AS alpha_a191_f0117
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0118(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(high-open,20)/m_sum(open-low,20)*100 
            AS alpha_a191_f0118
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0119(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_decay_linear(m_corr(vwap, m_sum(m_avg(volume,5), 26), 5), 7)) -c_pct_rank(m_decay_linear(m_rank(m_min(m_corr(c_pct_rank(open), c_pct_rank(m_avg(volume,15)), 21), 9), 7), 8))) 
            AS alpha_a191_f0119
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0120(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank((vwap -close)) / c_pct_rank((vwap + close))) 
            AS alpha_a191_f0120
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0121(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank((vwap - m_min(vwap, 12)))^m_rank(m_corr(m_rank(vwap, 20), m_rank(m_avg(volume,60), 2), 18), 3)) * -1) 
            AS alpha_a191_f0121
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0122(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2)-m_lag(m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2),1))/m_lag(m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2),1) 
            AS alpha_a191_f0122
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0123(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr(m_sum(((high + low) / 2), 20), m_sum(m_avg(volume,60), 20), 9))<c_pct_rank(m_corr(low, volume,6)))), 1, 0) * -1 
            AS alpha_a191_f0123
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0124(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close -vwap) / m_decay_linear(c_pct_rank(m_max(close, 30)),2) 
            AS alpha_a191_f0124
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0125(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_decay_linear(m_corr((vwap), m_avg(volume,80),17), 20)) / c_pct_rank(m_decay_linear(m_delta(((close * 0.5)+ (vwap * 0.5)), 3), 16))) 
            AS alpha_a191_f0125
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0126(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close+high+low)/3 
            AS alpha_a191_f0126
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0127(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg((100*(close-m_max(close,12))/(m_max(close,12)))^2, 12))^(1/2) 
            AS alpha_a191_f0127
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0128(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            100 - (100 / (1 + m_sum(IF(((high+low+close)/3 > m_lag((high+low+close)/3,1)), ((high+low+close)/3 * volume), 0), 14) / m_sum(IF(((high+low+close)/3 < m_lag((high+low+close)/3,1)), ((high+low+close)/3 * volume), 0), 14))) 
            AS alpha_a191_f0128
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0129(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close - m_lag(close, 1) < 0, abs(close - m_lag(close, 1)), 0), 12) 
            AS alpha_a191_f0129
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0130(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(close, m_avg(volume, 60), 10)) - c_pct_rank(m_corr(close, m_avg(volume, 10), 6))) 
            AS alpha_a191_f0130
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0131(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_delta(vwap, 1))^m_rank(m_corr(close,m_avg(volume,50), 18), 18)) 
            AS alpha_a191_f0131
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0132(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(amount, 20) 
            AS alpha_a191_f0132
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0133(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((20 - m_imax(high, 20)) / 20) * 100 - ((20 - m_imin(low, 20)) / 20) * 100 
            AS alpha_a191_f0133
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0134(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close - m_lag(close, 12)) / m_lag(close, 12) * volume 
            AS alpha_a191_f0134
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0135(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(m_lag(close / m_lag(close, 20), 1), 20, 1) 
            AS alpha_a191_f0135
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0136(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * c_pct_rank(m_delta(ret, 3))) * m_corr(open, volume, 10)) 
            AS alpha_a191_f0136
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0137(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            16 * (((close - m_lag(close, 1) + (close - open) / 2 + m_lag(close, 1) - m_lag(open, 1)) / (IF((abs(high - m_lag(close, 1)) > abs(low - m_lag(close, 1)) AND abs(high - m_lag(close, 1)) > abs(high - m_lag(low, 1))), (abs(high - m_lag(close, 1)) + abs(low - m_lag(close, 1)) / 2 + abs(m_lag(close, 1) - m_lag(open, 1)) / 4), IF((abs(low - m_lag(close, 1)) > abs(high - m_lag(low, 1)) AND abs(low - m_lag(close, 1)) > abs(high - m_lag(close, 1))), (abs(low - m_lag(close, 1)) + abs(high - m_lag(close, 1)) / 2 + abs(m_lag(close, 1) - m_lag(open, 1)) / 4), (abs(high - m_lag(low, 1)) + abs(m_lag(close, 1) - m_lag(open, 1)) / 4))))) * greatest(abs(high - m_lag(close, 1)), abs(low - m_lag(close, 1)))) 
            AS alpha_a191_f0137
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0138(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((c_pct_rank(m_decay_linear(m_delta((((low * 0.7) + (vwap * 0.3))), 3), 20)) - m_rank(m_decay_linear(m_rank(m_corr(m_rank(low, 8), m_rank(m_avg(volume, 60), 17), 5), 19), 16), 7)) * -1)
            AS alpha_a191_f0138
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0139(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * m_corr(open, volume, 10)) 
            AS alpha_a191_f0139
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0140(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            least(c_pct_rank(m_decay_linear(((c_pct_rank(open) + c_pct_rank(low)) - (c_pct_rank(high) + c_pct_rank(close))), 8)), m_rank(m_decay_linear(m_corr(m_rank(close, 8), m_rank(m_avg(volume, 60), 20), 8), 7), 3))) 
            AS alpha_a191_f0140
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0141(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(c_pct_rank(high), c_pct_rank(m_avg(volume, 15)), 9)) * -1)
            AS alpha_a191_f0141
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0142(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((-1 * c_pct_rank(m_rank(close, 10))) * c_pct_rank(m_delta(m_delta(close, 1), 1))) * c_pct_rank(m_rank((volume / m_avg(volume, 20)), 5))) 
            AS alpha_a191_f0142
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0143(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_product(IF(close / m_lag(close, 1) > 1, close / m_lag(close, 1), 1), 252) 
            AS alpha_a191_f0143
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0144(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close < m_lag(close, 1), abs(close / m_lag(close, 1) - 1) / amount, 0), 20) / m_sum(IF(close < m_lag(close, 1), 1, 0), 20) 
            AS alpha_a191_f0144
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0145(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg(volume, 9) - m_avg(volume, 26)) / m_avg(volume, 12) * 100 
            AS alpha_a191_f0145
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0146(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2), 20) * ((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2)) / m_ta_ewm(((close - m_lag(close, 1)) / m_lag(close, 1) - ((close - m_lag(close, 1)) / m_lag(close, 1) - m_ta_ewm((close - m_lag(close, 1)) / m_lag(close, 1), 61, 2))) ^ 2, 60, 2) 
            AS alpha_a191_f0146
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0147(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_slope(m_avg(close, 12), row_number(12), 12) 
            AS alpha_a191_f0147
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0148(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((c_pct_rank(m_corr((open), m_sum(m_avg(volume, 60), 9), 6)) < c_pct_rank((open - m_min(open, 14))))), 1, 0) * -1 
            AS alpha_a191_f0148
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0149(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_regr_slope((close / m_lag(close, 1) - 1), (bm_close / m_lag(bm_close, 1) - 1), 252) 
            AS alpha_a191_f0149
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 300)

    def alpha_a191_f0150(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close + high + low) / 3 * volume 
            AS alpha_a191_f0150
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0151(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(close - m_lag(close, 20), 20, 1) 
            AS alpha_a191_f0151
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 20)

    def alpha_a191_f0152(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(m_avg(m_lag(m_ta_ewm(m_lag(close / m_lag(close, 9), 1), 9, 1), 1), 12) - m_avg(m_lag(m_ta_ewm(m_lag(close / m_lag(close, 9), 1), 9, 1), 1), 26), 9, 1) 
            AS alpha_a191_f0152
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0153(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_avg(close, 3) + m_avg(close, 6) + m_avg(close, 12) + m_avg(close, 24)) / 4 
            AS alpha_a191_f0153
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 45)

    def alpha_a191_f0154(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(((vwap - m_min(vwap, 16)) < (m_corr(vwap, m_avg(volume, 180), 18))), 1, 0) 
            AS alpha_a191_f0154
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 270)

    def alpha_a191_f0155(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(volume, 13, 2) - m_ta_ewm(volume, 27, 2) - m_ta_ewm(m_ta_ewm(volume, 13, 2) - m_ta_ewm(volume, 27, 2), 10, 2) 
            AS alpha_a191_f0155
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0156(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (greatest(c_pct_rank(m_decay_linear(m_delta(vwap, 5), 3)), c_pct_rank(m_decay_linear(((m_delta(((open * 0.15) + (low * 0.85)), 2) / ((open * 0.15) + (low * 0.85))) * -1), 3))) * -1) 
            AS alpha_a191_f0156
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0157(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_min(m_product(c_pct_rank(c_pct_rank(log(m_sum(m_min(c_pct_rank(c_pct_rank((-1 * c_pct_rank(m_delta((close - 1), 5))))), 2), 1)))), 1), 5) + m_rank(m_lag((-1 * ret), 6), 5)) 
            AS alpha_a191_f0157
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0158(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((high - m_ta_ewm(close, 15, 2)) - (low - m_ta_ewm(close, 15, 2))) / close 
            AS alpha_a191_f0158
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0159(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((close - m_sum(least(low, m_lag(close, 1)), 6)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 6) * 12 * 24 + (close - m_sum(least(low, m_lag(close, 1)), 12)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 12) * 6 * 24 + (close - m_sum(least(low, m_lag(close, 1)), 24)) / m_sum(greatest(high, m_lag(close, 1)) - least(low, m_lag(close, 1)), 24) * 6 * 24) * 100 / (6 * 12 + 6 * 24 + 12 * 24) 
            AS alpha_a191_f0159
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0160(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(IF(close <= m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) 
            AS alpha_a191_f0160
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0161(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(greatest(greatest((high - low), abs(m_lag(close, 1) - high)), abs(m_lag(close, 1) - low)), 12) 
            AS alpha_a191_f0161
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0162(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100 - m_min(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12)) / (m_max(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12) -  m_min(m_ta_ewm(greatest(close - m_lag(close, 1), 0), 12, 1) / m_ta_ewm(abs(close - m_lag(close, 1)), 12, 1) * 100, 12)) 
            AS alpha_a191_f0162
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0163(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank(((((-1 * ret) * m_avg(volume, 20)) * vwap) * (high - close))) 
            AS alpha_a191_f0163
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0164(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm((IF((close > m_lag(close, 1)), 1 / (close - m_lag(close, 1)), 1) -  m_min(IF((close > m_lag(close, 1)), 1 / (close - m_lag(close, 1)), 1), 12)) / (high - low) * 100, 13, 2) 
            AS alpha_a191_f0164
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0165(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_max(c_sum(close - m_avg(close, 48)), 48) -  m_min(c_sum(close - m_avg(close, 48)), 48) / m_stddev(close, 48) 
            AS alpha_a191_f0165
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 150)

    def alpha_a191_f0166(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            -20 * ((20 - 1) ^ 1.5 * m_sum(close / m_lag(close, 1) - 1 - m_avg(close / m_lag(close, 1) - 1, 20), 20)) / ((20 - 1) * (20 - 2) * (m_sum(m_avg(close / m_lag(close, 1), 20) ^ 2, 20)) ^ 1.5) 
            AS alpha_a191_f0166
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0167(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(close - m_lag(close, 1) > 0, close - m_lag(close, 1), 0), 12) 
            AS alpha_a191_f0167
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0168(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (-1 * volume / m_avg(volume, 20)) 
            AS alpha_a191_f0168
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0169(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(m_avg(m_lag(m_ta_ewm(close - m_lag(close, 1), 9, 1), 1), 12) - m_avg(m_lag(m_ta_ewm(close - m_lag(close, 1), 9, 1), 1), 26), 10, 1) 
            AS alpha_a191_f0169
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)
    
    def alpha_a191_f0170(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((((c_pct_rank((1 / close)) * volume) / m_avg(volume,20)) * ((high * c_pct_rank((high -close))) / (m_sum(high, 5) /5))) -c_pct_rank((vwap - m_lag(vwap, 5))))
            AS alpha_a191_f0170
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0171(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((-1 * ((low -close) * (open^5))) / ((close -high) * (close^5))) 
            AS alpha_a191_f0171
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0172(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(abs((m_sum(IF(LD > 0 AND LD > HD, LD, 0), 14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0), 14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0), 14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0), 14) * 100 / m_sum(TR, 14))) * 100, 6) 
            AS alpha_a191_f0172
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0173(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            3*m_ta_ewm(close,13,2)-2*m_ta_ewm(m_ta_ewm(close,13,2),13,2)+m_ta_ewm(m_ta_ewm(m_ta_ewm(log(close),13,2),13,2),13,2) 
            AS alpha_a191_f0173
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0174(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_ta_ewm(IF(close > m_lag(close, 1), m_stddev(close, 20), 0), 20, 1) 
            AS alpha_a191_f0174
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0175(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(greatest(greatest((high-low),abs(m_lag(close,1)-high)),abs(m_lag(close,1)-low)),6) 
            AS alpha_a191_f0175
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0176(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_corr(c_pct_rank(((close -m_min(low, 12)) / (m_max(high, 12) - m_min(low,12)))),c_pct_rank(volume), 6) 
            AS alpha_a191_f0176
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0177(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((20-m_imax(high,20))/20)*100 
            AS alpha_a191_f0177
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0178(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (close-m_lag(close,1))/m_lag(close,1)*volume 
            AS alpha_a191_f0178
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0179(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(vwap, volume, 4)) * c_pct_rank(m_corr(c_pct_rank(low),c_pct_rank(m_avg(volume,50)), 12))) 
            AS alpha_a191_f0179
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 120)

    def alpha_a191_f0180(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            IF(m_avg(volume, 20) < volume, (-1 * m_rank(abs(m_delta(close, 7)), 60)) * sign(m_delta(close, 7)), -1 * volume) 
            AS alpha_a191_f0180
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0181(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(((close/m_lag(close,1)-1)-m_avg((close/m_lag(close,1)-1),20))-(bm_close-m_avg(bm_close,20))^2,20)/m_sum((bm_close-m_avg(bm_close,20))^3,20) 
            AS alpha_a191_f0181
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0182(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF((close > open AND bm_close > bm_open) OR (close < open AND bm_close < bm_open), 1, 0), 20) / 20 
            AS alpha_a191_f0182
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0183(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_max(c_sum(close-m_avg(close,24)),24)-m_min(c_sum(close-m_avg(close,24)),24)/m_stddev(close,24) 
            AS alpha_a191_f0183
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0184(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (c_pct_rank(m_corr(m_lag((open -close), 1), close, 200)) + c_pct_rank((open -close))) 
            AS alpha_a191_f0184
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 240)

    def alpha_a191_f0185(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            c_pct_rank((-1 * ((1 -(open / close))^2))) 
            AS alpha_a191_f0185
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 15)

    def alpha_a191_f0186(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (m_lag(m_avg(abs(m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)),14) * 100, 6) + m_lag(m_avg(abs(m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) - m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)) / (m_sum(IF(LD > 0 AND LD > HD, LD, 0),14) * 100 / m_sum(TR, 14) + m_sum(IF(HD > 0 AND HD > LD, HD, 0),14) * 100 / m_sum(TR, 14)),14) * 100, 6)) / 2 
            AS alpha_a191_f0186
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 90)

    def alpha_a191_f0187(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_sum(IF(open <= m_lag(open, 1), 0, greatest((high - open), (open - m_lag(open, 1)))), 20) 
            AS alpha_a191_f0187
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0188(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((high-low-m_ta_ewm(high-low,11,2))/m_ta_ewm(high-low,11,2))*100 
            AS alpha_a191_f0188
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0189(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            m_avg(abs(close-m_avg(close,6)),6) 
            AS alpha_a191_f0189
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 30)

    def alpha_a191_f0190(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            (((m_sum(IF(close / m_lag(close,1) - 1 > ((close / m_lag(close, 19))^(1/20) - 1), 1, 0), 20) - 1) * (m_sum(IF(close / m_lag(close,1) - 1 < (close / m_lag(close, 19))^(1/20) - 1, (close / m_lag(close,1) - 1 - (close / m_lag(close, 19))^(1/20) - 1)^2, 0), 20))) / ((m_sum(IF(close / m_lag(close,1) - 1 < (close / m_lag(close, 19))^(1/20) - 1, 1, 0), 20)) * (m_sum(IF(close / m_lag(close,1) - 1 > (close / m_lag(close, 19))^(1/20) - 1, (close / m_lag(close,1) - 1 - (close / m_lag(close, 19))^(1/20) - 1)^2, 0), 20)))) 
            AS alpha_a191_f0190
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)

    def alpha_a191_f0191(self):
        sql_alpha = f"""
        SELECT
            date,
            instrument,
            ((m_corr(m_avg(volume,20), low, 5) + ((high + low) / 2)) -close) 
            AS alpha_a191_f0191
        FROM data_base
        """
        return self.generate_alpha_df(sql_alpha, 60)
