import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.DATA.ORGN.ORGN import *

class CNDT_DATA_ORGN_STAD1(ORGN):

    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
    
    def cn_data_stock_bar1d(self):
        sql = """
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
            deal_number,
        FROM cn_stock_bar1d
        """
    
    def cn_data_stock_bar1w(self):
        sql = """
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
            deal_number,
        FROM cn_stock_bar1d
        """
