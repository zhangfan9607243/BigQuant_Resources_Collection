import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.DATA.ORGN.ORGN import *

class CNDT_DATA_ORGN_FTAD1(ORGN):

    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
