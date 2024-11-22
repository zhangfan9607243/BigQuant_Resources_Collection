import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.DATA.DATA import *

class ORGN(DATA):

    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
