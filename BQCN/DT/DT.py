import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.BQCN import *

class DT(BQCN):

    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
