import sys 
sys.path.append("BigQuant_Research_Frameworks")
from BQCN.DT.DT import *

class DATA(DT):

    def __init__(self, sd, ed, instruments = []):
        super().__init__(sd, ed, instruments)
