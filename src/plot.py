from analysis.plotutility import CFCombiner
from utils.rootutil import DataLoader
from config.selectionconfig import plotsetting as pltsetting

def hadd():
    comb = CFCombiner(pltsetting)
    comb(from_load=False, from_raw=True)

def getcf():
    comb = CFCombiner(pltsetting)
    comb(from_load=False)

if __name__ == '__main__':
    hadd()