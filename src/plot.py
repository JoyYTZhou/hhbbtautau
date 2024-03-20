from analysis.plotutility import CFCombiner
from config.selectionconfig import plotsetting as pltsetting

def hadd():
    comb = CFCombiner(pltsetting)
    comb.getweights(save=True, from_raw=True, from_load=False)
    comb.hadd_to_pkl()

def getcf():
    comb = CFCombiner(pltsetting)
    comb(from_load=False)

if __name__ == '__main__':
    getcf()