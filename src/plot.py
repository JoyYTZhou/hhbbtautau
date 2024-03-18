from analysis.plotutility import Combiner
from config.selectionconfig import plotsetting as pltsetting

def hadd():
    comb = Combiner(pltsetting)
    comb.getweights(save=True, from_raw=True, from_load=False)
    comb.hadd_to_pkl()

if __name__ == '__main__':
    hadd()