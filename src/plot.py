from analysis.plotutility import CFCombiner
from utils.rootutil import DataLoader
from config.selectionconfig import plotsetting as pltsetting

def hadd():
    dl = DataLoader(pltsetting)
    dl()

if __name__ == '__main__':
    hadd()