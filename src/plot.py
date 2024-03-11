from analysis.plotutility import Visualizer
from config.selectionconfig import plotsetting as pltsetting

def plot():
    vis = Visualizer(pltsetting)
    vis.grepweights(output=True, from_raw=True)
    vis.updatedir()
    raw_df, wgt_df = vis.compute_allcf(lumi=5000, output=True)
    vis.efficiency(raw_df, overall=True, append=False, save=True, save_name='rawcf')
    vis.efficiency(wgt_df, overall=True, append=False, save=True, save_name='wgtcf')
    vis.combine_roots(save=True, save_separate=True)

if __name__ == '__main__':
    plot() 
