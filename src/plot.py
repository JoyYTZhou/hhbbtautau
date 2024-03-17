from analysis.plotutility import *
from config.selectionconfig import plotsetting as pltsetting

def plot():
    vis = Visualizer(pltsetting)
    vis.getweights(save=True, from_raw=True)
    # DataLoader.combine_roots(pltsetting, vis.wgt_dict)
    # vis.updatedir()
    raw_df, wgt_df = vis.compute_allcf(lumi=5000, output=True)
    vis.efficiency(raw_df, overall=True, append=False, save=True, save_name='rawcf')
    vis.efficiency(wgt_df, overall=True, append=False, save=True, save_name='wgtcf')
    # vis.combine_roots(save=False, save_separate=True)

if __name__ == '__main__':
    plot() 
