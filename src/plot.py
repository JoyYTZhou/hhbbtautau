from analysis.plotutility import *
from config.selectionconfig import plotsetting as pltsetting

def get_newcf():
    vis = Visualizer(pltsetting)
    vis.getweights(save=True, from_raw=True)
    raw_df, wgt_df = vis.compute_allcf(lumi=5000, output=True)
    vis.efficiency(raw_df, overall=True, append=False, save=True, save_name='rawcf')
    return None
    
def plot():
    vis = Visualizer(pltsetting)
    # DataLoader.combine_roots(pltsetting, vis.wgt_dict)
    # vis.updatedir()
    # vis.combine_roots(save=False, save_separate=True)

if __name__ == '__main__':
    plot() 
