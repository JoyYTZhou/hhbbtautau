import mplhep as hep
import matplotlib.pyplot as plt
import json
from utils.filesysutil import checkpath, glob_files
from utils.rootutil import load_fields
from functools import wraps
from utils.datautil import arr_handler, iterwgt
from analysis.selutility import Object
import awkward as ak
import os
import numpy as np

pjoin = os.path.join

def iterdata(func):
    @wraps(func)
    def wrapper(instance, *args, **kwargs):
        results = []
        for process, dsitems in instance.data_dict.items():
                if instance.resolution:
                    for ds in dsitems.keys():
                        root_file = dsitems[ds]
                        results.append(func(instance, root_file, process, ds, *args, **kwargs))
                else:
                    result = []
                    for ds in dsitems.keys():
                        root_file = dsitems[ds]
                        result.append(func(instance, root_file, process, ds, *args, **kwargs))
                    results.append(ak.concatenate(result, axis=0))
        return results
    return wrapper
    
class DataPlotter():
    def __init__(self, cleancfg, plotsetting):
        self.plotcfg = plotsetting
        self.datadir = pjoin(cleancfg.LOCALOUTPUT, 'objlimited')
        with open(pjoin(cleancfg.DATAPATH, 'wgt_total.json'), 'r') as f:
            self.wgt_dict = json.load(f)
        self.data_dict = {}
        self.getdata()
        self.resolution = 1 if cleancfg.RESOLUTION == 'dataset' else 0
        self.labels = self.getlabels()
        self.wgt = self.getwgt()
        self.outdir = pjoin(cleancfg.LOCALOUTPUT, 'plots')
        checkpath(self.outdir)

    @iterwgt
    def getdata(self, process, ds):
        result = glob_files(self.datadir, startpattern=ds, endpattern='.root')
        if result:
            rootfile = result[0]
            if not process in self.data_dict: 
                self.data_dict[process] = {}
            if rootfile: self.data_dict[process][ds] = rootfile
    
    @iterdata
    def getobj(self, root_file, process, ds, obj_name):
        events = load_fields(root_file, tree_name=obj_name)
        return events
    
    def getlabels(self):
        if self.resolution:
            flattened_keys = [key for subdict in self.data_dict.values() for key in subdict.keys()]
            return flattened_keys
        else:
            return list(self.data_dict.keys())
    
    @iterdata
    def getwgt(self, root_file, process, ds, per_evt_wgt='Generator_weight', lumi=5000, *args, **kwargs):
        flat_wgt = self.wgt_dict[process][ds] * lumi
        wgt_arr = load_fields(root_file, tree_name='extra')[per_evt_wgt] * flat_wgt
        return wgt_arr
            
    def savewgt(self):
        """Save weighted selected events number to a csv."""
        pass
    
    def plotobj(self, objname, attridict):
        evts = self.getobj(objname)
        objplotter = ObjectPlotter(objname, self.plotcfg[objname], self.wgt, self.labels, evts)
            
    
        
class ObjectPlotter():
    def __init__(self, objname, plotcfg, wgt, labels, evts):
        self.objname = objname
        self.objcfg = plotcfg
        self.wgt = wgt
        self.labels = labels
        self.evts = evts
    
    def histobj(self, varname, objindx, bins_no, range, sort_by='pt'):
        evts = self.evts
        wgt_arrs = self.wgt
        list_of_hists = [None] * len(evts)
        bin_edges = None
        for i, obj_arr in enumerate(evts):
            var = ObjectPlotter.sortobj(obj_arr, sort_by=sort_by, sort_what=varname)[:,objindx]
            if i==0:
                list_of_hists[i], bin_edges = ObjectPlotter.deal_overflow(var, bins_no, range, weights=wgt_arrs[i])
            else:
                list_of_hists[i] = ObjectPlotter.deal_overflow(var, bins_no, range, weights=wgt_arrs[i])[0]
        return list_of_hists, bin_edges
    
    @staticmethod
    def plot_var(hist, bin_edges, legend, xlabel, range, save=True, title='', save_name='plot.png', **kwargs):
        fig, ax = plt.subplots(figsize=(18, 10))
        if title: ax.set_title(title)
        hep.style.use("CMS")
        hep.histplot(
            hist,
            bins=bin_edges,
            histtype=kwargs.pop("histtype", 'step'),
            ax=ax,
            label=legend,
            stack=kwargs.pop("stack", True),
            **kwargs
        )
        ax.set_xlabel(xlabel, fontsize=15)
        ax.set_ylabel("Events", fontsize=15)
        ax.set_xlim(*range)
        ax.legend(fontsize=15)
        if save:
            fig.savefig(save_name, dpi=300)
        fig.show() 
        
    @staticmethod
    def deal_overflow(arr, bins, range, weights=None):
        """Wrapper around numpy histogram function to deal with overflow.
        
        Parameters
        - `arr`: the array to be histogrammed
        - `bin_no`: number of bins
        - `range`: range of the histogram
        """
        if isinstance(bins, int):
            bins = np.linspace(*range, bins)
            min_edge = bins[0]
            max_edge = bins[-1]
        adjusted_data = np.clip(arr, min_edge, max_edge)
        hist, bin_edges = np.histogram(adjusted_data, bins=bins, weights=weights)
        return hist, bin_edges
            
    @staticmethod
    def sortobj(data, sort_by, sort_what, **kwargs):
        """Return an awkward array representation of the sorted attribute in data.
        
        Parameters
        - `sort_by`: the attribute to sort by
        - `sort_what`: the attribute to be sorted
        - `kwargs`: additional arguments for sorting
        """
        mask = Object.sortmask(data[sort_by], **kwargs)
        return arr_handler(data[sort_what])[mask]
        

# style a dataframe table
def makePretty(styler,color_code):
    styler.format(precision=3)
    css_indexes=f'background-color: {color_code}; color: white;'
    styler.applymap_index(lambda _: css_indexes, axis=1)
    return styler
