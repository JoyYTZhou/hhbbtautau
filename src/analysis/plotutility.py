import mplhep as hep
import matplotlib.pyplot as plt
import json
from utils.filesysutil import checkpath, glob_files
from utils.rootutil import load_fields, DataLoader
from functools import wraps
from utils.datautil import arr_handler, iterwgt
from analysis.selutility import Object
import awkward as ak
import os
import numpy as np

pjoin = os.path.join

def iterdata(func):
    """Wrapper that Returns a list of results from the function for each dataset."""
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
        self._datadir = pjoin(cleancfg.LOCALOUTPUT, 'objlimited')
        self.wgt_dict = DataLoader.haddWeights(cleancfg.DATAPATH, from_raw=False)
        self.data_dict = {}
        self.getdata()
        self.resolution = 1 if cleancfg.RESOLUTION == 'dataset' else 0
        self.labels = self.getlabels()
        self.wgt = self.getwgt()
        self.outdir = pjoin(cleancfg.LOCALOUTPUT, 'plots')
        checkpath(self.outdir)

    @iterwgt
    def getdata(self, process, ds):
        """Returns the root files for the datasets."""
        result = glob_files(self._datadir, startpattern=ds, endpattern='.root')
        if result:
            rootfile = result[0]
            if not process in self.data_dict: 
                self.data_dict[process] = {}
            if rootfile: self.data_dict[process][ds] = rootfile
    
    @iterdata
    def getobj(self, root_file, process, ds, obj_name):
        """Returns the object from the root file."""
        events = load_fields(root_file, tree_name=obj_name)
        return events
    
    def getlabels(self):
        """Returns the labels for the datasets."""
        if self.resolution:
            flattened_keys = [key for subdict in self.data_dict.values() for key in subdict.keys()]
            return flattened_keys
        else:
            return list(self.data_dict.keys())
    
    @iterdata
    def getwgt(self, root_file, process, ds, per_evt_wgt='Generator_weight', lumi=5000, **kwargs):
        """Returns the weights for the datasets."""
        signalname = kwargs.get("signal", 'ggF')
        if process == 'ggF': 
            factor = kwargs.get('factor', 100)
        else:
            factor = 1
        flat_wgt = self.wgt_dict[process][ds] * lumi * factor
        wgt_arr = load_fields(root_file, tree_name='extra')[per_evt_wgt] * flat_wgt
        return wgt_arr
            
    def savewgt(self):
        """Save weighted selected events number to a csv."""
        pass
    
    def plotobj(self, objname, attridict):
        """Plot the object attribute based on the attribute dictionary.
        
        Parameters
        - `objname`: the object to be loaded and plotted
        - `attridict`: a dictionary of attributes to be plotted
        """
        evts = self.getobj(objname)
        objplotter = ObjectPlotter(objname, self.wgt, self.labels, evts)
        for att, options in attridict.items():
            histlist, bins = objplotter.histobj(options.pop('varname', 'pt'), 
                                                options.pop('objindx', 0), 
                                                options.pop('bins', 10), 
                                                options.get('range', (0,200)), 
                                                sort_by=options.pop('sort_by', 'pt'))
            objplotter.plot_var(histlist, bins,
                                legend=self.labels, 
                                xlabel=options.pop('xlabel', 'GeV'),
                                range=options.pop('range', (0,200)),
                                save=options.pop('save', True), 
                                title=options.pop('title', ''),
                                save_name=pjoin(self.outdir, options.pop('save_name', 'plot.png')),
                                **options)
        return None
        
class ObjectPlotter():
    def __init__(self, objname, wgt, labels, evts):
        self.objname = objname
        self.wgt = wgt
        self.evts = evts
    
    def histobj(self, varname, objindx, bins_no, range, sort_by):
        """Returns a list of histograms for the object attribute.
        
        Parameters
        - `varname`: the attribute to be histogrammed
        - `objindx`: the index of the object in the object array
        - `bins_no`: number of bins
        - `range`: range of the histogram
        - `sort_by`: the attribute to sort by
        """
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
    def plot_var(hist, bin_edges, legend, xlabel, range, save, **kwargs):
        """Plot the object attribute."""
        fig, ax = plt.subplots(figsize=(18, 10))
        ax.set_title(kwargs.pop('title', 'plot'))
        save_name = kwargs.pop('save_name', 'plot.png')
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
