import mplhep as hep
import matplotlib.pyplot as plt
import os
import json
import shutil
from utils.filesysutil import *
from utils.cutflowutil import *
from utils.rootutil import load_fields
from functools import wraps
from utils.datautil import arr_handler, iterwgt
from analysis.selutility import Object
import awkward as ak

class CFCombiner():
    """
    This class provides methods for getting cutflows of datasets
    It includes functions for combining root files, computing cutflow tables, calculating efficiencies,
    and loading/saving data.
    
    Attributes:
        cleancfg (object): An object containing the plot configuration.
        indir (str): The input directory path.
        outdir (str): The output directory path.
        wgt_dict (dict): A dictionary containing the weights for each dataset.
    """

    def __init__(self, cleancfg):
        self._cleancfg = cleancfg
        self.indir = cleancfg.INPUTDIR
        self.outdir = cleancfg.OUTPUTDIR
        checkpath(self.outdir)
        self.wgt_dict = None
    
    def __call__(self, from_load=False, from_raw=True, name='cutflow_tot', **kwargs):
        """Get total cutflow and efficiency for all datasets.
        Parameters:
        - `from_load`: whether to load from output directory
        - `from_raw`: whether to compute weights based on number of raw events instead of weighted
        - `name`: name of the cutflow output file
        - `kwargs`: additional arguments for the function
        Returns:
        - Raw cutflow dataframe, weighted cutflow dataframe
        """
        self.getweights(from_load=from_load, from_raw=from_raw)
        if self.cleancfg.REFRESH: 
            for ds in self.cleancfg.DATASETS:
                if os.path.isdir(pjoin(self.outdir, ds)): shutil.rmtree(pjoin(self.outdir, ds))
        raw_df, wgt_df = self.get_totcf(from_load=from_load, name=name)
        efficiency(self.outdir, wgt_df, append=False, save=True, save_name='tot')
        if self.cleancfg.CONDOR_TRANSFER: 
            condorpath = self.cleancfg.CONDORPATH
            checkpath(condorpath)
            transferfiles(self.outdir, condorpath)
        return raw_df, wgt_df
    
    def checkupdates(self):
        for ds in self.cleancfg.DATASETS:
            sync_files(pjoin(self.indir, ds), f"{self.cleancfg.CONDORPATH}/{ds}")
    
    def getweights(self, save=False, from_raw=False, from_load=False):
        """Compute/Load weights needed for these datasets. Save if needed."""
        if from_load:
            with open(pjoin(self.cleancfg.DATAPATH, 'wgt_total.json'), 'r') as f:
                self.wgt_dict = json.load(f)        
    
    def sort_cf(self, srcdir, save=True):
        """Create a multi index table that contains all channel cutflows for all datasets.
        :param ds_list: list of strings of dataset
        :param srcdir: output cutflow source directory
        """
        multi_indx = []
        ds_list = self.cleancfg.DATASETS
        df_list = [None]*len(ds_list)
        for i, ds in enumerate(ds_list):
            ds_dir = os.path.join(srcdir, ds)
            ds_cf = self.combine_cf(ds_dir)
            efficiency(self.outdir, ds_cf)
            df_list[i] = ds_cf
            multi_indx += [(ds, indx) for indx in ds_cf.index]
        
        allds_cf = pd.concat(df_list)
        allds_cf.index = pd.MultiIndex.from_tuples(multi_indx, names=['Process', 'Selection'])

        if save: 
            finame = os.path.join(self.cleancfg['LOCAL_OUTPUT'], 'cutflow_table.csv')
            allds_cf.to_csv(finame)

        return allds_cf
        
def iterdata(func):
    @wraps(func)
    def wrapper(instance, *args, **kwargs):
        results = []
        for process, dsitems in instance.data_dict.items():
            if dsitems:
                for ds in dsitems.keys():
                    root_file = dsitems[ds]
                    results.append(func(instance, root_file, process, ds, *args, **kwargs))
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
        self.resolution = cleancfg.RESOLUTION
        self.labels = self.getlabels()
        self.wgt = self.getwgt()

    @iterwgt
    def getdata(self, process, ds):
        result = glob_files(self.datadir, startpattern=ds, endpattern='.root')
        if result:
            rootfile = result[0]
            if not process in self.data_dict.keys(): 
                self.data_dict[process] = {}
            if rootfile: self.data_dict[process][ds] = rootfile
    
    @iterdata
    def getobj(self, root_file, process, ds, obj_name):
        events = load_fields(root_file, tree_name=obj_name)
        return events
    
    @iterdata
    def getlabels(self, root_file, process, ds):
        label = process if self.resolution == 'process' else ds
        return label
    
    @iterdata
    def getwgt(self, root_file, process, ds, per_evt_wgt='Generator_weight', lumi=5000, *args, **kwargs):
        flat_wgt = self.wgt_dict[process][ds] * lumi
        wgt_arr = load_fields(root_file, tree_name='extra')[per_evt_wgt] * flat_wgt
        return wgt_arr
    
    @staticmethod
    def concatevts(labels, objs, wgts):
        concatedobjs = {}
        concatedwgts = {}
        for label, wgt, obj in zip(labels, wgts, objs):
            if label in concatedobjs: 
                concatedobjs[label].append(obj)
                concatedwgts[label].append(wgt)
            else:
                concatedwgts[label] = [wgt]
                concatedobjs[label] = [obj]
        
        objlist = []
        wgtlist = []
        lablist = []
        for label in concatedobjs.keys():
            objlist.append(ak.concatenate(concatedobjs[label], axis=0))
            wgtlist.append(ak.concatenate(concatedwgts[label], axis=0))
            lablist.append(label)

        return objlist, wgtlist, lablist
            
    def savewgt(self):
        """Save weighted selected events number to a csv."""
        pass
    
    def plotobj(self, objname):
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
