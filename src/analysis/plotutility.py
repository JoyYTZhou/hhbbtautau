import mplhep as hep
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import os, json

from utils.datautil import arr_handler, iterwgt
from analysis.evtselutil import Object
from utils.filesysutil import checkpath, glob_files, pjoin
from utils.cutflowutil import load_csvs
from utils.rootutil import load_fields
from config.selectionconfig import cleansetting as cleancfg

indir = cleancfg.INPUTDIR
resolve = cleancfg.get("RESOLVE", False)
lumi = cleancfg.LUMI
processes = cleancfg.DATASETS
localout = cleancfg.LOCALOUTPUT

colors = list(mpl.colormaps['Dark2'].colors) + list(mpl.colormaps['tab10'].colors)
    
class CSVPlotter():
    def __init__(self, datasource=pjoin(cleancfg.LOCALOUTPUT, 'datasource')):
        self._datadir = datasource
        self.wgt_dict = haddWeights(cleancfg.DATAPATH)
        self.data_dict = {}
        # self.getdata()
        self.labels = list(self.wgt_dict.keys())
        self.outdir = pjoin(localout, 'plots')
        checkpath(self.outdir)
    
    def postprocess(self, fitype='csv', per_evt_wgt='Generator_weight'):
        list_of_df = []
        for process in processes:
            for ds in self.wgt_dict[process].keys():
                rwfac = self.rwgt_fac(process, ds) 
                def add_wgt(dfs):
                    df = dfs[0]
                    df['weight'] = df[per_evt_wgt] * rwfac
                    df['process'] = process if not resolve else ds
                    return df
                if fitype == 'csv': 
                    df = load_csvs(pjoin(self._datadir, process), f'{ds}_out', func=add_wgt)
                    list_of_df.append(df)
        processed = pd.concat(list_of_df, axis=0).reset_index().drop('index', axis=1)
        processed.to_csv(pjoin(self._datadir, 'processed.csv'))
        return processed

    @iterwgt
    def getdata(self, process, ds, file_type='.root'):
        """Returns the root files for the datasets."""
        result = glob_files(self._datadir, startpattern=ds, endpattern=file_type)
        if result: 
            rootfile = result[0]
            if not process in self.data_dict: 
                self.data_dict[process] = {}
            if rootfile: self.data_dict[process][ds] = rootfile
        else:
            raise FileNotFoundError(f"Check if there are any files of specified pattern in {self._datadir}.")
    
    def getobj(self, file: 'str', process, ds, obj_name):
        """Returns the object from the root file."""
        if file.endswith('.root'):
            events = load_fields(file, tree_name=obj_name)
        elif file.endswith('.csv'):
            pass
        return events
    
    def rwgt_fac(self, process, ds):
        signalname = cleancfg.get("signal", ['ggF'])
        if process in signalname: 
            factor = cleancfg.get('factor', 100)
        else:
            factor = 1 
        flat_wgt = self.wgt_dict[process][ds] * lumi * 1000 * factor 
        return flat_wgt
            
    def plot_hist(self, evts: 'pd.DataFrame', attridict: 'dict', group=['ZH', 'ggF', 'ZZ']):
        """Plot the object attribute based on the attribute dictionary.
        
        Parameters
        - `objname`: the object to be loaded and plotted
        - `attridict`: a dictionary of attributes to be plotted
        """
        for att, options in dict(attridict).items():
            pltopts = options['plot']
            histopts = options['hist']
            bin_no = histopts.get('bins', 10)
            bin_range = histopts.get('range', (0,200))
            hist_list = []
            if group:
                thisdf = evts[evts['process'].isin(group)]
                otherdf = evts[~evts['process'].isin(group)]
                thishist, bins = ObjectPlotter.hist_overflow(thisdf[att], bin_no, bin_range, thisdf['weight'])
                thathist, bins = ObjectPlotter.hist_overflow(otherdf[att], bin_no, bin_range, otherdf['weight'])
                hist_list = [thishist, thathist]
                pltlabel = ['Signal', 'Background']
            else:
                for label in self.labels:
                    thisdf = evts[evts['process']==label]
                    hist, bins = ObjectPlotter.hist_overflow(thisdf[att], bin_no, bin_range, thisdf['weight'])
                    hist_list.append(hist)
                pltlabel = self.labels
            ObjectPlotter.plot_var(hist_list, bins, label=pltlabel, xrange=bin_range, title='', 
                                   save_name=pjoin(self.outdir, f'{att}.png'), **pltopts)
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
                list_of_hists[i], bin_edges = ObjectPlotter.hist_overflow(var, bins_no, range, weights=wgt_arrs[i])
            else:
                list_of_hists[i] = ObjectPlotter.hist_overflow(var, bins_no, range, weights=wgt_arrs[i])[0]
        return list_of_hists, bin_edges
    
    @staticmethod
    def plot_var(hist, bin_edges, label, xrange, title='plot', save_name='plot.png', **kwargs):
        """Plot the object attribute."""
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.set_title(title)
        hep.style.use("CMS")
        xlabel = kwargs.pop('xlabel', 'GeV')
        ax.set_prop_cycle('color', colors)
        hep.histplot(
            hist,
            bins=bin_edges,
            label=label,
            ax=ax,
            linewidth=3,
            **kwargs)
        ax.set_xlabel(xlabel, fontsize=20)
        ax.set_ylabel("Events", fontsize=20)
        ax.set_xlim(*xrange)
        ax.legend(fontsize=18)
        fig.savefig(save_name, dpi=300)
        fig.show() 
        
    @staticmethod
    def hist_overflow(arr, bins: int, range: list[int, int], weights=None) -> tuple[np.ndarray, np.ndarray]:
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

def haddWeights(grepdir) -> dict:
    """Function for self use only, grep weights from a list of json files formatted in a specific way.
    
    Parameters
    - `grepdir`: directory where the json files are located
    """
    wgt_dict = {}
    jsonfiles = glob_files(grepdir)
    for filename in jsonfiles:
        ds = os.path.basename(filename).rsplit('.json', 1)[0]
        with open(filename, 'r') as f:
            meta = json.load(f)
            dsdict = {}
            for dskey, dsval in meta.items():
                weight = dsval['Per Event']
                dsdict[dskey] = weight
            wgt_dict[ds] = dsdict
    return wgt_dict