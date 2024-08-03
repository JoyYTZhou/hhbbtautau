import mplhep as hep
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd

from utils.datautil import arr_handler, iterwgt
from analysis.objutil import Object
from utils.filesysutil import checkpath, glob_files, pjoin
from utils.cutflowutil import load_csvs
from utils.datautil import haddWeights

from config.selectionconfig import cleansetting as cleancfg

indir = cleancfg.INPUTDIR
resolve = cleancfg.get("RESOLVE", False)
lumi = cleancfg.LUMI
processes = cleancfg.DATASETS
localout = cleancfg.LOCALOUTPUT

colors = list(mpl.colormaps['Set2'].colors)

class CSVPlotter():
    def __init__(self):
        self.wgt_dict = haddWeights(cleancfg.DATAPATH)
        self.data_dict = {}
        self.labels = list(self.wgt_dict.keys())
        self.outdir = pjoin(localout, 'plots')
        checkpath(self.outdir)
    
    def addextcf(self, cutflow: 'dict', df, ds, wgtname) -> None:
        """Add the cutflow to the dictionary to be udpated to the cutflow table.
        
        Parameters
        - `cutflow`: the cutflow dictionary to be updated"""
        cutflow[f'{ds}_raw'] = len(df)
        cutflow[f'{ds}_wgt'] = df[wgtname].sum()
    
    def postprocess_csv(self, datasource=pjoin(cleancfg.LOCALOUTPUT, 'datasource'), per_evt_wgt='Generator_weight', extraprocess=False, selname='Pass') -> pd.DataFrame:
        """Post-process the datasets and save the processed dataframes to csv files.
        
        Parameters
        - `fitype`: the file type to be saved
        - `per_evt_wgt`: the weight to be multiplied to the flat weights
        - `extraprocess`: additional processing to be done on the dataframe"""
        list_of_df = []
        new_outdir = f'{datasource}_extrasel'
        checkpath(new_outdir)
        def add_wgt(dfs, rwfac):
            df = dfs[0]
            if df.empty: return None
            df['weight'] = df[per_evt_wgt] * rwfac
            df['process'] = process if not resolve else ds
            if extraprocess: return extraprocess(df)
            else: return df
        for process in processes:
            load_dir = pjoin(datasource, process) 
            cf_dict = {}
            cf_df = load_csvs(load_dir, f'{process}_cf')[0]
            for ds in self.wgt_dict[process].keys():
                rwfac = self.rwgt_fac(process, ds) 
                df = load_csvs(load_dir, f'{ds}_out', func=add_wgt, rwfac=rwfac)
                checkpath(f'{new_outdir}/{process}')
                if df is not None: 
                    list_of_df.append(df)
                    self.addextcf(cf_dict, df, ds, per_evt_wgt)
                else:
                    cf_dict[f'{ds}_raw'] = 0
                    cf_dict[f'{ds}_wgt'] = 0
            cf_df = pd.concat([cf_df, pd.DataFrame(cf_dict, index=[selname])])
            cf_df.to_csv(pjoin(new_outdir, process, f'{process}_cf.csv'))
        processed = pd.concat(list_of_df, axis=0).reset_index().drop('index', axis=1)
        processed.to_csv(pjoin(datasource, 'processed.csv'))
        return processed

    @iterwgt
    def getdata(self, process, ds, file_type='.root'):
        """Returns the root files for the datasets."""
        result = glob_files(self._datadir, filepattern=f'{ds}*{file_type}')
        if result: 
            rootfile = result[0]
            if not process in self.data_dict: 
                self.data_dict[process] = {}
            if rootfile: self.data_dict[process][ds] = rootfile
        else:
            raise FileNotFoundError(f"Check if there are any files of specified pattern in {self._datadir}.")
    
    def rwgt_fac(self, process, ds):
        signalname = cleancfg.get("signal", ['ggF'])
        if process in signalname: 
            factor = cleancfg.get('factor', 10)
        else:
            factor = 1 
        flat_wgt = self.wgt_dict[process][ds] * lumi * 1000 * factor 
        return flat_wgt
            
    def plot_hist(self, evts: 'pd.DataFrame', attridict: 'dict', group: 'dict'=None):
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
            pltlabel = list(group.keys()) if group is not None else self.labels
            for label in pltlabel:
                proc_list = group[label] if group is not None else [label]
                thisdf = evts[evts['process'].isin(proc_list)]
                thishist, bins = ObjectPlotter.hist_overflow(thisdf[att], bin_no, bin_range, thisdf['weight'])
                hist_list.append(thishist)
            ObjectPlotter.plot_var(hist_list, bins, label=pltlabel, xrange=bin_range, title='', 
                                   save_name=pjoin(self.outdir, f'{att}.png'), **pltopts)
        return None
        
class ObjectPlotter():
    def __init__(self, objname, wgt, evts):
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
        hep.cms.label(label='Work in Progress')
        xlabel = kwargs.pop('xlabel', 'GeV')
        ax.set_prop_cycle('color', colors)
        hep.histplot(
            hist,
            bins=bin_edges,
            label=label,
            ax=ax,
            linewidth=2,
            **kwargs)
        ax.set_xlabel(xlabel, fontsize=20)
        ax.set_ylabel("Events", fontsize=20)
        ax.set_xlim(*xrange)
        ax.legend(fontsize=18)
        fig.savefig(save_name, dpi=300)
        
    @staticmethod
    def hist_overflow(arr, bins: int, range: list[int, int], weights=None) -> tuple[np.ndarray, np.ndarray]:
        """Wrapper around numpy histogram function to deal with overflow.
        
        Parameters
        - `arr`: the array to be histogrammed
        - `bin_no`: number of bins
        - `range`: range of the histogram
        """
        if isinstance(bins, int):
            bins = np.linspace(*range, bins+1)
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