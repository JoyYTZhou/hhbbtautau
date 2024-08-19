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

resolve = cleancfg.get("RESOLVE", False)
lumi = cleancfg.LUMI
processes = cleancfg.DATASETS

colors = list(mpl.colormaps['Set2'].colors)

class CSVPlotter():
    def __init__(self, outdir):
        self.wgt_dict = haddWeights(cleancfg.DATAPATH)
        self.data_dict = {}
        self.labels = list(self.wgt_dict.keys())
        self.outdir = outdir
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
            cf_df.to_csv(pjoin(new_outdir, process, f'{process}_{selname.replace(" ", "")}_cf.csv'))
        processed = pd.concat(list_of_df, axis=0).reset_index().drop('index', axis=1)
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
            factor = cleancfg.get('factor', 100)
        else:
            factor = 1 
        flat_wgt = self.wgt_dict[process][ds] * lumi * 1000 * factor 
        return flat_wgt
   
    def get_hist(self, evts: 'pd.DataFrame', att, options, group: 'dict'=None) -> tuple[dict, dict, list[int, int]]:
        """Histogram an attribute of the object"""
        histopts = options['hist']
        bin_no = histopts.get('bins', 10)
        bin_range = histopts.get('range', (0,200))
        pltlabel = list(group.keys()) if group is not None else self.labels
        hist_list = []
        for label in pltlabel:
            proc_list = group[label] if group is not None else [label]
            thisdf = evts[evts['process'].isin(proc_list)]
            thishist, bins = ObjectPlotter.hist_overflow(thisdf[att], bin_no, bin_range, thisdf['weight'])
            hist_list.append(thishist)
        return hist_list, bins, bin_range
                
    def plot_hist(self, evts: 'pd.DataFrame', attridict: 'dict', title='', group: 'dict'=None, save_name=''):
        """Plot the object attribute based on the attribute dictionary.
        
        Parameters
        - `objname`: the object to be loaded and plotted
        - `attridict`: a dictionary of attributes to be plotted
        """
        for att, options in dict(attridict).items():
            hist_list, bins, bin_range = self.get_hist(evts, att, options, group)
            pltopts = options['plot']
            pltlabel = list(group.keys()) if group is not None else self.labels
            ObjectPlotter.plot_var(hist_list, bins, label=pltlabel, xrange=bin_range, title=title, 
                                   save_name=pjoin(self.outdir, f'{att}{save_name}.png'), **pltopts)
    
    def plot_SVB(self, evts, attridict, sgroup, bgroup, title='', save_name=''):
        """Plot the signal and background histograms.
        
        Parameters
        - `evts`: the dataframe to be plotted
        - `attridict`: the dictionary of attributes to be plotted
        """
        for att, options in dict(attridict).items():
            pltopts = options['plot']
            b_hists, bins, range = self.get_hist(evts, att, options, bgroup)
            blabels = list(bgroup.keys())
            slabels = list(sgroup.keys())
            s_hists, bins, range = self.get_hist(evts, att, options, sgroup)
            ObjectPlotter.plotSigVBkg(s_hists, b_hists, bins, slabels, blabels, range, title=title, save_name=pjoin(self.outdir, f'{att}{save_name}.png'), **pltopts) 
        
class ObjectPlotter():
    def __init__(self):
        pass
    
    @staticmethod
    def set_style(title, xlabel):
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.set_title(title, fontsize=14)
        hep.style.use("CMS")
        hep.cms.label(label='Work in Progress', fontsize=11)
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel("Events", fontsize=12)
        ax.set_prop_cycle('color', colors)
        ax.tick_params(axis='both', which='major', labelsize=10, length=0)
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
        return fig, ax

    @staticmethod
    def plot_var(hist, bin_edges: np.ndarray, label, xrange, title='plot', save_name='plot.png', **kwargs):
        """Plot the object attribute.
        
        Parameters
        - `hist`: np.ndarray object as histogram, or a list of histograms
        - `bin_edges`: the bin edges
        """
        xlabel = kwargs.pop('xlabel', 'GeV')
        fig, ax = ObjectPlotter.set_style(title, xlabel)
        hep.histplot(
            hist,
            bins=bin_edges,
            label=label,
            ax=ax,
            linewidth=1,
            **kwargs)
        ax.legend(fontsize=12, loc='upper right')
        fig.savefig(save_name, dpi=300)
    
    @staticmethod
    def plotSigVBkg(sig_hists, bkg_hists, bin_edges, sig_label, bkg_label, xrange, title='plot', save_name='plot.png', **kwargs):
        """Plot the signal and background histograms.
        
        Parameters
        - `sig_hists`: the signal histograms
        - `bkg_hists`: the background histograms
        - `bin_edges`: the bin edges
        """
        xlabel = kwargs.pop('xlabel', 'GeV')
        fig, ax = ObjectPlotter.set_style(title, xlabel)
        s_colors = ['red', 'blue', 'forestgreen']
        hep.histplot(
            sig_hists,
            bins=bin_edges,
            ax=ax,
            color=s_colors,
            label=sig_label,
            stack=False,
            histtype='step',
            alpha=1.0,
            linewidth=2)
        hep.histplot(
            bkg_hists,
            bins=bin_edges,
            label=bkg_label,
            ax=ax,
            histtype='fill',
            alpha=0.4,
            stack=True,
            linewidth=1)
        ax.set_xlim(*xrange)
        ax.legend(fontsize=12, loc='upper right')
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