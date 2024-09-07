import mplhep as hep
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import json

from src.utils.datautil import arr_handler, iterwgt
from src.analysis.objutil import Object
from src.utils.filesysutil import checkpath, glob_files, pjoin
from src.utils.cutflowutil import load_csvs

from config.selectionconfig import cleansetting as cleancfg

resolve = cleancfg.get("RESOLVE", False)
lumi = cleancfg.LUMI
processes = cleancfg.DATASETS

colors = list(mpl.colormaps['Set2'].colors)

class CSVPlotter():
    def __init__(self, outdir):
        with open(pjoin(cleancfg.DATAPATH, 'availableQuery.json'), 'r') as f:
            self.meta_dict = json.load(f)
        self.data_dict = {}
        self.labels = list(self.meta_dict.keys())
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
        def add_wgt(dfs, rwfac, ds, group):
            df = dfs[0]
            if df.empty: return None
            df['weight'] = df[per_evt_wgt] * rwfac
            df['dataset'] = ds
            df['group'] = group 
            if extraprocess: return extraprocess(df)
            else: return df
        for process in processes:
            load_dir = pjoin(datasource, process) 
            cf_dict = {}
            cf_df = load_csvs(load_dir, f'{process}_cf')[0]
            for ds in self.meta_dict[process].keys():
                rwfac = self.rwgt_fac(process, ds) 
                dsname = self.meta_dict[process][ds]['shortname']
                df = load_csvs(load_dir, f'{dsname}_out', func=add_wgt, rwfac=rwfac, ds=dsname, group=process)
                checkpath(f'{new_outdir}/{process}')
                if df is not None: 
                    list_of_df.append(df)
                    self.addextcf(cf_dict, df, dsname, per_evt_wgt)
                else:
                    cf_dict[f'{dsname}_raw'] = 0
                    cf_dict[f'{dsname}_wgt'] = 0
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
        flat_wgt = self.meta_dict[process][ds]['per_evt_wgt'] * factor 
        return flat_wgt
   
    def get_hist(self, evts: 'pd.DataFrame', att, options, group: 'dict'=None, **kwargs) -> tuple[list, list, list[int, int], list, list]:
        """Histogram an attribute of the object for the given dataframe for groups of datasets. 
        Return sorted histograms based on the total counts.
        
        Parameters
        - `group`: the group of datasets to be plotted. {groupname: [list of datasets]}
        - `kwargs`: additional arguments for the `ObjectPlotter.hist_arr` function
        
        Returns
        - `hist_list`: a sorted list of histograms
        - `bins`: the bin edges
        - `bin_range`: the range of the histogram
        - `pltlabel`: the sorted labels of the datasets
        - `b_colors`: the colors of the datasets"""
        histopts = options['hist']
        bins = histopts.get('bins', 40)
        bin_range = histopts.get('range', (0,200))
        pltlabel = list(group.keys()) if group is not None else self.labels
        hist_list = []
        b_colors = colors[0:len(pltlabel)]
        for label in pltlabel:
            proc_list = group[label] if group is not None else [label]
            thisdf = evts[evts['process'].isin(proc_list)]
            thishist, bins = ObjectPlotter.hist_arr(thisdf[att], bins, bin_range, thisdf['weight'], **kwargs)
            hist_list.append(thishist)
        return hist_list, bins, bin_range, pltlabel, b_colors
    
    @staticmethod
    def get_order(hist_list) -> list:
        """Order the histograms based on the total counts."""
        total_counts = [np.sum(hist) for hist in hist_list]
        sorted_indx = np.argsort(total_counts)[::-1]
        return sorted_indx
    
    @staticmethod
    def order_list(list_of_obj, order) -> list:
        """Order the list of lists based on the order."""
        return [list_of_obj[i] for i in order]
    
    def plot_hist(self, evts: 'pd.DataFrame', attridict: 'dict', title='', group: 'dict'=None, save_name='', **kwargs):
        """Plot the object attribute based on the attribute dictionary.
        
        Parameters
        - `objname`: the object to be loaded and plotted
        - `attridict`: a dictionary of attributes to be plotted
        """
        for att, options in attridict.items():
            hist_list, bins, bin_range, pltlabel, _= self.get_hist(evts, att, options, group, **kwargs)
            pltopts = options['plot']
            ObjectPlotter.plot_var(hist_list, bins, label=pltlabel, xrange=bin_range, title=title, 
                                   save_name=pjoin(self.outdir, f'{att}{save_name}.png'), **pltopts)

    def plot_shape(self, list_of_evts: list[pd.DataFrame], labels, attridict: dict, title='', save_name=''):
        """Compare the shape of the object attribute for different dataframes."""
        for att, options in attridict.items():
            hist_list = []
            pltopts = options['plot']
            bins = options['hist']['bins']
            bin_range = options['hist']['range']
            for evts in list_of_evts:
                thishist, bins = ObjectPlotter.hist_arr(evts[att], bins, bin_range, evts['weight'], density=True, keep_overflow=False)
                hist_list.append(thishist)
            ObjectPlotter.plot_var(hist_list, bins, labels, bin_range, title=title, 
                                   save_name=pjoin(self.outdir, f'{att}{save_name}.png'), stack=False, histtype='step', alpha=1.0,  
                                   **pltopts)
            
    def plot_SVB(self, evts, attridict, sgroup, bgroup, title='', save_name='', **kwargs):
        """Plot the signal and background histograms.
        
        Parameters
        - `evts`: the dataframe to be plotted
        - `attridict`: the dictionary of attributes to be plotted
        - `kwargs`: additional arguments to be passed into the `ObjectPlotter.hist_arr` function
        """
        for att, options in attridict.items():
            pltopts = options['plot']
            b_hists, bins, range, blabels, b_color = self.get_hist(evts, att, options, bgroup, **kwargs)
            if order is not None:
                b_hists = ObjectPlotter.order_list(b_hists, order)
                blabels = ObjectPlotter.order_list(blabels, order)
            else:
                order = ObjectPlotter.get_order(b_hists)
                b_hists = ObjectPlotter.order_list(b_hists, order)
            s_hists, bins, range, slabels, _ = self.get_hist(evts, att, options, sgroup, **kwargs)
            ObjectPlotter.plotSigVBkg(s_hists, b_hists, bins, slabels, blabels, range, title=title, save_name=pjoin(self.outdir, f'{att}{save_name}.png'), b_color=b_color, **pltopts) 
    
    def plot_SvBHist(self, evts, att, attoptions, bgroup, sgroup, **kwargs) -> list:
        """Plot the signal and background histograms.
        
        Parameters
        - `evts`: the dataframe to be plotted
        - `att`: the attribute to be plotted
        - `attoptions`: the options for the attribute to be plotted
        - `bgroup`: the background group
        - `sgroup`: the signal group
        
        Return
        - `order`: the order of the histograms"""
        b_hists, bins, range, blabels, _ = self.get_hist(evts, att, attoptions, bgroup)
        order = kwargs.pop('order', ObjectPlotter.get_order(b_hists))
        b_hists, blabels = ObjectPlotter.order_list(b_hists, order), ObjectPlotter.order_list(blabels, order)

        s_hists, bins, range, slabels, _ = self.get_hist(evts, att, attoptions, sgroup, **kwargs)
        ObjectPlotter.plotSigVBkg(s_hists, b_hists, bins, slabels, blabels, range, **kwargs)  

        return order

    def plot_fourRegions(self, regionA, regionB, regionC, regionD, attridict, title='', save_name='', **kwargs):
        """Plot the signal and background histograms for the four regions."""
        for att, options in attridict.items():
            pltopts = options['plot']
            fig, axes = ObjectPlotter.set_style(title, pltopts.pop('xlabel',''), n_row=2, n_col=2)
            order = self.plot_SvBHist(regionA, att, options, regionB, regionC, ax=axes[0,0], **kwargs)
            self.plot_SvBHist(regionB, att, options, regionA, regionC, ax=axes[0,1], order=order, **kwargs)
            self.plot_SvBHist(regionC, att, options, regionA, regionB, ax=axes[1,0], order=order, **kwargs)
            self.plot_SvBHist(regionD, att, options, regionA, regionB, ax=axes[1,1], order=order, **kwargs)
            fig.savefig(pjoin(self.outdir, f'{att}{save_name}.png'), dpi=300)
        
class ObjectPlotter():
    def __init__(self):
        pass
    
    @staticmethod
    def set_style(title, xlabel, n_row=1, n_col=1):
        """Set the style of the plot to CMS HEP."""
        fig, axes = plt.subplots(n_row, n_col, figsize=(8, 5))
        fig.suptitle(title, fontsize=14)
        hep.style.use("CMS")
        hep.cms.label(label='Work in Progress', fontsize=11)

        for ax in axes.flat:
            ax.set_xlabel(xlabel, fontsize=12)
            ax.set_ylabel("Events", fontsize=12)
            ax.set_prop_cycle('color', colors)
            ax.tick_params(axis='both', which='major', labelsize=10, length=0)
            for spine in ax.spines.values():
                spine.set_linewidth(0.5)
        return fig, axes

    @staticmethod
    def plot_var(ax, hist, bin_edges: np.ndarray, label, xrange, **kwargs):
        """A wrapper around the histplot function in mplhep to plot the histograms.
        
        Parameters
        - `hist`: np.ndarray object as histogram, or a list of histograms
        - `bin_edges`: the bin edges
        """
        hep.histplot(
            hist,
            bins=bin_edges,
            label=label,
            ax=ax,
            linewidth=1.5,
            **kwargs)
        ax.legend(fontsize=12, loc='upper right')
        ax.set_xlim(*xrange)
    
    @staticmethod
    def plotSigVBkg(ax, sig_hists, bkg_hists, bin_edges, sig_label, bkg_label, xrange, **kwargs):
        """Plot the signal and background histograms.
        
        Parameters
        - `sig_hists`: the signal histograms
        - `bkg_hists`: the background histograms
        - `bin_edges`: the bin edges
        """
        s_colors = ['red', 'blue', 'forestgreen']
        hep.histplot(
            bkg_hists,
            bins=bin_edges,
            label=bkg_label,
            ax=ax,
            color=kwargs.pop('b_color', colors),
            histtype='fill',
            alpha=0.6,
            stack=True,
            linewidth=1)
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
        ax.set_xlim(*xrange)
        ax.legend(fontsize=12, loc='upper right')
        
    @staticmethod
    def hist_arr(arr, bins: int, range: list[int, int], weights=None, density=False, keep_overflow=True) -> tuple[np.ndarray, np.ndarray]:
        """Wrapper around numpy histogram function to deal with overflow.
        
        Parameters
        - `arr`: the array to be histogrammed
        - `bin_no`: number of bins
        - `range`: range of the histogram
        - `weights`: the weights of the array
        """
        if isinstance(bins, int):
            bins = np.linspace(*range, bins+1)
            min_edge = bins[0]
            max_edge = bins[-1]
            if keep_overflow: adjusted_data = np.clip(arr, min_edge, max_edge)
            else: adjusted_data = arr
        else:
            adjusted_data = arr
        hist, bin_edges = np.histogram(adjusted_data, bins=bins, weights=weights, density=density)
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