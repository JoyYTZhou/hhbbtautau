import mplhep as hep
import matplotlib.pyplot as plt
import glob
import os
import json
from analysis.helper import *
from analysis.selutility import Object
import pickle
import awkward as ak
from analysis.mathhelper import *

class Combiner():
    """
    This class provides methods for getting cutflows of datasets
    It includes functions for combining root files, computing cutflow tables, calculating efficiencies,
    and loading/saving data.
    
    Attributes:
        pltcfg (object): An object containing the plot configuration.
        indir (str): The input directory path.
        outdir (str): The output directory path.
        wgt_dict (dict): A dictionary containing the weights for each dataset.
    """

    def __init__(self, plt_cfg):
        self._pltcfg = plt_cfg
        self.indir = plt_cfg.INPUTDIR
        self.outdir = plt_cfg.OUTPUTDIR
        checkpath(self.outdir)
        self.wgt_dict = None
    
    def __call__(self, from_load=False):
        """Get total cutflow and efficiency for all datasets.
        Parameters:
        - `from_load`: whether to load from output directory"""
        self.getweights(from_load=from_load)
        raw_df, wgt_df = self.get_totcf(from_load=from_load)
        efficiency(self.outdir, wgt_df, append=False, save=True, save_name='total_cutflow_efficiency.csv')
        return raw_df, wgt_df

    @property
    def pltcfg(self):
        return self._pltcfg

    def hadd_to_pkl(self):
        """Combine all root files of datasets in plot setting into one dataframe and save as pickle."""
        checkcondorpath(self.pltcfg.CONDORPATH)
        DataLoader.combine_roots(self.pltcfg, self.wgt_dict)
    
    def checkupdates(self):
        for ds in self.pltcfg.DATASETS:
            sync_files(pjoin(self.indir, ds), f"{self.pltcfg.CONDORPATH}/{ds}")
    
    def getweights(self, save=False, from_raw=False, from_load=False):
        """Compute/Load weights needed for these datasets. Save if needed."""
        if from_load:
            with open(pjoin(self.pltcfg.DATAPATH, 'wgt_total.json'), 'r') as f:
                self.wgt_dict = json.load(f)        
        else:
            self.wgt_dict = DataLoader.haddWeights(self.pltcfg.DATASETS, self.pltcfg.DATAPATH, save, from_raw)
    
    def get_totcf(self, from_load=False, lumi=50, output=True):
        """Load all cutflow tables for all datasets from output directory and combine them into one.
        
        Parameters
        - `from_load`: whether to load from output directory
        - `lumi`: luminosity (pb^-1). In the future should be eliminated. Right now for scaling purpose
        - `output`: whether to save results.

        Returns
        - Tuple of two dataframes (raw, weighted) of cutflows
        """
        if from_load:
            raw_df = pd.read_csv(pjoin(self.outdir, "cutflow_raw_tot.csv"), index_col=0)
            wgt_df = pd.read_csv(pjoin(self.outdir, "cutflow_wgt_tot.csv"), index_col=0)
        else: 
            raw_df_list = []
            wgt_df_list = []
            for process, dsitems in self.wgt_dict.items():
                for ds in dsitems.keys():
                    raw_df = combine_cf(pjoin(self.indir, process), ds, 
                                        output=True, outpath=pjoin(self.outdir, f'{ds}_cutflowraw.csv'))
                    raw_df_list.append(raw_df)
                    wgt = self.wgt_dict[process][ds]
                    wgt_df_list.append(weight_cf(self.outdir, ds, wgt, raw_df, lumi))
            
            raw_df = pd.concat(raw_df_list, axis=1)
            wgt_df = pd.concat(wgt_df_list, axis=1)
            if output:
                raw_df.to_csv(pjoin(self.outdir, "cutflow_raw_tot.csv"))
                wgt_df.to_csv(pjoin(self.outdir, "cutflow_wgt_tot.csv"))

        return raw_df, wgt_df
    
    def load_computed(self):
        """Load all computed combined csv's for datasets in store"""
        raw_pattern = pjoin(self.outdir, '*_cutflowraw.csv')
        raw_df_list = load_csvs(raw_pattern)
        wgt_pattern = pjoin(self.outdir, '*_cutflowwgt.csv')
        wgt_df_list = load_csvs(wgt_pattern)

        return raw_df_list, wgt_df_list
    
    def sort_cf(self, srcdir, save=True):
        """Create a multi index table that contains all channel cutflows for all datasets.
        :param ds_list: list of strings of dataset
        :param srcdir: output cutflow source directory
        """
        multi_indx = []
        ds_list = self.pltcfg.DATASETS
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
            finame = os.path.join(self.pltcfg['LOCAL_OUTPUT'], 'cutflow_table.csv')
            allds_cf.to_csv(finame)

        return allds_cf

    def concat_obj(self, srcdir, dsname, save=True):
        """Take a src dir and one dataset name to concat all observables.csv output in one channel""" 
        df_dict = {}
        channel_list = self.pltcfg.CHANNELS
        for j, channelname in enumerate(channel_list):
            pattern = f'{srcdir}/{dsname}/{channelname}*.csv' 
            files = glob.glob(pattern)
            dfs = [pd.read_csv(file_name, header=0) for file_name in files]
            concat_df = pd.concat(dfs)
            if save:
                outfiname = os.path.join(self.pltcfg.LOCAL_OUTPUT, f'{dsname}_{channelname}.csv')
                concat_df.to_csv(outfiname)
            df_dict.update({channelname: concat_df})
            
        return df_dict
    
    def load_allds(self):
        srcdir = self.pltcfg.LOCAL_OUTPUT
        pass

    def updatedir(self):
        """Update local input directories from condor"""
        if self.pltcfg.REFRESH:
            for ds in self.pltcfg.DATASETS:
                sync_files(pjoin(self.indir, ds),
                           pjoin(self.pltcfg.CONDORPATH, ds))
        
class DataPlotter():
    def __init__(self):
        pass
    
    def sortobj(self, sort_by, sort_what, **kwargs):
        """Return an awkward array representation of the sorted attribute in data.
        
        Parameters
        - `sort_by`: the attribute to sort by
        - `sort_what`: the attribute to be sorted
        - `kwargs`: additional arguments for sorting
        """
        mask = DataPlotter.sortmask(self.data[sort_by], **kwargs)
        return arr_handler(self.data[sort_what])[mask]
    
    @staticmethod
    def sortmask(dfarr, **kwargs):
        """Wrapper around awkward argsort function.
        
        Parameters
        - `dfarr`: the data arr to be sorted
        """
        dfarr = arr_handler(dfarr)
        sortmask = ak.argsort(dfarr, 
                   axis=kwargs.get('axis', -1), 
                   ascending=kwargs.get('ascending', False),
                   highlevel=kwargs.get('highlevel', True)
                   )
        return sortmask
    
    @staticmethod
    def deal_overflow(arr, bin_no, range):
        """Wrapper around numpy histogram function to deal with overflow.
        
        Parameters
        - `arr`: the array to be histogrammed
        - `bin_no`: number of bins
        - `range`: range of the histogram
        """
        bins = np.linspace(*range, bin_no)
        min_edge = bins[0]
        max_edge = bins[-1]
        adjusted_data = np.clip(arr, min_edge, max_edge)
        hist, bin_edges = np.histogram(adjusted_data, bins=bins)
        return hist, bin_edges

    @staticmethod
    def plot_var(hist, bin_edges, title, xlabel, range):
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title(title)
        hep.histplot(
            hist,
            bins=bin_edges,
            histtype="fill",
            color="b",
            alpha=0.5,
            edgecolor="black",
            ax=ax,
        )
        ax.set_xlabel(xlabel, fontsize=15)
        ax.set_ylabel("Events", fontsize=15)
        ax.set_xlim(*range)
        ax.legend()
        fig.show() 
            
            
class DataLoader():
    def __init__(self) -> None:
        return None
    
    def __call__(self, source, **kwargs):
        """Constructor for DataLoader class.
        
        Parameters
        - `source`: source of the data. Can be a string (path to file), a dataframe, or an awkward array.
        """
        if isinstance(source, str):
            if source.endswith('.pkl'):
                data = DataLoader.load_pkl(source)
            elif source.endswith('.csv'):
                data = pd.read_csv(source, **kwargs)
            elif source.endswith('.root'):
                data = uproot.open(source)
            elif source.endswith('.parquet'):
                data = pd.read_parquet(source, **kwargs)
            else:
                raise ValueError("This is not a valid file type.")
        elif checkevents(source):
            data = source
        else:
            data = source
            raise UserWarning(f"This might not be a valid source. The data type is {type(source)}")
        return data
    
    @staticmethod
    def load_pkl(filename):
        """Load a pickle file and return the data."""
        with open(filename, 'rb') as f:
            data = pickle.load(f)
        return data

    @staticmethod
    def findfields(dframe):
        """Find all fields in a dataframe."""
        if isinstance(dframe, pd.core.frame.DataFrame):
            return dframe.columns
        elif hasattr(dframe, 'keys') and callable(getattr(dframe, 'keys')):
            return dframe.keys()
        else:
            return "Not supported yet..."

    @staticmethod
    def haddWeights(regexlist, grepdir, output=False, from_raw=False):
        """Function for self use only, grep weights from a list of json files formatted in a specific way.
        
        Parameters
        - `regexlist`: list of strings of dataset names
        - `grepdir`: directory where the json files are located
        - `output`: whether to save the weights into a json file
        - `from_raw`: whether to compute weights based on number of raw events instead of weighted
        """
        wgt_dict = {}
        for ds in regexlist:
            with open(pjoin(grepdir, f'{ds}.json'), 'r') as f:
                meta = json.load(f)
                dsdict = {}
                for dskey, dsval in meta.items():
                    if from_raw:
                        dsdict.update({dskey: dsval['xsection']/dsval['Raw Events']})
                    else:
                        dsdict.update({dskey: dsval['Per Event']})
                wgt_dict.update({ds: dsdict})
        if output: 
            outname = pjoin(grepdir, 'wgt_total.json')
            with open(outname, 'w') as f:
                json.dump(wgt_dict, f, indent=4)
        return wgt_dict
    
    @staticmethod
    def load_vars(varname, filenames):
        pass

    @staticmethod
    def combine_roots(pltcfg, wgt_dict, level=1, out_suffix='', **kwargs) -> None:
        """Combine all root files of datasets in plot setting into one dataframe.
        
        Parameters
        - `level`: concatenation level. 0 for overall process, 1 for dataset
        - `wgt_dict`: dictionary of process, dataset, and weights
        - `flat`: whether it's n-tuple
        - `out_suffix`: suffix for the output file
        """
        outdir = pltcfg.OUTPUTDIR
        checkpath(outdir)
        for process, dsitems in wgt_dict.items():
            indir = pltcfg.INPUTDIR
            ds_dir = pjoin(indir, process)
            for ds in dsitems.keys():
                added_columns = {'dataset': process} if level==0 else {'dataset': ds} 
                empty_fis = concat_roots(directory=ds_dir,
                                         startpattern=f'{ds}_',
                                         fields=pltcfg.PLOT_VARS, 
                                         outdir=outdir,
                                         outname=ds+out_suffix,
                                         extra_branches=pltcfg.EXTRA_VARS, 
                                         tree_name = pltcfg.TREENAME,
                                         added_columns=added_columns,
                                         **kwargs)
                if pltcfg.CONDOR_TRANSFER:
                    transferfiles(outdir, pltcfg.CONDORPATH)
                    delfiles(outdir, pattern='*.pkl')
        if empty_fis != [] & pltcfg.CLEAN: delfilelist(empty_fis)
        return None

def clopper_pearson_error(passed, total, level=0.6827):
    """
    matching TEfficiency::ClopperPearson(),
    >>> ROOT.TEfficiency.ClopperPearson(total, passed, level, is_upper)
    """
    import scipy.stats

    alpha = 0.5 * (1.0 - level)
    low = scipy.stats.beta.ppf(alpha, passed, total - passed + 1)
    high = scipy.stats.beta.ppf(1 - alpha, passed + 1, total - passed)
    return low, high
    
def simplifyError(passed,total,level=0.6827):
    low,high=clopper_pearson_error(passed, total, level)
    err=high-passed
    return err

# style a dataframe table
def makePretty(styler,color_code):
    styler.format(precision=3)
    css_indexes=f'background-color: {color_code}; color: white;'
    styler.applymap_index(lambda _: css_indexes, axis=1)
    return styler




    
         
        
        