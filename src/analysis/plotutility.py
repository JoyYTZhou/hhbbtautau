import mplhep as hep
import matplotlib.pyplot as plt
import glob
import os
import json
from analysis.helper import *
import pickle
import awkward as ak

class Visualizer():
    """
    This class provides methods for plotting, data manipulation, and analysis of datasets.
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
        plt.style.use(hep.style.CMS)

    @property
    def pltcfg(self):
        return self._pltcfg
    
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
    
    def compute_allcf(self, lumi=50, output=True):
        """Load all cutflow tables for all datasets from output directory and combine them into one.
        
        Parameters
        - `lumi`: luminosity (pb^-1). In the future should be eliminated. Right now for scaling purpose
        - `output`: whether to save results.

        Returns
        - Tuple of two dataframes (raw, weighted) of cutflows
        """
        raw_df_list = []
        wgt_df_list = []
        for process, dsitems in self.wgt_dict.items():
            for ds in dsitems.keys():
                raw_df = combine_cf(pjoin(self.indir, process), ds, 
                                    output=True, outpath=pjoin(self.outdir, f'{ds}_cutflowraw.csv'))
                raw_df_list.append(raw_df)
                wgt = self.wgt_dict[process][ds]
                wgt_df_list.append(self.weight_cf(ds, wgt, raw_df, lumi))
        
        raw_df = pd.concat(raw_df_list, axis=1)
        wgt_df = pd.concat(wgt_df_list, axis=1)

        if output:
            raw_df.to_csv(pjoin(self.outdir, "cutflow_raw_tot.csv"))
            wgt_df.to_csv(pjoin(self.outdir, "cutflow_wgt_tot.csv"))

        return raw_df, wgt_df

    def efficiency(self, cfdf, overall=True, append=True, save=False, save_name=None):
        """Add or return efficiency for the cutflow table.
        
        Parameters
        - `cfdf`: cutflow dataframe
        - `overall`: whether to calculate overall efficiency
        - `append`: whether to append efficiency columns to the input dataframe
        - `save`: whether to save the efficiency table
        - `save_name`: name of the saved efficiency table. If none is given, it will be named 'tot_eff.csv'
        """
        if not overall:
            efficiency_df = incrementaleff(cfdf)
        else:
            efficiency_df = overalleff(cfdf)
        efficiency_df *= 100
        efficiency_df.columns = [f'{col}_eff' for col in cfdf.columns]
        if append:
            for col in efficiency_df.columns:
                cfdf[col] = efficiency_df[col]
            return_df = cfdf
        else:
            return_df = efficiency_df
        if save:
            finame = pjoin(self.outdir, f'{save_name}_eff.csv') if save_name else pjoin(self.outdir, 'tot_eff.csv')
            return_df.to_csv(finame)
        return return_df
    


    def load_allcf(self):
        raw_df = pd.read_csv(pjoin(self.outdir, "cutflow_raw_tot.csv"), index_col=0)
        wgt_df = pd.read_csv(pjoin(self.outdir, "cutflow_wgt_tot.csv"), index_col=0)
        return raw_df, wgt_df

    def weight_cf(self, dsname, wgt, raw_cf, lumi=50):
        """Calculate weighted table based on raw table.""" 
        wgt_df = raw_cf * wgt * lumi
        wgt_df.columns = [dsname]
        outfiname = pjoin(self.outdir, f'{dsname}_cutflowwgt.csv')
        wgt_df.to_csv(outfiname)
        return wgt_df
    
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
            self.efficiency(ds_cf)
            df_list[i] = ds_cf
            multi_indx += [(ds, indx) for indx in ds_cf.index]
        
        allds_cf = pd.concat(df_list)
        allds_cf.index = pd.MultiIndex.from_tuples(multi_indx, names=['Process', 'Selection'])

        if save: 
            finame = os.path.join(self.pltcfg['LOCAL_OUTPUT'], 'cutflow_table.csv')
            allds_cf.to_csv(finame)

        return allds_cf

    def plot_var(self, df, name, title, xlabel, bins, range):
        fig, ax = plt.subplots(figsize=(10, 5))
        hep.histplot(
            df[name],
            bins=bins,
            histtype="fill",
            color="b",
            alpha=0.5,
            edgecolor="black",
            title=title,
            ax=ax,
        )
        ax.set_xlabel(xlabel, fontsize=15)
        ax.set_ylabel("Events", fontsize=15)
        ax.set_xlim(*range)
        ax.legend()
        fig.show()
    
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
    def __init__(self, source, **kwargs) -> None:
        """Constructor for DataPlotter class.
        
        Parameters
        - `source`: source of the data. Can be a string (path to file), a dataframe, or an awkward array.
        """
        if isinstance(source, str):
            if source.endswith('.pkl'):
                self.data = self.load_pkl(source)
            elif source.endswith('.csv'):
                self.data = pd.read_csv(source, **kwargs)
            elif source.endswith('.root'):
                self.data = uproot.open(source)
        elif isinstance(source, pd.core.frame.DataFrame) or isinstance(source, ak.highlevel.Array):
            self.data = source
        else:
            self.data = source
            raise UserWarning(f"This might not be a valid source. The data type is {type(source)}")
    
    def sortobj(self, field, attr, sortall=True, **kwargs):
        mask = DataPlotter.sortmask(self.data[f'{field}_{attr}'], **kwargs)
        if sortall:
            fieldlist = DataLoader.findfields(self.data) 
        pass

    @staticmethod
    def load_pkl(filename):
        with open(filename, 'rb') as f:
            data = pickle.load(f)
        return data

    @staticmethod
    def sortmask(dfarr, **kwargs):
        """Wrapper around awkward argsort function.
        
        Parameters
        - `dfarr`: the data arr to be sorted
        """
        dfarr = DataLoader.arr_handler(dfarr)
        sortmask = ak.argsort(dfarr, 
                   axis=kwargs.get('axis', -1), 
                   ascending=kwargs.get('ascending', False),
                   highlevel=kwargs.get('highlevel', True)
                   )
        return sortmask
    
    @staticmethod
    def plot_var(df, name, title, xlabel, bins, range):
        fig, ax = plt.subplots(figsize=(10, 5))
        hep.histplot(
            df[name],
            bins=bins,
            histtype="fill",
            color="b",
            alpha=0.5,
            edgecolor="black",
            title=title,
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
    def arr_handler(dfarr):
        """Handle different types of data arrays."""
        if isinstance(dfarr, pd.core.series.Series):
            return dfarr.ak.array
        elif isinstance(dfarr, pd.core.frame.DataFrame):
            raise ValueError("specify a column. This is a dataframe.")
        elif isinstance(dfarr, ak.highlevel.Array):
            return dfarr
        else:
            raise TypeError(f"This is of type {type(dfarr)}")

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
    def load_roots(outdir):
        """Load hadded csv files, if any."""
        fipath = pjoin(outdir, 'hadded_roots.csv')
        if os.path.exists(fipath):
            roots_df = pd.read_csv(fipath)
            return roots_df
        else:
            print("You didn't save the hadded result last time!")
            return False
        
    @staticmethod
    def combine_roots(pltcfg, wgt_dict, level=1, flat=False):
        """Combine all root files of datasets in plot setting into one dataframe.
        
        Parameters
        - `level`: concatenation level. 0 for overall process, 1 for dataset
        - `save`: whether to save the hadded result (dataframe)
        - `save_separate`: whether to save separate csv files for each dataset
        - `flat`: whether it's n-tuple
        """
        outdir = pltcfg.OUTPUTDIR
        for process, dsitems in wgt_dict.items():
            for ds in dsitems.keys():
                indir = pltcfg.INPUTDIR
                ds_dir = pjoin(indir, process)
                added_columns = {'dataset': process} if level==0 else {'dataset': ds} 
                empty_fis = concat_roots(directory=ds_dir, pattern=f'{ds}_*.root', fields=pltcfg.PLOT_VARS, 
                                   outdir=outdir,
                                   outname=ds,
                                   extra_branches=pltcfg.EXTRA_VARS, 
                                   tree_name = pltcfg.TREENAME,
                                   added_columns=added_columns
                                   )
        return None
    
        
    # from https://github.com/aminnj/yahist/blob/master/yahist/utils.py#L133 
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




    
         
        
        