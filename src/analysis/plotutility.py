import mplhep as hep
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
import json
from analysis.helper import *

class Visualizer():
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
    
    def getAll(self):
        pass
        
    def grepweights(self, output=False):
        """Function for self use only, grep weights from a list json files formatted in a specific way."""
        wgt_dict = {}
        for ds in self.pltcfg.DATASETS:
            with open(pjoin(self.pltcfg.DATAPATH, f'{ds}.json'), 'r') as f:
                meta = json.load(f)
                dsdict = {}
                for dskey, dsval in meta.items():
                    dsdict.update({dskey: dsval['Per Event']})
                wgt_dict.update({ds: dsdict})
        self.wgt_dict = wgt_dict

        if output: 
            outname = pjoin(self.pltcfg.DATAPATH, 'wgt_total.json')
            with open(outname, 'w') as f:
                json.dump(self.wgt_dict, f, indent=4)
    
    def loadweights(self):
        pass

    def combine_cf(self, inputdir, dsname, output=True):
        """Combines all cutflow tables in source directory belonging to one datset and output them into output directory"""
        dirpattern = pjoin(inputdir, f'{dsname}_cutflow*.csv')
        dfs = load_csvs(dirpattern)

        concat_df = pd.concat(dfs)
        combined = concat_df.groupby(concat_df.index, sort=False).sum()
        combined.columns = [dsname]
        
        if output: 
            finame = pjoin(self.outdir, f'{dsname}_cutflowraw.csv') 
            combined.to_csv(finame)
        
        return combined
    
    def efficiency(self, cfdf):
        """Add efficiency for the cutflow table"""
        for column in cfdf.columns:
            cfdf[f'{column}_eff'] = cfdf[column]/cfdf[column].shift(1)
        cfdf.replace([np.inf, -np.inf], np.nan, inplace=True)
        cfdf.fillna(1, inplace=True)
    
    def beautify_cf(self, cfdf):
        pass

    def compute_allcf(self, lumi=50, output=True):
        """Load all cutflow tables for all datasets from output directory and combine them into one"""
        raw_df_list = []
        wgt_df_list = []
        for process, dsitems in self.wgt_dict.items():
            for ds in list(dsitems.keys()):
                raw_df = self.combine_cf(pjoin(self.indir, process), ds)
                raw_df_list.append(raw_df)
                wgt = self.wgt_dict[process][ds]
                wgt_df_list.append(self.weight_cf(ds, wgt, raw_df, lumi))
        
        raw_df = pd.concat(raw_df_list, axis=1)
        wgt_df = pd.concat(wgt_df_list, axis=1)

        if output:
            raw_df.to_csv(pjoin(self.outdir, "cutflow_raw_tot.csv"))
            wgt_df.to_csv(pjoin(self.outdir, "cutflow_wgt_tot.csv"))

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

    def plot_var(self, df, name, title, bins, range):
        fig, ax = plt.subplots(figsize=(10, 5))
        hep.histplot(
            df[name],
            bins=bins,
            histtype="fill",
            color="b",
            alpha=0.5,
            edgecolor="black",
            label=r"ZZ $\rightarrow$ 4l",
            ax=ax,
        )
        ax.set_xlabel("4l invariant mass (GeV)", fontsize=15)
        ax.set_ylabel("Events / 3 GeV", fontsize=15)
        ax.set_xlim(rmin, rmax)
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




    
         
        
        