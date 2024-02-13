import mplhep as hep
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import warnings
from analysis.helper import *

class Visualizer():
    def __init__(self, plt_cfg):
        self._pltcfg = plt_cfg
        
    @property
    def pltcfg(self):
        return self._pltcfg

    def combine_cf(self, sourcedir):
        """Combines all cutflow tables in source directory and output them into output directory"""
        pattern = f'{sourcedir}/cutflow_*.csv'
        file_names = glob.glob(pattern)
        
        dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names]
        concat_df = pd.concat(dfs)
        combined = concat_df.groupby(concat_df.index, sort=False).sum()
        
        return combined
    
    def efficiency(self, cfdf):
        """Add efficiency for the cutflow table"""
        for column in cfdf.columns:
            cfdf[f'{column}_eff'] = cfdf[column]/cfdf[column].shift(1)
        cfdf.replace([np.inf, -np.inf], np.nan, inplace=True)
        cfdf.fillna(1, inplace=True)
    
    def beautify_cf(self, cfdf):
        pass




    def sort_cf(self, srcdir, ds_list, save=True):
        """Create a multi index table that contains all channel cutflows for all datasets.
        :param ds_list: list of strings of dataset
        :param srcdir: output cutflow source directory
        """
        multi_indx = []
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
    styler.hide(axis='index')
    styler.format(precision=3)
    css_indexes=f'background-color: {color_code}; color: white;'
    styler.applymap_index(lambda _: css_indexes, axis=1)
    return styler




    
         
        
        