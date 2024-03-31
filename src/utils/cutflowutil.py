import os, subprocess
import numpy as np
import pandas as pd
from utils.filesysutil import glob_files 

pjoin = os.path.join
runcom = subprocess.run

def load_csvs(dirname, startpattern):
    """Load csv files matching a pattern into a list of DataFrames."""
    file_names = glob_files(dirname, startpattern=startpattern, endpattern='.csv')
    dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names] 
    return dfs

def hadd_csvs(pattern):
    """return a combined DataFrame from csv files matching a pattern."""
    dfs = load_csvs(pattern)
    return pd.concat(dfs, axis=1)

def combine_cf(inputdir, dsname, output=True, outpath=None):
    """Combines all cutflow tables in a source directory belonging to one datset and output them into output directory.
    
    Parameters
    - `inputdir`: source directory
    - `dsname`: dataset name. 
    - `output`: whether to save the combined table into a csv file
    - `outpath`: path to the output
    """
    dfs = load_csvs(dirname=inputdir, startpattern=f'{dsname}_cutflow')
    concat_df = pd.concat(dfs)
    combined = concat_df.groupby(concat_df.index, sort=False).sum()
    combined.columns = [dsname]
    if output and outpath is not None: combined.to_csv(outpath)
    return combined

def add_selcutflow(cutflowlist, save=True, outpath=None):
    """Add cutflows sequentially.
    
    Parameters
    - `cutflowlist`: list of cutflow csv files
    - `save`: whether to save the combined table into a csv file
    - `outpath`: path to the output
    
    Return
    - combined cutflow table"""
    dfs = load_csvs(cutflowlist)
    dfs = [df.iloc[1:] for i, df in enumerate(dfs) if i != 0]
    result = pd.concat(dfs, axis=1)
    if save: result.to_csv(outpath)
    return result

def weight_cf(wgt_dict, raw_cf, save=False, outname=None, lumi=50):
    """Calculate weighted table based on raw table.
    
    Parameters
    - `wgt_dict`: dictionary of weights
    - `raw_cf`: raw cutflow table
    - `lumi`: luminosity (pb^-1)

    Return
    - `wgt_df`: weighted cutflow table
    """ 
    weights = {key: wgt*lumi for key, wgt in wgt_dict.items()}
    wgt_df = raw_cf.mul(weights)
    if save and outname is not None: wgt_df.to_csv(outname)
    return wgt_df

def efficiency(outdir, cfdf, overall=True, append=True, save=False, save_name='tot'):
    """Add or return efficiency for the cutflow table.
    
    Parameters
    - `outdir`: name of the output directory
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
        finame = pjoin(outdir, f'{save_name}_eff.csv')
        return_df.to_csv(finame)
    return return_df

def incrementaleff(cfdf):
    """Return incremental efficiency for a table."""
    eff_df = cfdf.div(cfdf.shift(1)).fillna(1)
    eff_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    eff_df.fillna(1, inplace=True) 
    return eff_df

def overalleff(cfdf):
    """Return efficiency wrt total events."""
    first_row = cfdf.iloc[0]
    eff_df = cfdf.div(first_row).fillna(1)
    eff_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    eff_df.fillna(1, inplace=True)
    return eff_df

def sort_cf(ds_list, srcdir, outdir, save=True):
    """Create a multi index table that contains all channel cutflows for all datasets.
    :param ds_list: list of strings of dataset
    :param srcdir: output cutflow source directory
    """
    multi_indx = []
    df_list = [None]*len(ds_list)
    for i, ds in enumerate(ds_list):
        ds_dir = os.path.join(srcdir, ds)
        ds_cf = combine_cf(ds_dir, ds, save=False, outpath=None)
        efficiency(outdir=None, cfdf=ds_cf, save=False)
        df_list[i] = ds_cf
        multi_indx += [(ds, indx) for indx in ds_cf.index]
    
    allds_cf = pd.concat(df_list)
    allds_cf.index = pd.MultiIndex.from_tuples(multi_indx, names=['Process', 'Selection'])

    if save: 
        finame = os.path.join(outdir, 'cutflow_table.csv')
        allds_cf.to_csv(finame)

    return allds_cf