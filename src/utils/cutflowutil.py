import os, subprocess
import numpy as np
import pandas as pd
import awkward as ak
from coffea.analysis_tools import PackedSelection, Cutflow
import coffea.util
import dask_awkward, dask
from collections import namedtuple

from utils.filesysutil import glob_files 
pjoin = os.path.join
runcom = subprocess.run

class weightedCutflow(Cutflow):
    def __init__(
        self, names, nevonecut, nevcutflow, wgtevcutflow, masksonecut, maskscutflow, delayed_mode
    ):
        self._names = names
        self._nevonecut = nevonecut
        self._nevcutflow = nevcutflow
        self._wgtevcutflow = wgtevcutflow
        self._masksonecut = masksonecut
        self._maskscutflow = maskscutflow
        self._delayed_mode = delayed_mode
    
    def __add__(self, cutflow2):
        if self._delayed_mode != cutflow2._delayed_mode:
            raise TypeError("Concatenation of delayed and computed cutflows are not supported now!")
        names = self._names + cutflow2._names
        nevonecut = self._nevonecut + cutflow2._nevonecut
        nevcutflow = self._nevcutflow + cutflow2._nevcutflow
        wgtevcutflow = self._wgtevcutflow + cutflow2._wgtevcutflow
        masksonecut = self._masksonecut + cutflow2._masksonecut
        maskscutflow = self._maskscutflow + cutflow2._maskscutflow

        return weightedCutflow(names, nevonecut, nevcutflow, wgtevcutflow, masksonecut, maskscutflow, self._delayed_mode)

    def result(self):
        """Returns the results of the cutflow as a namedtuple

        Returns
        -------
            result : CutflowResult
                A namedtuple with the following attributes:

                nevonecut : list of integers or dask_awkward.lib.core.Scalar objects
                    The number of events that survive each cut alone as a list of integers or delayed integers
                nevcutflow : list of integers or dask_awkward.lib.core.Scalar objects
                    The number of events that survive the cumulative cutflow as a list of integers or delayed integers
                wgtevcutflow: list of integers or dask_awesome.lib.core.Scalar objects
                    The number of events that survive the weighted cutflow as a list of integers or delayed integers
                masksonecut : list of boolean numpy.ndarray or dask_awkward.lib.core.Array objects
                    The boolean mask vectors of which events pass each cut alone as a list of materialized or delayed boolean arrays
                maskscutflow : list of boolean numpy.ndarray or dask_awkward.lib.core.Array objects
                    The boolean mask vectors of which events pass the cumulative cutflow a list of materialized or delayed boolean arrays
        """
        CutflowResult = namedtuple(
            "CutflowResult",
            ["labels", "nevonecut", "nevcutflow", "wgtevcutflow", "masksonecut", "maskscutflow"],
        )
        labels = ["initial"] + list(self._names)
        return CutflowResult(
            labels,
            self._nevonecut,
            self._nevcutflow,
            self._wgtevcutflow,
            self._masksonecut,
            self._maskscutflow,
        )

class weightedSelection(PackedSelection):
    def __init__(self, perevtwgt, dtype="uint32"):
        """An inherited class that represents a set of selections on a set of events with weights
        Parameters
        - ``perevtwgt`` : dask.array.Array that represents the weights of the events
        """
        super().__init__(dtype)
        self._perevtwgt = perevtwgt
    
    def add_sequential(self, name, thissel, lastsel, fill_value=False):
        if isinstance(thissel, dask.array.Array) or isinstance(lastsel, dask.array.Array):
            raise ValueError(
                "Dask arrays are not supported, please convert them to dask_awkward.Array by using dask_awkward.from_dask_array()"
            )
        thissel = coffea.util._ensure_flat(thissel, allow_missing=True)
        lastsel = coffea.util._ensure_flat(lastsel, allow_missing=True)
        last1 = lastsel[lastsel==True]
        result1 = last1 & thissel
        result = np.full(lastsel.shape, False)
        result[lastsel==True] = result1
        if isinstance(result, np.ndarray):
            self._PackedSelection__add_eager(name, result, fill_value)
        elif isinstance(result, dask_awkward.Array):
            self._PackedSelection__add_delayed(name, result, fill_value)
    
    def cutflow(self, *names) -> weightedCutflow:
        for cut in names:
            if not isinstance(cut, str) or cut not in self._names:
                raise ValueError(
                    "All arguments must be strings that refer to the names of existing selections"
                )
        masksonecut, maskscutflow, maskwgtcutflow = [], [], []
        for i, cut in enumerate(names):
            mask1 = self.any(cut)
            mask2 = self.all(*(names[: i + 1]))
            maskwgt = self._perevtwgt[mask2]
            masksonecut.append(mask1)
            maskscutflow.append(mask2)
            maskwgtcutflow.append(maskwgt)

        if not self.delayed_mode:
            nevonecut = [len(self._data)]
            nevcutflow = [len(self._data)]
            nevonecut.extend(np.sum(masksonecut, axis=1, initial=0))
            nevcutflow.extend(np.sum(maskscutflow, axis=1, initial=0))
            if self._perevtwgt is not None:
                wgtevcutflow = [np.sum(self._perevtwgt)]
                wgtevcutflow.extend([np.sum(ak.to_numpy(maskwgt), initial=0) for maskwgt in maskwgtcutflow])
            else:
                wgtevcutflow = None

        else:
            nevonecut = [dask_awkward.count(self._data, axis=0)]
            nevcutflow = [dask_awkward.count(self._data, axis=0)]
            
            def catchZeroArr(mask):
                try:
                    return dask_awkward.sum(mask)
                except:
                    return np.sum(mask.compute(), initial=0)

            nevonecut.extend([catchZeroArr(mask1) for mask1 in masksonecut])
            nevcutflow.extend([catchZeroArr(mask2) for mask2 in maskscutflow])

            if self._perevtwgt is not None:
                wgtevcutflow = [catchZeroArr(self._perevtwgt)] 
                wgtevcutflow.extend([catchZeroArr(self._perevtwgt[mask2]) for mask2 in maskscutflow])
            else:
                wgtevcutflow = None

        return weightedCutflow(
            names, nevonecut, nevcutflow, wgtevcutflow, masksonecut, maskscutflow, self.delayed_mode
        )

def load_csvs(dirname, startpattern, func=None, *args, **kwargs) -> pd.DataFrame:
    """Load csv files matching a pattern into a list of DataFrames."""
    file_names = glob_files(dirname, startpattern=startpattern, endpattern='.csv')
    dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names] 
    if func is None:
        return dfs
    else:
        return func(dfs, *args, **kwargs)

def combine_cf(inputdir, dsname, keyword='cutflow', output=True, outpath=None):
    """Combines all cutflow tables in a source directory belonging to one datset and output them into output directory.
    Essentially this will grep files of pattern "{dsname}_{keyword}*.csv" and combine them to one csv file.
    
    Parameters
    - `inputdir`: source directory
    - `dsname`: dataset name. 
    - `keyword`: keyword in file names to search for
    - `output`: whether to save the combined table into a csv file
    - `outpath`: path to the output
    """
    concat_df = load_csvs(dirname=inputdir, startpattern=f'{dsname}_cutflow', 
                          func=lambda dfs: pd.concat(dfs))
    combined = concat_df.groupby(concat_df.index, sort=False).sum()
    if combined.shape[1] != 1:
        combined.columns = [f"{dsname}_{col}" for col in combined.columns]
    else:
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

def efficiency(outdir, cfdf, overall=True,  save=False, save_name='tot'):
    """Add or return efficiency for the cutflow table.
    
    Parameters
    - `outdir`: name of the output directory
    - `cfdf`: cutflow dataframe
    - `overall`: whether to calculate overall efficiency
    - `save`: whether to save the efficiency table
    - `save_name`: name of the saved efficiency table. If none is given, it will be named 'tot_eff.csv'
    """
    if not overall:
        efficiency_df = incrementaleff(cfdf)
    else:
        efficiency_df = overalleff(cfdf)
    efficiency_df *= 100
    efficiency_df.columns = [f'{col}_eff' for col in cfdf.columns]
    return_df = efficiency_df
    if save:
        finame = pjoin(outdir, f'{save_name}_eff.csv')
        return_df.to_csv(finame)
    return return_df

def incrementaleff(cfdf, column_name=None):
    """Return incremental efficiency for a table/column"""
    if column_name is None: eff_df = cfdf.div(cfdf.shift(1)).fillna(1)
    else: eff_df = cfdf[column_name].div(cfdf[column_name].shift(1)).fillna(1)
    eff_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    eff_df.fillna(0, inplace=True) 
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