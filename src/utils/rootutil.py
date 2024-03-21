import os
import pandas as pd
import numpy as np
import uproot
import json
import pickle
import random
import gc
from utils.filesysutil import *

pjoin = os.path.join

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
                gc.collect()
                if pltcfg.CONDOR_TRANSFER:
                    transferfiles(outdir, pltcfg.CONDORPATH)
                    delfiles(outdir, pattern='*.pkl')
        if empty_fis != [] & pltcfg.CLEAN: delfilelist(empty_fis)
        return None

def checkevents(events):
    """Returns True if the events are in the right format, False otherwise."""
    if hasattr(events, 'keys') and callable(getattr(events, 'keys')):
        return True
    elif hasattr(events, 'fields'):
        return True
    elif isinstance(events, pd.core.frame.DataFrame):
        return True
    else:
        raise TypeError("Invalid type for events. Must be an awkward object or a DataFrame")

def arr_handler(dfarr):
    """Handle different types of data arrays to convert them to awkward arrays."""
    if isinstance(dfarr, pd.core.series.Series):
        try: 
            ak_arr = dfarr.ak.array
            return ak_arr
        except AttributeError as e:
            return dfarr
    elif isinstance(dfarr, pd.core.frame.DataFrame):
        raise ValueError("specify a column. This is a dataframe.")
    elif isinstance(dfarr, ak.highlevel.Array):
        return dfarr
    else:
        raise TypeError(f"This is of type {type(dfarr)}")


def make_ntuple(filelist, outname, outdir, tree_name='tree', branch_names=None):
    pass

def load_roots(filelist, **kwargs):
    pass

def evts_to_roots(events, destination, **kwargs):

    compression = kwargs.pop('compression', None)
    compression_level = kwargs.pop('compression_level', 1)
    tree_name = kwargs.pop('tree_name', 'Events')

    if compression in ("LZMA", "lzma"):
        compression_code = uproot.const.kLZMA
    elif compression in ("ZLIB", "zlib"):
        compression_code = uproot.const.kZLIB
    elif compression in ("LZ4", "lz4"):
        compression_code = uproot.const.kLZ4
    elif compression in ("ZSTD", "zstd"):
        compression_code = uproot.const.kZSTD
    elif compression is None:
        raise UserWarning("Not sure if this option is supported, should be...")
    else:
        msg = f"unrecognized compression algorithm: {compression}. Only ZLIB, LZMA, LZ4, and ZSTD are accepted."
        raise ValueError(msg)
    
    if compression is not None: 
        compression = uproot.compression.Compression.from_code_pair(compression_code, compression_level)
    
    out_file = uproot.recreate(
        destination,
        compression=compression
    )
    branch_types = {name: events[name].type for name in events.fields}
    
    out_file.mktree(name=tree_name,
                    branch_types=branch_types,
                    title='Events')
    out_file[tree_name].extend({name: events[name] for name in events.fields})

    return out_file


def load_roots_pd(filelist, branch_names, tree_name):
    """Load root files in filelist and combine them into a single DataFrame.
    
    Parameters:
    - filelist: list of file paths
    - branch_names: list of branch names to load
    - tree_name: name of the tree in the root file

    Returns:
    - A pandas DataFrame containing the combined data from all root files in filelist.
    - A list of empty files
    """
    emptylist = []
    dfs = []
    for root_file in filelist:
        with uproot.open(root_file) as file:
            if file.keys() == []:
                emptylist.append(root_file) 
            else:
                tree = file[tree_name]
                df = tree.arrays(branch_names, library="pd")
                dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df, emptylist

def concat_roots(directory, startpattern, fields, outdir, outname, batch_size=35, extra_branches = [], tree_name='tree', added_columns={}):
    """
    Load specific branches from ROOT files matching a pattern in a directory, and combine them into a single DataFrame.

    Parameters:
    - directory: Path to the directory containing ROOT files.
    - startpattern: Pattern to match the start of the ROOT file name.
    - fields: List of field names to load from each ROOT file.
    - outdir: Path to the directory to save the combined DataFrame.
    - outname: Name of the combined DataFrame.
    - batch_size: Number of ROOT files to load at a time.
    - extra_branches: List of extra branches to load from each ROOT file.
    - tree_name: Name of the tree to load
    - added_columns: Dictionary of additional columns to add to the combined DataFrame.

    Returns:
    - A list of empty files among the searched ROOT files
    """
    checkpath(outdir)
    root_files = glob_files(directory, startpattern, endpattern='.root')
    random.shuffle(root_files)
    emptyfiles = []
    branch_names = find_branches(root_files[0], fields, tree_name) 
    branch_names.extend(extra_branches)
    for i in range(0, len(root_files), batch_size):
        batch_files = root_files[i:i+batch_size]
        combined_df, empty_list = load_roots(batch_files, branch_names, tree_name)
        emptyfiles.extend(empty_list)
        if added_columns != {}: 
            for column, value in added_columns.items():
                combined_df[column] = value
        outfilepath = pjoin(outdir, f'{outname}_{i//batch_size + 1}.pkl')
        combined_df.to_pickle(outfilepath)
    return emptyfiles

def find_branches(file_path, object_list, tree_name) -> list:
    """ Return a list of branches for objects in object_list

    Paremters
    - `file_path`: path to the root file
    - `object_list`: list of objects to find branches for
    - `tree_name`: name of the tree in the root file

    Returns
    - list of branches
    """
    file = uproot.open(file_path)
    tree = file[tree_name]
    branch_names = tree.keys()
    branches = []
    for object in object_list:
        branches.extend([name for name in branch_names if name.startswith(object)])
    return branches

def combine_cf(inputdir, dsname, output=True, outpath=None):
    """Combines all cutflow tables in a source directory belonging to one datset and output them into output directory.
    
    Parameters
    - `inputdir`: source directory
    - `dsname`: dataset name. 
    - `output`: whether to save the combined table into a csv file
    - `outpath`: path to the output
    """
    dirpattern = pjoin(inputdir, f'{dsname}_cutflow*.csv')
    dfs = load_csvs(dirpattern)

    concat_df = pd.concat(dfs)
    combined = concat_df.groupby(concat_df.index, sort=False).sum()
    combined.columns = [dsname]

    if output and outpath is not None:
        combined.to_csv(outpath)
    
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


