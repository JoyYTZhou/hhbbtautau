import os
import glob
import subprocess
import logging
import pandas as pd
import numpy as np
import uproot
import random
from utils.filesysutil import *

runcom = subprocess.run
pjoin = os.path.join

def logresult(result, success_msg):
    if result.returncode == 0:
        logging.debug(success_msg)
    else:
        # Ensure stderr is a string. Decode if it's bytes.
        stderr_message = result.stderr.decode('utf-8') if isinstance(result.stderr, bytes) else result.stderr
        # Check if stderr is empty or None
        if not stderr_message:
            stderr_message = "No error message available."
        logging.info(f"Operation not successful! Return code: {result.returncode}. Here's the error message =========================\n{stderr_message}")


def initLogger(name, suffix):
    """Initialize a logger for a module."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    debug_handler = logging.FileHandler(f"{name}_daskworker_{suffix}.log")
    debug_handler.setLevel(logging.DEBUG)

    error_handler = logging.FileHandler(f"{name}daskworker_{suffix}.err")
    error_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)

    return logger

def load_csvs(pattern):
    """Load csv files matching a pattern into a list of DataFrames."""
    file_names = glob.glob(pattern)
    dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names] 
    return dfs

def hadd_csvs(pattern):
    """return a combined DataFrame from csv files matching a pattern."""
    dfs = load_csvs(pattern)
    return pd.concat(dfs, axis=1)

def load_roots(filelist, branch_names, tree_name):
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


