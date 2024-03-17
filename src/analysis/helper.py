import os
import glob
import subprocess
from pathlib import Path
import logging
import shutil
import pandas as pd
import numpy as np
from datetime import datetime
import uproot

runcom = subprocess.run
pjoin = os.path.join
PREFIX = "root://cmseos.fnal.gov"

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

def cproot(srcpath, localpath):
    """Copy a root file FROM condor to LOCAL."""
    
    comstr = f'xrdcp {srcpath} {localpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    logresult(result, "Transfer file from condor successful!")
    return result

def cpcondor(srcpath, destpath, is_file=True):
    """Copy srcpath (file/directory) FROM local to condor destpath"""

    comstr = f'xrdcp -f {srcpath} {PREFIX}/{destpath}' if is_file else f'xrdcp -r {srcpath} {destpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    logresult(result, "Transfer files/directories successful!")
    return result

def transferfiles(srcpath, destpath):
    """Copy all files FROM one local directory to a condor directory. 
    If destpath does not exist, will create one."""
    checkcondorpath(destpath)
    for srcfile in Path(srcpath).iterdir():
        if srcfile.is_file():
            cpcondor(str(srcfile), f"{destpath}/{srcfile.name}", is_file=True)

def delfiles(dirname, pattern='*.root'):
    """Delete all files in a directory with a specific pattern."""
    if pattern is not None:
        dirpath = Path(dirname)
        for fipath in dirpath.glob(pattern):
            fipath.unlink()
            logging.info(f"Deleted {fipath}")

def filter_xrdfs_files(remote_dir, start_pattern, end_pattern, add_prefix=True):
    """Filter XRDFS files in a remote directory by a specific file ending."""
    all_files = list_xrdfs_files(remote_dir, PREFIX)
    if add_prefix:
        filtered_files = [PREFIX + "/" + f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
    else: 
        filtered_files = [f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
    return filtered_files

def checkcondorpath(dirname):
    """Check if a condor path exists. If not will create one."""
    check_dir_cmd = f"xrdfs {PREFIX} stat {dirname}"
    create_dir_cmd = f"xrdfs {PREFIX} mkdir -p {dirname}"

    proc = runcom(check_dir_cmd, shell=True, capture_output=True, text=True) 

    if proc.returncode == 0:
        logging.debug(f"The directory {dirname} already exists.")
    else:
        logging.info(f"Creating directory {dirname}.")
        runcom(create_dir_cmd, shell=True)
    
def checkpath(pathstr):
    """Check if a local path exists. If not will create one."""
    path = Path(pathstr) 
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created directory: {path}")
    else:
        logging.debug(f"Directory already exists: {path}")

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

def concat_roots(directory, pattern, fields, outdir, outname, batch_size=30, extra_branches = [], tree_name='tree', added_columns={}):
    """
    Load specific branches from ROOT files matching a pattern in a directory, and combine them into a single DataFrame.

    Parameters:
    - directory: Path to the directory containing ROOT files.
    - pattern: Pattern to match ROOT files, e.g., "*.root".
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
    full_pattern = os.path.join(directory, pattern)
    root_files = glob.glob(full_pattern)
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
        # outfilepath = pjoin(outdir, f'{outname}_{i//batch_size + 1}.parquet')
        # combined_df.to_parquet(outfilepath)
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

def delfilelist(filelist):
    """Remove a list of file"""
    for file_path in filelist:
        file = Path(file_path)
        try:
            file.unlink()
            print(f"Deleted {file}")
        except FileNotFoundError:
            print(f"File not found: {file}")
        except PermissionError:
            print(f"Permission denied: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")
    return None

def list_xrdfs_files(remote_dir):
    """List files/dirs in a remote xrdfs directory using subprocess.run."""
    cmd = ["xrdfs", PREFIX, "ls", remote_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = result.stdout.strip().split('\n')
    return files

def get_xrdfs_file_info(remote_file):
    """Get information (size, modification time) of a remote xrdfs file/dir"""
    cmd = ["xrdfs", PREFIX, "stat", remote_file]
    output = subprocess.check_output(cmd).decode()
    size = None
    mod_time = None

    for line in output.split('\n'):
        if line.startswith('Size:'):
            size = int(line.split()[1])
        elif line.startswith('MTime:'):
            mod_time = ' '.join(line.split()[1:])
    return size, mod_time

def sync_files(local_dir, remote_dir):
    """Check for discrepancies and update local files from a remote xrdfs directory."""
    remote_files = list_xrdfs_files(remote_dir)
    for remote_file in remote_files:
        local_file = os.path.join(local_dir, os.path.basename(remote_file))
        if not os.path.exists(local_file):
            copy_file = True
        else:
            local_size = os.path.getsize(local_file)
            local_mod_time = datetime.fromtimestamp(os.path.getmtime(local_file))
            remote_size, remote_mod_time_str = get_xrdfs_file_info(remote_file)
            remote_mod_time = datetime.strptime(remote_mod_time_str, '%Y-%m-%d %H:%M:%S')
            copy_file = (local_size != remote_size) or (local_mod_time < remote_mod_time)
        
        if copy_file:
            cmd = ["xrdcp", f"{PREFIX}/{remote_file}", local_file]
            subprocess.run(cmd)
