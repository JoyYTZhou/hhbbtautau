import os
import glob
import subprocess
from pathlib import Path
import logging
import shutil
import pandas as pd

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
    if pattern is not None:
        dirpath = Path(dirname)
        for fipath in dirpath.glob(pattern):
            fipath.unlink()
            logging.info(f"Deleted {fipath}")

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

def initLogger(self, name, suffix):
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
    file_names = glob.glob(pattern)
    dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names] 
    return dfs

def hadd_csvs(pattern):
    dfs = load_csvs(pattern)
    return pd.concat(dfs, axis=1)

# below needs testing
def list_xrdfs_files(remote_dir):
    """List files in a remote xrdfs directory."""
    cmd = ["xrdfs", PREFIX, "ls", remote_dir]
    output = subprocess.check_output(cmd).decode()
    files = output.strip().split('\n')
    return files

def get_xrdfs_file_info(remote_file):
    """Get information (size, modification time) of a remote xrdfs file."""
    cmd = ["xrdfs", "root://server.example.com", "stat", remote_file]
    output = subprocess.check_output(cmd).decode()
    # Parse output to extract size and modification time
    # This will need to be adjusted based on the actual output format of `xrdfs stat`
    size = ...
    mod_time = ...
    return size, mod_time

def sync_files(local_dir, remote_dir):
    """Check for discrepancies and update local files from a remote xrdfs directory."""
    remote_files = list_xrdfs_files(remote_dir)
    for remote_file in remote_files:
        local_file = os.path.join(local_dir, os.path.basename(remote_file))
        # If the file doesn't exist locally, or sizes/modification times differ, copy it
        if not os.path.exists(local_file):
            copy_file = True
        else:
            local_size = os.path.getsize(local_file)
            local_mod_time = os.path.getmtime(local_file)
            remote_size, remote_mod_time = get_xrdfs_file_info(remote_file)
            copy_file = (local_size != remote_size) or (local_mod_time != remote_mod_time)
        
        if copy_file:
            cmd = ["xrdcp", f"root://server.example.com/{remote_file}", local_file]
            subprocess.run(cmd)

# Example usage
# local_dir = "/path/to/local/dir"
# remote_dir = "/path/to/remote/dir"
# sync_files(local_dir, remote_dir)
