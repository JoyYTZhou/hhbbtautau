import os
import glob
import subprocess
from pathlib import Path
import logging
import shutil

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


