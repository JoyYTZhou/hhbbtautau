import os
import glob
import subprocess
from pathlib import Path
import logging

runcom = subprocess.run
pjoin = os.path.join
PREFIX = "root://cmseos.fnal.gov"

def cpcondor(srcpath, destpath, is_file=True):
    """Copy srcpath (file/directory) FROM local to condor destpath"""

    comstr = f'xrdcp -f {srcpath} {destpath}' if is_file else f'xrdcp -r {srcpath} {destpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    if result.returncode == 0: logging.debug("Transfer objects successful!")
    else: 
        logging.info(f"Transfer from {srcpath} to {destpath} not successful! Here's the error message =========================")
        logging.info(result.stderr)
    return result

def transferfiles(srcpath, destpath):
    """Copy all files FROM one local directory to a condor directory. 
    If destpath does not exist, will create one."""
    checkcondorpath(destpath)
    for srcfile in srcpath.iterdir():
        if srcfile.is_file():
            cpcondor(str(srcfile), str(destpath/srcfile.name), is_file=True)

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


