import os
import glob
import subprocess
from pathlib import Path

runcom = subprocess.run
pjoin = os.path.join

def cpcondor(srcpath, destpath, is_file=True):
    """Copy srcpath (file/directory) FROM local to condor destpath"""

    comstr = f'xrdcp -f {srcpath} {destpath}' if is_file else f'xrdcp -r {srcpath} {destpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    if result.returncode==0: print("Transfer objects successful!")
    else: 
        print("Transfer not successful! Here's the error message =========================")
        print(result.stderr)
    
    return result

def transferfiles(srcpath, destpath):
    """Copy all files FROM one local directory to an existing condor directory"""
    for srcfile in srcpath.iterdir():
        if srcfile.is_file():
            cpcondor(str(srcfile), str(destpath/srcfile.name), is_file=True)


