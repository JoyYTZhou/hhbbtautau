import os
import glob
import subprocess

runcom = subprocess.run
pjoin = os.path.join

def cpcondor(srcpath, destpath, is_file=True):
    """Copy srcpath (file/directory) FROM condor to local destpath"""

    comstr = f'xrdcp -f {srcpath} {destpath}' if is_file else f'xrdcp -r {srcpath} {destpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    if result.returncode==0: print("Transfer objects successful!")
    else: 
        print("Transfer not successful! Here's the error message =========================")
        print(result.stderr)
    
    return result


