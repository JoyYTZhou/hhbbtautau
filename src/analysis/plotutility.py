import mplhep
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import subprocess

pjoin = os.path.join
runcom = subprocess.run

class Visualizer():
    def __init__(self, plt_cfg):
        self._pltcfg = plt_cfg
        
    @property
    def pltcfg(self):
        return self._pltcfg

    def combine_cf(self, sourcedir):
        """Combines all cutflow tables in source directory and output them into output directory"""
        pattern = f'{sourcedir}/cutflow_*.csv'
        file_names = glob.glob(pattern)
        
        dfs = [pd.read_csv(file_name, index_col=0, header=0) for file_name in file_names]
        concat_df = pd.concat(dfs)
        combined = concat_df.groupby(concat_df.index, sort=False).sum()
        
        return combined
    


def cpsource(srcdir, destdir):
    """Copy srcdir in condor to local destdir"""
    comstr = f'xrdcp -r {srcdir} {destdir}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    
    if result.returncode==0: print("Transfer object file successful!")
    else: 
        print("Transfer not successful! Here's the error message =========================")
        print(result.stderr)


    
         
        
        