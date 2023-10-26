#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import subprocess
import json
import os

def dasgo_query(query, json=False):
    cmd = ["dasgoclient", "--query", query]
    if json:
        cmd.append("-json")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Could not run DAS Go client query: {query}. Stderr: \n{stderr}")

    return stdout.decode().splitlines()

if __name__ == "__main__":
    with open("MCSamplepath.json", 'r') as ds:
        dsjson = json.load(ds)

    complete_dict = {}


    print(dasgo_query("dataset=/GluGlutoHHto2B2Tau_kl-*_kt-1p00_*_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22EEMiniAODv3-Poisson60KeepRAW_124X_mcRun3_2022_realistic_postEE_v1-v2/MINIAODSIM").decode().splitlines())
    

