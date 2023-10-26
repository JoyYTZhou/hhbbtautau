#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import subprocess
import json
import os
from tqdm import tqdm

def dasgo_query(query, json=False):
    cmd = ["dasgoclient", "--query", query]
    if json:
        cmd.append("-json")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Could not run DAS Go client query: {query}. Stderr: \n{stderr}")

    return stdout.decode().splitlines()

def xrootd_format(fpath):
    """Ensure that the file path is file:/* or xrootd"""
    if fpath.startswith("/store/"):
        return f"root://cms-xrd-global.cern.ch//{fpath}"
    elif fpath.startswith("file:") or fpath.startswith("root:"):
        return fpath
    else:
        return f"file://{fpath}"

if __name__ == "__main__":
    with open("MCsamplepath.json", 'r') as ds:
        dsjson = json.load(ds)

    complete_dict = {}
    
    for process in dsjson.keys():
        complete_dict[process] = {}
        for name, sample_list in tqdm(dsjson[process].items(), f"finding {process} samples..."):
            # make dataset query for a list of dataset in one process
            query_ds = lambda ds: "".join(["dataset=", ds])
            ds_query_list = list(map(query_ds, sample_list))
            to_flatten = list(map(dasgo_query, ds_query_list))
            dslist = [item for sublist in to_flatten for item in sublist] 

            query_file = lambda ds: "".join(["file dataset=", ds])
            file_query_list = list(map(query_file, dslist))
            to_flatten = list(map(dasgo_query, file_query_list))
            filelist = [item for sublist in to_flatten for item in sublist] 
            filelist_xrootd = list(map(xrootd_format, filelist))
            complete_dict[process].update({name: filelist_xrootd})

    with open("completepath.json", 'w') as jsonfile:
        json.dump(complete_dict, jsonfile)


        
