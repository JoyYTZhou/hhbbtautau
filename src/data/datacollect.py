#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, os, subprocess
from tqdm import tqdm
import uproot
from itertools import chain
import pandas as pd
import glob

def dasgo_query(query, json=False):
    """Query dasgoclient and return the result as a list of strings."""
    cmd = ["dasgoclient", "--query", query]
    if json:
        cmd.append("-json")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Could not run DAS Go client query: {query}. Stderr: \n{stderr}")

    return stdout.decode().splitlines()

def xrootd_format(fpath, prefix):
    """Ensure that the file path is file:/* or xrootd"""
    file_prefix = "root://cmsxrootd.fnal.gov/" if prefix == 'local' else "root://cms-xrd-global.cern.ch/"
    if fpath.startswith("/store/"):
        return f"{file_prefix}{fpath}"
    elif fpath.startswith("file:") or fpath.startswith("root:"):
        return fpath
    else:
        return f"file://{fpath}"

def query_MCsamples(dspath, outputfn, regex=None):
    """ Query xrootd to find all filepaths to a given set of dataset names.
    the result is saved to a new file.

    Parameters
    - `dspath`: path to json file containing dataset names
    - `outputfn`: path to output json file containing full dataset paths
    - `regex`: optional, a string to filter the dataset names
    """
    with open(dspath, 'r') as ds:
        dsjson = json.load(ds)

    query_fistr = lambda ds: "".join(["file dataset=", ds])
     
    for name, dataset_dict in tqdm(dsjson.items(), f"finding samples ..."):
        keys_to_del = []
        for ds, ds_dict in dataset_dict.items():
            if regex is None:
                filelist = list(chain.from_iterable(dasgo_query(query_fistr(s)) for s in ds_dict["string"]))
            else:
                filelist = list(chain.from_iterable(dasgo_query(query_fistr(s)) for s in ds_dict["string"] if regex in s))
            if filelist:
                ds_dict["filelist"] = filelist
            else:
                keys_to_del.append(ds)
        for key in keys_to_del: del dataset_dict[key]

    with open(outputfn, 'w') as jsonfile:
        json.dump(dsjson, jsonfile, indent=4)

def add_weight(dspath, outputdir, dsname=None):
    """Add the number of raw events, weighted events, and per event weight to the dataset dictionary.
    The result is saved to a new file.
    
    Parameters
    - `dspath`: path to json file containing dataset names
    - `outputdir`: path to output directory
    - `dsname`: optional, a string to specify the dataset name"""

    with open(dspath, 'r') as ds:
        dsjson = json.load(ds)
    os.makedirs(outputdir, exist_ok=True)

    if dsname is None:
        searchitems = dsjson
    elif isinstance(dsjson, str):
        searchitems = {dsname: dsjson[str]}
    else: 
        raise ValueError("Needs to pass a dataset name!")

    for name, dataset_dict in tqdm(searchitems.items(), f"finding samples ..."):
        if dataset_dict != {}:
            for ds, ds_dict in dataset_dict.items():
                print(f"locating {ds}")
                raw_tot, wgt_tot, success_list, failed_list = weight_fl(ds_dict['filelist']) 
                ds_dict['filelist'] = success_list
                ds_dict['failedlist'] = failed_list
                ds_dict["Raw Events"] = raw_tot
                ds_dict["Wgt Events"] = wgt_tot
                ds_dict["Per Event"] = ds_dict['xsection']/wgt_tot
            fipath = os.path.join(outputdir, f'{name}.json')
            with open(fipath, 'w') as jsonfile:
                json.dump(dataset_dict, jsonfile, indent=4)
    
    return None

def weight_fl(filelist):
    """Find the total number of raw events, weighted events in a list of files.
    
    Parameters
    - `filelist`: list of file paths
    Returns
    - `raw_tot`: total number of raw events
    - `wgt_tot`: total number of weighted events
    - `success_list`: list of successful file paths
    - `failed_list`: list of failed file paths (unable to open/query)
    """
    wgt_tot = 0
    raw_tot = 0
    success_list = []
    failed_list = []
    for file in tqdm(filelist):
        xrd_file = xrootd_format(file, 'local')
        result = info_file(xrd_file)
        if isinstance(result, str): 
            failed_list.append(xrd_file)
            continue
        n_raw, n_wgt = result
        wgt_tot += n_wgt
        raw_tot += n_raw
        success_list.append(xrd_file)
    
    return raw_tot, wgt_tot, success_list, failed_list
    

def info_file(file):
    """Return the number of raw events and weighted events of a file in a json dictionary
    with error handled."""
    nevents_wgt = 0
    nevents_raw = 0
    try: 
        with uproot.open(file) as f:
            t = f.get("Runs")
            nevents_wgt = t["genEventSumw"].array(library="np").sum()
            nevents_raw = f.get("Events").num_entries
        return nevents_raw, nevents_wgt
    except Exception as e:
        message = f"Failed to find {file}: {e}"
        return message

def preprocess_files(inputfn, step_size=10000, tree_name="Events", process_name = "DYJets"):
    """Preprocess the files in the dataset dictionary by chunking the files into smaller steps.
    The result is saved to a new file.
    
    Parameters
    - `inputfn`: path to json file containing dataset names
    - `step_size`: size of the chunk
    - `tree_name`: name of the tree
    - `process_name`: name of the process (supposedly a key in the json file)
    """
    def chunkfile_dict(file_path, tree_name, step_size):
        with uproot.open(file_path) as file:
            print("=============", file_path, "=============")
            tree = file[tree_name]
            n_events = tree.num_entries
            steps = [[i, min(i + step_size, n_events)] for i in range(0, n_events, step_size)]
            result_dict = {
                file_path: {
                    "object_path": tree_name,
                    "steps": steps
                }
            }
        return result_dict
    with open(inputfn, 'r') as ds:
        dsjson = json.load(ds)

    input_dict = {}
    failed_dict = {}
    ds = process_name
    pathlist = dsjson[process_name]
    result = {}
    for path in tqdm(pathlist, desc=f"Finding sample {ds}"):
        try:
            result.update(chunkfile_dict(path, tree_name, step_size))
        except Exception as e:
            print(f"Failed to find {path}: {e}")
            failed_dict.update({ds: path})
    if result != {}: input_dict.update({ds: result})

    outputfn = f"chunked/{process_name}.json"
    with open(outputfn, 'w') as jsonfile:
        json.dump(input_dict, jsonfile)

    errorfn = f"chunked/{process_name}_failed.json"
    if failed_dict != {}:
        with open(errorfn, 'w') as errorfile:
            json.dump(failed_dict, errorfile)

    return None

def divide_samples(inputfn, outputfn, dict_size=5):
    """Divide the ds into smaller list as value per key.

    :param inputfn: path to json file containing dataset names. With structure:
        {processname: [list of filenames]}
    :type inputfn: string
    :param outputfn: path to output json file containing full dataset paths
    :type outputfn: string
        {
            processname1: [list of 5 filenames]
            processname2: [list of 5 filenames]
            processname3: [list of 5 filenames]
            ...
        }
    :param dict_size: size of the list as keys
    :type dict_size: int
    """
    with open(inputfn, 'r') as jsonfile:
        dsjson = json.load(jsonfile)

    complete_dict = {}

    for process, processlist in dsjson.items():
        complete_dict.update({process: {}})
        for dsname, itemlist in processlist.items():
            n_dicts = len(itemlist) // dict_size + (len(itemlist) % dict_size > 0)

        # Create the smaller dictionaries
            smaller_dicts = {f"{dsname}_{i+1}": itemlist[i*dict_size:(i+1)*dict_size] for i in range(n_dicts)}
            complete_dict[process].update(smaller_dicts)

    with open(outputfn, 'w') as jsonfile:
        json.dump(complete_dict, jsonfile)

def produceCSV(datadir):
    json_files = glob.glob(f"{datadir}/*.json")
    df = []
    for file_path in json_files:
        with open(file_path, 'r') as file:
            data = json.load(file)
            for name, attributes in data.items():
                df.append({
                    'name': name,
                    'string': attributes['string'][0] if attributes['string'] else None,
                    'xsection': attributes.get('xsection'),
                    '# raw events': attributes.get('Raw Events'),
                    '# weighted events': attributes.get('Wgt Events')
                })
    all_df = pd.DataFrame(df)
    all_df.to_csv('compiled_weight.csv', index=False)
    
if __name__ == "__main__":
    # query_MCsamples("data.json", "data_file.json", regex="NanoAODv")
    add_weight("data_file.json", "preprocessed")
    # print("Jobs finished!")
    # produceCSV('preprocessed')



