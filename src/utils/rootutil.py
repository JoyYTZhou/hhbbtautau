import os
import pandas as pd
import numpy as np
import uproot
import json
import pickle
import awkward as ak
import random
import subprocess
from analysis.selutility import Object
from utils.filesysutil import transferfiles, glob_files, checkpath, delfiles
from utils.datautil import checkevents, find_branches

pjoin = os.path.join
runcom = subprocess.run

class DataLoader():
    def __init__(self, pltcfg) -> None:
        self.pltcfg = pltcfg
        self.wgt_dict = DataLoader.haddWeights(self.pltcfg.DATASETS, self.pltcfg.DATAPATH)
    
    def __call__(self, source, **kwargs):
        """Constructor for DataLoader class.
        
        Parameters
        - `source`: source of the data. Can be a string (path to file), a dataframe, or an awkward array.
        """
        if isinstance(source, str):
            if source.endswith('.pkl'):
                data = load_pkl(source)
            elif source.endswith('.csv'):
                data = pd.read_csv(source, **kwargs)
            elif source.endswith('.root'):
                data = uproot.open(source)
            elif source.endswith('.parquet'):
                data = pd.read_parquet(source, **kwargs)
            else:
                raise ValueError("This is not a valid file type.")
        elif checkevents(source):
            data = source
        else:
            data = source
            raise UserWarning(f"This might not be a valid source. The data type is {type(source)}")
        return data

    def get_objs(self):
        pltcfg = self.pltcfg
        for process in pltcfg.DATASETS:
            for ds in self.wgt_dict[process].keys():
                datadir = pjoin(pltcfg.PLOTDATA, process)
                files = glob_files(datadir, startpattern=ds, endpattern='.root')
                destination = pjoin(pltcfg.OUTPUTDIR, f"{ds}_limited.root")
                with uproot.recreate(destination) as output:
                    DataLoader.write_obj(output, files, pltcfg.PLOT_VARS, pltcfg.EXTRA_VARS)

    def getbranches(self, file):
        pltcfg = self.pltcfg
        branchnames = find_branches(file, pltcfg.PLOT_VARS, tree_name=pltcfg.TREENAME, 
                                    extra=pltcfg.EXTRA_VARS)
        return branchnames

    @staticmethod
    def haddWeights(regexlist, grepdir, output=True, from_raw=True):
        """Function for self use only, grep weights from a list of json files formatted in a specific way.
        
        Parameters
        - `regexlist`: list of strings of dataset names
        - `grepdir`: directory where the json files are located
        - `output`: whether to save the weights into a json file
        - `from_raw`: whether to compute weights based on number of raw events instead of weighted
        """
        wgt_dict = {}
        for ds in regexlist:
            with open(pjoin(grepdir, f'{ds}.json'), 'r') as f:
                meta = json.load(f)
                dsdict = {}
                for dskey, dsval in meta.items():
                    if from_raw:
                        dsdict.update({dskey: dsval['xsection']/dsval['Raw Events']})
                    else:
                        dsdict.update({dskey: dsval['Per Event']})
                wgt_dict.update({ds: dsdict})
        if output: 
            outname = pjoin(grepdir, 'wgt_total.json')
            with open(outname, 'w') as f:
                json.dump(wgt_dict, f, indent=4)
        return wgt_dict

    @staticmethod
    def hadd_roots(pltcfg, wgt_dict) -> None:
        """Combine all root files of datasets in plot setting into one ro
        
        Parameters
        """
        batch_size = pltcfg.HADD_BATCH
        for process, dsitems in wgt_dict.items():
            outdir = pjoin(pltcfg.OUTPUTDIR, process)
            checkpath(outdir)
            indir = pltcfg.INPUTDIR
            ds_dir = pjoin(indir, process)
            condorpath = pjoin(pltcfg.CONDORPATH, process)
            for ds in dsitems.keys():
                root_files = glob_files(ds_dir, ds, '.root')
                for i in range(0, len(root_files), batch_size):
                    batch_files = root_files[i:i+batch_size]
                    outname = pjoin(outdir, f"{ds}_{i//batch_size+1}.root") 
                    call_hadd(outname, batch_files)
                if pltcfg.CONDOR_TRANSFER:
                    checkpath(condorpath)
                    transferfiles(outdir, condorpath)
                    if pltcfg.CLEAN: delfiles(outdir, pattern='*.root')
        return None
    
    @staticmethod
    def write_obj(writable, filelist, objnames, extra=None):
        objdict = {obj: [] for obj in objnames}
        if extra is not None:
            extradict = {name: [] for name in extra}
        for file in filelist:
            evts = load_fields(file)
            for name in objnames:
                obj = Object(evts, name)
                zipped = obj.getzipped()
                objdict[name].append(zipped)
            if extra is not None:
                for name in extra: extradict[name].append(evts[name])
        for name, arrlist in objdict.items(): writable[name] = ak.concatenate(arrlist)
        if extradict: 
            for name, arrlist in extradict.items(): writable[name] = ak.concatenate(arrlist)
        return None

def get_compression(**kwargs):
    compression = kwargs.pop('compression', None)
    compression_level = kwargs.pop('compression_level', 1)

    if compression in ("LZMA", "lzma"):
        compression_code = uproot.const.kLZMA
    elif compression in ("ZLIB", "zlib"):
        compression_code = uproot.const.kZLIB
    elif compression in ("LZ4", "lz4"):
        compression_code = uproot.const.kLZ4
    elif compression in ("ZSTD", "zstd"):
        compression_code = uproot.const.kZSTD
    elif compression is None:
        raise UserWarning("Not sure if this option is supported, should be...")
    else:
        msg = f"unrecognized compression algorithm: {compression}. Only ZLIB, LZMA, LZ4, and ZSTD are accepted."
        raise ValueError(msg)
    
    if compression is not None: 
        compression = uproot.compression.Compression.from_code_pair(compression_code, compression_level)

    return compression

def load_fields(file, branch_names=None, tree_name='Events', lib='ak'):
    """Load specific fields from root files in filelist and combine them into a single Arr.
    
    Parameters:
    - file: file
    - branch_names: list of branch names to load
    - tree_name: name of the tree in the root file

    Returns:
    - A data arr containing the combined data from all root files in filelist.
    - A list of empty files
    """
    def load_one(fi):
        with uproot.open(fi) as file:
            if file.keys() == []:
                return False
            else:
                tree = file[tree_name] 
        return tree.arrays(branch_names, library=lib)

    returned = None
    if isinstance(file, str):
        returned = load_one(file)
    elif isinstance(file, list):
        dfs = []
        emptylist = []
        for root_file in file:
            if load_one(file):
                dfs.append(load_one(file))
            else: emptylist.append(root_file)
        combined_evts = ak.concatenate(dfs)
        returned = (combined_evts, emptylist)
    return returned

def write_root(evts, destination, outputtree="Events", title="Events", compression=None):
    """Write arrays to root file. Highly inefficient methods in terms of data storage."""
    branch_types = {name: evts[name].type for name in evts.fields}
    with uproot.recreate(destination, compression=compression) as file:
        file.mktree(name=outputtree, branch_types=branch_types, title=title)
        file[outputtree].extend({name: evts[name] for name in evts.fields}) 

def call_hadd(output_file, input_files):
    command = ['hadd', '-f0 -O', output_file] + input_files
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Merged files into {output_file}")
    else:
        print(f"Error merging files: {result.stderr}")    

def concat_roots(directory, startpattern, outdir, fields=None, extra_branches = [], **kwargs):
    """
    Load specific branches from ROOT files matching a pattern in a directory, and combine them into a single DataFrame.

    Parameters:
    - directory: Path to the directory containing ROOT files.
    - startpattern: Pattern to match the start of the ROOT file name.
    - fields: List of field names to load from each ROOT file.
    - outdir: Path to the directory to save the combined DataFrame.
    - extra_branches: List of extra branches to load from each ROOT file.
    - tree_name: Name of the tree to load

    Returns:
    - A list of empty files among the searched ROOT files
    """
    checkpath(outdir)
    tree_name=kwargs.pop('tree_name', "Events")
    root_files = glob_files(directory, startpattern, endpattern=kwargs.pop('endpattern', '.root'))
    random.shuffle(root_files)
    emptyfiles = []
    if fields is not None:
        branch_names = find_branches(root_files[0], fields, tree_name=tree_name, extra=extra_branches)
    else:
        branch_names = None
    combined_evts, empty_list = load_fields(root_files, branch_names, tree_name=tree_name)
    emptyfiles.extend(empty_list)
    outfilepath = pjoin(outdir, f'{startpattern}.root')
    write_root(combined_evts, outfilepath, **kwargs)
    return emptyfiles

def load_pkl(filename):
    """Load a pickle file and return the data."""
    with open(filename, 'rb') as f:
        data = pickle.load(f)
    return data
