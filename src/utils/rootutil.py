import os
import uproot
import json
import awkward as ak
import random
import subprocess
import pandas as pd

from analysis.selutility import Object
from utils.filesysutil import transferfiles, glob_files, checkpath, delfiles
from utils.datautil import checkevents, find_branches
from utils.cutflowutil import weight_cf, combine_cf

pjoin = os.path.join
runcom = subprocess.run

class DataLoader():
    """Class for loading and hadding data from skims/predefined selections produced directly by Processor."""
    def __init__(self, pltcfg) -> None:
        self.pltcfg = pltcfg
        self.get_wgt()
    
    def __call__(self):
        if self.pltcfg.REFRESH:
            #DataLoader.hadd_roots(self.pltcfg, self.wgt_dict)
            self.hadd_cfs()
        # self.get_objs()
        self.get_totcf()
    
    def get_wgt(self):
        """Compute/Load weights needed for these datasets. Save if needed."""
        from_load = self.pltcfg.FROM_LOAD
        if from_load:
            with open(pjoin(self.pltcfg.DATAPATH, 'wgt_total.json'), 'r') as f:
                self.wgt_dict = json.load(f)        
        else:
            self.wgt_dict = DataLoader.haddWeights(self.pltcfg.DATASETS, self.pltcfg.DATAPATH)
            
    def get_totcf(self, resolution=0):
        """Load all cutflow tables for all datasets from output directory and combine them into one. 
        Scaled by luminosity in self.pltcfg currently.
        
        Parameters
        - `resolution`: resolution of the cutflow table. 0 keep process level. 1 keep dataset level (specific channels etc.)

        Returns
        - Tuple of two dataframes (raw, weighted) of cutflows
        """
        def process_file(path, process, resolution):
            """Read and process a file based on resolution."""
            df = pd.read_csv(path)
            if resolution == 0:
                df = df.sum(axis=1).to_frame(name=process)
            return df
        pltcfg = self.pltcfg
        tot_raw_list = []
        tot_wgt_list = []
        for process in self.wgt_dict.keys():
            wgt_path = glob_files(pjoin(pltcfg.PLOTDATA, process), startpattern=process, endpattern='wgtcf.csv')[0]
            wgt_df = process_file(wgt_path, process, resolution)
            tot_wgt_list.append(wgt_df)

            raw_path = glob_files(pjoin(pltcfg.PLOTDATA, process), startpattern=process, endpattern='rawcf.csv')[0]
            raw_df = process_file(raw_path, process, resolution)
            tot_raw_list.append(raw_df)

        raw_df = pd.concat(tot_raw_list, axis=1)
        wgt_df = pd.concat(tot_wgt_list, axis=1)

        raw_df.to_csv(pjoin(pltcfg.OUTPUTDIR, 'final_raw_data.csv'))
        wgt_df.to_csv(pjoin(pltcfg.OUTPUTDIR, 'final_wgt_data.csv'))

        return raw_df, wgt_df

    def get_objs(self):
        """Writes the selected, concated objects to root files.
        Get from processes in pltcfg only, regardless of the entries in weight dictionary."""
        pltcfg = self.pltcfg
        outdir = pjoin(pltcfg.OUTPUTDIR, 'objlimited')
        checkpath(outdir)
        for process in pltcfg.DATASETS:
            for ds in self.wgt_dict[process].keys():
                datadir = pjoin(pltcfg.PLOTDATA, process)
                files = glob_files(datadir, startpattern=ds, endpattern='.root')
                destination = pjoin(outdir, f"{ds}_limited.root")
                with uproot.recreate(destination) as output:
                    print(f"Writing limited data to file {destination}")
                    DataLoader.write_obj(output, files, pltcfg.PLOT_VARS, pltcfg.EXTRA_VARS)
    
    def hadd_cfs(self):
        """Hadd cutflow tables"""
        pltcfg = self.pltcfg
        for process, dsitems in self.wgt_dict.items():
            rawdflist = []
            wgtdflist = []
            condorpath = pjoin(pltcfg.CONDORPATH, process)
            outpath = pjoin(pltcfg.OUTPUTDIR, process)
            checkpath(outpath)
            indir = pltcfg.INPUTDIR
            for ds in dsitems.keys():
                raw_df = combine_cf(pjoin(indir, process), ds, output=False)
                rawdflist.append(raw_df)
                wgt = self.wgt_dict[process][ds]
                wgtdflist.append(weight_cf(ds, wgt, raw_df, save=False, lumi=self.pltcfg.LUMI))
            pd.concat(rawdflist, axis=1).to_csv(pjoin(outpath, f"{process}_rawcf.csv"))
            pd.concat(wgtdflist, axis=1).to_csv(pjoin(outpath, f"{process}_wgtcf.csv"))
            
            if pltcfg.CONDOR_TRANSFER:
                transferfiles(outpath, condorpath, endpattern='.csv')
                if pltcfg.CLEAN: delfiles(outpath, pattern='*.csv')

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
                    weight = dsval['xsection']/dsval['Raw Events'] if from_raw else dsval['Per Event']
                    dsdict[dskey] = weight
                wgt_dict[ds] = dsdict
        if output: 
            with open(pjoin(grepdir, 'wgt_total.json'), 'w') as f:
                json.dump(wgt_dict, f, indent=4)
        return wgt_dict

    @staticmethod
    def hadd_roots(pltcfg, wgt_dict) -> None:
        """Hadd root files of datasets into appropriate size based on plot setting. 
        
        Parameters
        - `pltcfg`: plot setting
        - `wgt_dict`: dictionary of weights for each process
        """
        batch_size = pltcfg.HADD_BATCH
        indir = pltcfg.INPUTDIR
        for process, dsitems in wgt_dict.items():
            outdir = pjoin(pltcfg.OUTPUTDIR, process)
            checkpath(outdir)
            ds_dir = pjoin(indir, process)
            condorpath = pjoin(pltcfg.CONDORPATH, process)
            for ds in dsitems.keys():
                root_files = glob_files(ds_dir, ds, '.root')
                for i in range(0, len(root_files), batch_size):
                    batch_files = root_files[i:i+batch_size]
                    outname = pjoin(outdir, f"{ds}_{i//batch_size+1}.root") 
                    call_hadd(outname, batch_files)
                if pltcfg.CONDOR_TRANSFER:
                    transferfiles(outdir, condorpath, endpattern='.root')
                    if pltcfg.CLEAN: delfiles(outdir, pattern='*.root')
        return None
    
    @staticmethod
    def write_obj(writable, filelist, objnames, extra=[]) -> None:
        """Writes the selected, concated objects to root files.
        Parameters:
        - `writable`: the uproot.writable directory
        - `filelist`: list of root files to extract info from
        - `objnames`: list of objects to load. Required to be entered in the selection config file.
        - `extra`: list of extra branches to save"""

        all_names = objnames + extra
        print(all_names)
        all_data = {name: [] for name in objnames}
        all_data['extra'] = {name: [] for name in extra}
        for file in filelist:
            evts = load_fields(file)
            print(f"events loaded for file {file}")
            for name in all_names:
                if name in objnames:
                    obj = Object(evts, name)
                    zipped = obj.getzipped()
                    all_data[name].append(zipped)
                else:
                    all_data['extra'][name].append(evts[name])
        for name, arrlist in all_data.items():
            if name != 'extra':
                writable[name] = ak.concatenate(arrlist)
            else:
                writable['extra'] = {branchname: ak.concatenate(arrlist[branchname]) for branchname in arrlist.keys()}

def find_branches(file_path, object_list, tree_name, extra=[]) -> list:
    """Return a list of branches for objects in object_list

    Paremters
    - `file_path`: path to the root file
    - `object_list`: list of objects to find branches for
    - `tree_name`: name of the tree in the root file
    - `extra`: list of extra branches to include

    Returns
    - list of branches
    """
    file = uproot.open(file_path)
    tree = file[tree_name]
    branch_names = tree.keys()
    branches = []
    for object in object_list:
        branches.extend([name for name in branch_names if name.startswith(object)])
    if extra != []:
        branches.extend([name for name in extra if name in branch_names])
    return branches

def load_fields(file, branch_names=None, tree_name='Events', lib='ak'):
    """Load specific fields if any. Otherwise load all. If the file is a list, concatenate the data from all files.
    
    Parameters:
    - file: path to the root file or list of paths
    - branch_names: list of branch names to load
    - tree_name: name of the tree in the root file
    - lib: library to use for loading the data

    Returns:
    - awkward array of the loaded data (, list of empty files)
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
    """Write arrays to root file. Highly inefficient methods in terms of data storage.
    Parameters
    - `evts`: awkward array to write
    - `destination`: path to the output root file
    - `outputtree`: name of the tree to write to
    - `title`: title of the tree
    - `compression`: compression algorithm to use"""
    branch_types = {name: evts[name].type for name in evts.fields}
    with uproot.recreate(destination, compression=compression) as file:
        file.mktree(name=outputtree, branch_types=branch_types, title=title)
        file[outputtree].extend({name: evts[name] for name in evts.fields}) 

def call_hadd(output_file, input_files):
    """Merge ROOT files using hadd.
    Parameters
    - `output_file`: path to the output file
    - `input_files`: list of paths to the input files"""
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
