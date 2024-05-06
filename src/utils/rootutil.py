import os
import uproot
import json
import awkward as ak
import random
import subprocess
import pandas as pd

from analysis.selutility import Object
from config.selectionconfig import cleansetting as cleancfg
from utils.filesysutil import transferfiles, glob_files, checkpath, delfiles, get_xrdfs_file_info
from utils.datautil import find_branches, pjoin, getmeta
from utils.cutflowutil import combine_cf, efficiency, incrementaleff
from functools import wraps

PREFIX = "root://cmseos.fnal.gov"

indir = cleancfg.INPUTDIR
localout = cleancfg.LOCALOUTPUT
lumi = cleancfg.LUMI
resolve = cleancfg.get("RESOLVE", False)

checkpath(indir, createdir=False, raiseError=True)

def iterprocess(func):
    """Decorator function that iterates over all processes in the cleancfg.DATASETS."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for process in cleancfg.DATASETS:
            print(f"Processing {process} files ..................................................")
            with open(pjoin(cleancfg.DATAPATH, f"{process}.json"), 'r') as jsonfile:
                meta = json.load(jsonfile)
            func(process, meta, *args, **kwargs)
        return func
    return wrapper

class DataLoader():
    """Class for loading and hadding data from skims/predefined selections produced directly by Processor."""
    def __init__(self) -> None:
        pass
    
    @staticmethod
    @iterprocess 
    def hadd_roots(process, meta) -> None:
        """Hadd root files of datasets into appropriate size based on settings.
        
        Parameters
        - `cleancfg`: plot setting
        - `wgt_dict`: dictionary of weights for each process
        """
        outdir = pjoin(localout, process)
        checkpath(outdir, createdir=True)
        ds_dir = pjoin(indir, process)
        for ds in meta.keys():
            condorpath = cleancfg.CONDORPATH if cleancfg.get("CONDORPATH", False) else pjoin(f'{indir}_hadded', process)
            root_files = glob_files(ds_dir, ds, '.root', add_prefix=False)
            size = get_xrdfs_file_info(root_files[0])[0]
            batch_size = int(10**9/size)
            print(f"Merging in batches of {batch_size} individual root files!")
            root_files = [PREFIX + "/" + f for f in root_files]
            for i in range(0, len(root_files), batch_size):
                batch_files = root_files[i:i+batch_size]
                outname = pjoin(outdir, f"{ds}_{i//batch_size+1}.root") 
                call_hadd(outname, batch_files)
        transferfiles(outdir, condorpath, endpattern='.root')
        if cleancfg.get("CLEANROOT", True): delfiles(outdir, pattern='*.root')

    @staticmethod
    @iterprocess
    def hadd_cfs(process, meta) -> None:
        """Hadd cutflow table output from processor, saved to LOCALOUTPUT. 
        Transfer to prenamed condorpath if needed.
        
        Parameters
        - `process`: Process
        - `meta`: metadata for the process"""
        dflist = []
        condorpath = cleancfg.CONDORPATH if cleancfg.get("CONDORPATH", False) else pjoin(f'{indir}_hadded', process)
        outpath = pjoin(localout, process)
        checkpath(outpath)
        for ds in meta.keys():
            print(pjoin(indir, process))
            print(f"Dealing with {ds} now ...............................")
            df = combine_cf(inputdir=pjoin(indir, process), dsname=ds, output=False)
            dflist.append(df)
        pd.concat(dflist, axis=1).to_csv(pjoin(outpath, f"{process}_cf.csv"))
        transferfiles(outpath, condorpath, endpattern='.csv')
        if cleancfg.get("CLEANCSV", False): delfiles(outpath, pattern='*.csv')
        return None
    
    @staticmethod
    def merge_cf(signals=['ggF', 'ZH', 'ZZ']) -> None:
        """Merge all cutflow tables for all processes into one. Save to LOCALOUTPUT.
        Output formatted cutflow table as well.
        
        Parameters
        - `signals`: list of signal process names"""
        list_df = []
        wgt_dfdict= {}
        for process in cleancfg.DATASETS:
            condorpath = pjoin(cleancfg.CONDORPATH, process) if cleancfg.get("CONDORPATH", False) else pjoin(f'{indir}_hadded', process)
            cf, wgt_df = DataLoader.load_cf(process, condorpath)
            list_df.append(cf)
            wgt_dfdict[process] = wgt_df
        total_df = pd.concat(list_df, axis=1)
        wgt_total_df = total_df.filter(like='wgt')
        efficiency(localout, wgt_total_df, overall=False, save=True, save_name=f'stepwise') 
        efficiency(localout, wgt_total_df, overall=False, save=True, save_name=f'tot') 
        yield_df = pd.DataFrame(wgt_dfdict, index=total_df.index)
        yield_df = DataLoader.process_yield(yield_df, signals)
        total_df.to_csv(pjoin(localout, 'allcf.csv'))
        yield_df.to_csv(pjoin(localout, 'yieldcf.csv'))
        yield_df = DataLoader.scale_yield(yield_df)
        yield_df.to_csv(pjoin(localout, 'scaledyield.csv'))
        return None
    
    @staticmethod
    def load_cf(process, datasrcpath) -> tuple:
        """Load cutflow tables for a process.
        Parameters
        -`process`: the name of the cutflow that will be grepped from datasrcpath
        -`datasrcpath`: path to the directory containing cutflow tables.
        
        Returns
        - cutflow dataframe, weight dataframe
        """
        cf = pd.read_csv(glob_files(datasrcpath, startpattern=process, endpattern='cf.csv')[0], index_col=0)
        meta = getmeta(process)
        for ds in meta.keys():
            scale_wgt = meta[ds]['Per Event']
            sel_cols = cf.filter(like=ds).filter(like='wgt')
            cf[sel_cols.columns] = sel_cols*scale_wgt
        if not resolve:
            DataLoader.add_cfcol_by_kwd(cf, 'raw', f"{process}_raw")
            wgt_df = DataLoader.add_cfcol_by_kwd(cf, 'wgt', f"{process}_wgt")
        return cf, wgt_df
    
    @staticmethod
    def process_yield(yield_df, signals) -> pd.DataFrame:
        """Process the yield dataframe to include signal and background efficiencies.
        Parameters
        - `yield_df`: dataframe of yields
        - `signals`: list of signal process names"""
        sig_list = [signal for signal in signals if signal in yield_df.columns]
        bkg_list = yield_df.columns.difference(sig_list)
        yield_df['Tot Sig'] = yield_df[sig_list].sum(axis=1)
        yield_df['Sig Eff'] = incrementaleff(yield_df, "Tot Sig")
        yield_df['Tot Bkg'] = yield_df[bkg_list].sum(axis=1)
        yield_df['Bkg Eff'] = incrementaleff(yield_df, 'Tot Bkg')
        new_order = list(bkg_list) + ['Tot Bkg', 'Bkg Eff'] + list(sig_list) + ['Tot Sig', 'Sig Eff']
        yield_df = yield_df[new_order]
        return yield_df

    @staticmethod
    def scale_yield(yield_df):
        selcols = yield_df.columns.difference(yield_df.filter(like='Eff').columns)
        yield_df[selcols] = yield_df[selcols] * lumi * 1000
        return yield_df 

    @staticmethod
    def add_cfcol_by_kwd(cfdf, keyword, name) -> pd.Series:
        """Add a column to the cutflow table by summing up all columns with the keyword.

        Parameters
        - `cfdf`: cutflow dataframe
        - `keyword`: keyword to search for in the column names
        - `name`: name of the new column

        Return
        - Series of the summed column"""
        same_cols = cfdf.filter(like=keyword)
        sumcol = same_cols.sum(axis=1)
        cfdf = cfdf.drop(columns=same_cols)
        cfdf[name] = sumcol
        return sumcol

    def get_objs(self):
        """Writes the selected, concated objects to root files.
        Get from processes in cleancfg only, regardless of the entries in weight dictionary.
        Results saved to LOCALOUTPUT/objlimited
        """
        outdir = pjoin(localout, 'objlimited')
        checkpath(outdir)
        for process in cleancfg.DATASETS:
            for ds in self.wgt_dict[process].keys():
                datadir = pjoin(cleancfg.INPUTDIR, process)
                files = glob_files(datadir, startpattern=ds, endpattern='.root')
                destination = pjoin(outdir, f"{ds}_limited.root")
                with uproot.recreate(destination) as output:
                    print(f"Writing limited data to file {destination}")
                    DataLoader.write_obj(output, files, cleancfg.PLOT_VARS, cleancfg.EXTRA_VARS)

    @staticmethod
    def haddWeights(grepdir):
        """Function for self use only, grep weights from a list of json files formatted in a specific way.
        
        Parameters
        - `grepdir`: directory where the json files are located
        """
        wgt_dict = {}
        jsonfiles = glob_files(grepdir)
        for filename in jsonfiles:
            ds = os.path.basename(filename).rsplit('.json', 1)[0]
            with open(filename, 'r') as f:
                meta = json.load(f)
                dsdict = {}
                for dskey, dsval in meta.items():
                    weight = dsval['Per Event']
                    dsdict[dskey] = weight
                wgt_dict[ds] = dsdict
        return wgt_dict

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
    
    @staticmethod
    def process_file(path, process, resolution):
        """Read and process a file based on resolution."""
        df = pd.read_csv(path, index_col=0)
        if resolution == 0:
            df = df.sum(axis=1).to_frame(name=process)
        return df

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
