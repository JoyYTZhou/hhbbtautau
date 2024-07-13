from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import traceback, os, random

from .custom import switch_selections
from .processor import Processor, getTransfer
from utils.filesysutil import glob_files, get_xrdfs_file_info, check_missing, checkpath, pjoin
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

evtselclass = switch_selections(rs.SEL_NAME)

parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

datapath = pjoin(parent_directory, 'data', 'data.json')
with open(datapath, 'r') as data:
    realmeta = json.load(data)

transferP = getTransfer(rs)

def job(fn, i, dataset, transferP=transferP, eventSelection=evtselclass) -> int:
    """Run the processor for a single file.
    Parameters
    - `fn`: The name of the file to process
    - `i`: The index of the file in the list of files
    - `dataset`: The name of the dataset (same dataset has same xsection)
    - `eventSelection`: Custom Defined Event Selection Class
    """
    proc = Processor(rs, dataset, transferP, eventSelection)
    print(f"Processing filename {fn}")
    try: 
        rc = proc.runfile(fn, i)
        if rc !=0: 
            print(f"File failed for file index {i} in {dataset}.")
        else:
            print(f"Execution finished for file index {i} in {dataset}!")
        return rc
    except TypeError as e:
        print(f"TypeError encountered for file index {i} in {dataset}: {e}")
        return 1
    
def loadmeta(filterfunc, dsindx=None, inputpath=rs.INPUTFILE_PATH, tsferP=transferP) -> dict:
    """Load metadata from input file, or straight from directories containing files to process.
    
    Parameters
    - `filterfunc`: function to filter files to run among meta files
    - `dsindx`: Index of datasets to process in json file. If None, process all datasets.
    
    Return
    """
    if inputpath.endswith('.json'):
        inputdatap = pjoin(parent_directory, inputpath)
        with open(inputdatap, 'r') as samplepath:
            loaded = json.load(samplepath)
        if dsindx is not None:
            dskey = list(loaded.keys())[dsindx]
            loaded = {dskey: loaded[dskey]}
    elif inputpath.startswith('/store/user/'):
        loaded = realmeta[rs.PROCESS_NAME]
        for dataset in loaded.keys():
            loaded[dataset]['filelist'] = glob_files(inputpath, startpattern=dataset, endpattern='.root')
    else:
        raise TypeError("Check INPUTFILE_PATH in runsetting.toml. It's not of a valid format!")
    if filterfunc is not None: loaded = filterfunc(loaded, tsferP)
    return loaded

def filterResume(metadata, tsferP=transferP) -> dict:
    """Resume jobs from last checkpoint"""
    if tsferP:
        statcode = checkpath(tsferP, createdir=False)
        if statcode != 0: 
            loaded = metadata
        else:
            loaded = {}
            datasets = metadata.keys()
            for ds in datasets:
                print(f"Checking {ds} ========================================================")
                fileno = len(metadata[ds]['filelist'])
                fileindx1 = check_missing(f'{ds}_cutflow', fileno, tsferP, endpattern='.csv')
                if fileindx1: print(f"Missing cutflow tables for these files: {fileindx1}!")
                fileindx1 = set(fileindx1)
                fileindx2 = check_missing(f'{ds}', fileno, tsferP, endpattern='.root')
                if fileindx2: print(f"Missing output for these files: {fileindx2} ")
                fileindx2 = set(fileindx2)
                fileindx = list(fileindx1.union(fileindx2))
                if fileindx != []:
                    loaded[ds] = {}
                    loaded[ds]['resumeindx'] = fileindx
                    loaded[ds]['filelist'] = metadata[ds]['filelist']
    else:
        loaded = metadata
    return loaded

def checkjobs(tsferP=transferP) -> None:
    """Check if there are files left to be run."""
    loaded = loadmeta(filterResume)
    print(f"Checking {tsferP} for output files!")
    if loaded:
        for ds in loaded.keys():
            if 'resumeindx' in loaded[ds]:
                filelen = len(loaded[ds]['resumeindx'])
            else:
                filelen = len(loaded[ds]['filelist'])
            print(f"There are {filelen} files left to be run in {ds}.")
    else:
        print("All the files have been processed!")

def submitfutures(client, ds, filelist, indx) -> list:
    """Submit jobs as futures to client.
    
    Parameters
    - `client`: Dask client
    - `ds`: dataset name
    - `filelist`: List of files to process
    - `indx`: List of indices to process

    Returns
    list: List of futures for each file in the dataset.
    """
    futures = []
    if indx is None:
        futures.extend([client.submit(job, fn, i, ds) for i, fn in enumerate(filelist)])
    else:
        futures.extend([client.submit(job, filelist[i], i, ds) for i in indx])
    return futures

def submitloops(ds, filelist, indx) -> None:
    """Put file processing in loops, i.e. one file by one file.
    Usually used for large file size.
    
    Parameters
    - `ds`: dataset name
    - `filelist`: List of files to process
    - `indx`: List of indices to process"""
    print(f"Processing {ds}...")
    if indx is None:
        print(f"Expected to see {len(filelist)} number of outputs")
        for i, file in enumerate(filelist):
            job(file, i, ds) 
    else:
        print(f"Starting with file number {indx[0]}............")
        print(f"Expected to see {len(indx)} number of outputs")
        for i in indx:
            job(filelist[i], i, ds)
    return None

def submitjobs(client, dsindx=None, fileindx=None) -> int:
    """Run jobs based on client settings.
    If a valid client is found and future mode is true, submit simultaneously run jobs.
    If not, fall back into a loop mode. Note that even in this mode, any dask computations will be managed by client explicitly or implicitly.
    """
    loaded = loadmeta(filterfunc=filterResume, dsindx=dsindx)
    if not loaded: 
        print("All the files have been processed for this dataset!")
        return 0
    if client is None or (not daskcfg.get('SPAWN_FUTURE', False)): 
        print("Submit jobs in loops!")
        for ds, dsitems in loaded.items():
            resumeindx = dsitems.get('resumeindx', None)
            submitloops(ds, dsitems['filelist'], resumeindx)
        return 0
    else: 
        for ds, dsitems in loaded.items():
            resumeindx = dsitems.get('resumeindx', None)
            futures = submitfutures(client, ds, dsitems['filelist'], resumeindx)
            result = process_futures(futures)
        return 0

def sampleloaded(loaded) -> int:
    """Sample a random file from loaded metadata."""
    firstitem = loaded[next(iter(loaded))]
    randindx = random.randint(0, len(firstitem['filelist']))
    splitname = '//store'
    filename = firstitem['filelist'][randindx]
    if not filename.startswith('/store'):
        splitted = filename.split(splitname, 1)
        redir = splitted[0]
        finame = '/store' + splitted[1]
        size, mod_time = get_xrdfs_file_info(finame, redir)
    else:
        size, mod_time = get_xrdfs_file_info(filename)
    return size
    
def testsubmit():
    client = spawnclient()
    print(client.get_versions(check=True))
    with open(rs.INPUTFILE_PATH, 'r') as samplepath:
        metadata = json.load(samplepath)
    for dataset, info in metadata.items():
        print(f"Processing {dataset}...")
        client.submit(job, info['filelist'][0], 0, dataset)
    return client

def process_futures(futures, results_file='futureresult.txt', errors_file='futureerror.txt'):
    """Process a list of Dask futures.
    :param futures: List of futures returned by client.submit()
    :return: A list of results from successfully completed futures.
    """
    processed_results = []
    errors = []
    for future in as_completed(futures):
        try:
            if future.exception():
                error_msg = f"An error occurred: {future.exception()}"
                print(future.traceback())
                print(f"Traceback: {traceback.extract_tb(future.traceback())}")
                print(error_msg)
                errors.append(error_msg)
            else:
                result = future.result()
                processed_results.append(result)
        except Exception as e:
            error_msg = f"Error processing future result: {e}"
            print(error_msg)
            errors.append(error_msg)
    with open(results_file, 'w') as f:
        for result in processed_results:
            f.write(str(result) + '\n')
    if errors:
        with open(errors_file, 'w') as f:
            for error in errors:
                f.write(str(error) + '\n')
    return processed_results, errors

def spawnLocal():
    """Spawn dask client for local cluster"""
    # size_mb = size/1024/1024
    cluster = LocalCluster(processes=daskcfg.get('SPAWN_PROCESS', False), threads_per_worker=daskcfg.get('THREADS_NO', 2))
    cluster.adapt(minimum=1, maximum=4)
    client = Client(cluster)
    print("successfully created a dask client in local cluster!")
    print("===================================")
    print(client)
    return client

def spawnclient(default=False):
    """Spawn appropriate client based on runsetting."""
    if not daskcfg.SPAWN_CONDOR:
        client = spawnLocal()
    else:
        client = spawnCondor(default)
    return client 

def spawnCondor(default=False):
    """Spawn dask client for condor cluster"""
    from lpcjobqueue import LPCCondorCluster

    print("Trying to submit jobs to condor via dask!")

    if default:
        cluster = LPCCondorCluster(ship_env=True)
        cluster.adapt(maximum=3)
        print(cluster.job_script())
    else:
        condor_args = {"ship_env": True, 
                    "processes": daskcfg.PROCESS_NO,
                    "cores": daskcfg.CORE_NO,
                    "memory": daskcfg.MEMORY,
                    "disk": daskcfg.DISK
                    }
        cluster = LPCCondorCluster(**condor_args)
        cluster.job_extra_directives = {
            'output': 'dask_output.$(ClusterId).$(ProcId).out',
            'error': 'daskr_error.$(ClusterId).$(ProcId).err',
            'log': 'dask_log.$(ClusterId).log',
        }
        cluster.adapt(minimum=daskcfg.MIN_WORKER, maximum=daskcfg.MAX_WORKER)
        print(cluster.job_script())

    client = Client(cluster)
    print("One client created in LPC Condor!")
    print("===================================")
    print(client)

    return client

