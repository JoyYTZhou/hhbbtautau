from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import traceback
import os

from .custom import switch_selections
from .processor import Processor
from utils.filesysutil import glob_files, initLogger, check_missing, checkpath
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

pjoin = os.path.join

# logger = initLogger(__name__.split('.')[-1], rs.PROCESS_NAME)
evtselclass = switch_selections(rs.SEL_NAME)

parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

datapath = pjoin(parent_directory, 'data', 'data.json')
with open(datapath, 'r') as data:
    realmeta = json.load(data)

def job(fn, i, dataset, eventSelection=evtselclass):
    """Run the processor for a single file.
    Parameters
    - `fn`: The name of the file to process
    - `i`: The index of the file in the list of files
    - `dataset`: The name of the dataset (same dataset has same xsection)
    - `eventSelection`: Custom Defined Event Selection Class
    """
    proc = Processor(rs, dataset, eventSelection)
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
    
def loadmeta():
    """Load metadata from input file"""
    if rs.INPUTFILE_PATH.endswith('.json'):
        inputdatap = pjoin(parent_directory, rs.INPUTFILE_PATH)
        with open(inputdatap, 'r') as samplepath:
            metadata = json.load(samplepath)
        loaded = checkresumes(metadata)
    elif rs.INPUTFILE_PATH.startswith('/store/user/'):
        loaded = realmeta[rs.PROCESS_NAME]
        for dataset in loaded.keys():
            loaded[dataset]['filelist'] = glob_files(rs.INPUTFILE_PATH, startpattern=dataset, endpattern='.root')
    else:
        raise TypeError("Check INPUTFILE_PATH in runsetting.toml. It's not of a valid format!")
    return loaded

def checkresumes(metadata):
    """Resume jobs from last checkpoint"""
    statcode = checkpath(rs.TRANSFER_PATH, createdir=False)
    if statcode != 0: 
        loaded = metadata
        return loaded
    else:
        loaded = {}
        datasets = metadata.keys()
        for ds in datasets:
            fileno = len(metadata[ds]['filelist'])
            fileindx = check_missing(f'{ds}_cutflow', fileno, rs.TRANSFER_PATH, endpattern='.csv')
            if fileindx != []:
                loaded[ds] = {}
                loaded[ds]['resumeindx'] = fileindx
                loaded[ds]['filelist'] = metadata[ds]['filelist']
    if loaded == {}: 
        raise FileExistsError("All the files have been processed for this process!")
    return loaded

def submitfutures(client, ds, filelist, indx):
    futures = []
    if indx is None:
        futures.extend([client.submit(job, fn, i, ds) for i, fn in enumerate(filelist)])
    else:
        futures.extend([client.submit(job, filelist[i], i, ds) for i in indx])
    return futures

def submitloops(ds, filelist, indx):
    """Put file processing in loops, i.e. one file by one file.
    Usually used for large file size."""
    print(f"Processing {ds}...")
    failed = 0
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

def submitjobs(client):
    """Run jobs based on client settings.
    If a valid client is found and future mode is true, submit simultaneously run jobs.
    If not, fall back into a loop mode. Note that even in this mode, any dask computations will be managed by client.
    """
    loaded = loadmeta()
    if client is None or (not daskcfg.SPAWN_FUTURE): 
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

def spawnLocal():
    """Spawn dask client for local cluster"""
    cluster = LocalCluster(processes=daskcfg.get('SPAWN_PROCESS', False), threads_per_worker=daskcfg.get('THREADS_NO', 4))
    cluster.adapt(minimum=1, maximum=4)
    client = Client(cluster)
    print("successfully created a dask client in local cluster!")
    print("===================================")
    print(client)
    return client