from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import traceback, os, random

from .custom import switch_selections
from .processor import Processor
from utils.filesysutil import glob_files, get_xrdfs_file_info, initLogger, check_missing, checkpath, pjoin
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

# logger = initLogger(__name__.split('.')[-1], rs.PROCESS_NAME)
evtselclass = switch_selections(rs.SEL_NAME)

parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

datapath = pjoin(parent_directory, 'data', 'data.json')
with open(datapath, 'r') as data:
    realmeta = json.load(data)

def inittransfer(selname, processname) -> str:
    condorbase = os.environ.get("CONDOR_BASE", False)
    if condorbase:
        return pjoin(condorbase, selname, processname)
    else:
        raise EnvironmentError("Export condor base directory properly!")

def getTransfer(rtcfg) -> str | bool:
    if rtcfg.get('TRANSFER', True): 
        rtcfg_path = rtcfg.get('TRANSFER_PATH', False)
        if rtcfg_path:
            transfer = rtcfg_path
        else:
            selname = rtcfg.SEL_NAME
            processname = rtcfg.PROCESS_NAME
            transfer = inittransfer(selname, processname)
        return transfer
    else:
        return False

def job(fn, i, dataset, transferP, eventSelection=evtselclass) -> int:
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
    
def loadmeta(dsindx=None, inputpath=rs.INPUTFILE_PATH) -> dict:
    """Load metadata from input file, or straight from directories containing files to process.
    
    Parameters
    - `dsindx`: Index of datasets to process in json file. If None, process all datasets.
    
    Return
    """
    if inputpath.endswith('.json'):
        inputdatap = pjoin(parent_directory, inputpath)
        with open(inputdatap, 'r') as samplepath:
            metadata = json.load(samplepath)
        if dsindx is not None:
            dskey = list(metadata.keys())[dsindx]
            metadata = {dskey: metadata[dskey]}
        loaded = checkresumes(metadata)
    elif inputpath.startswith('/store/user/'):
        loaded = realmeta[rs.PROCESS_NAME]
        for dataset in loaded.keys():
            loaded[dataset]['filelist'] = glob_files(inputpath, startpattern=dataset, endpattern='.root')
    else:
        raise TypeError("Check INPUTFILE_PATH in runsetting.toml. It's not of a valid format!")
    return loaded

def checkresumes(metadata) -> dict:
    """Resume jobs from last checkpoint"""
    tsferP = getTransfer(rs)
    if tsferP:
        statcode = checkpath(tsferP, createdir=False)
        if statcode != 0: 
            loaded = metadata
        else:
            loaded = {}
            datasets = metadata.keys()
            for ds in datasets:
                fileno = len(metadata[ds]['filelist'])
                fileindx1 = set(check_missing(f'{ds}_cutflow', fileno, tsferP, endpattern='.csv'))
                fileindx2 = set(check_missing(f'{ds}', fileno, tsferP, endpattern='.root'))
                fileindx = list(fileindx1.union(fileindx2))
                if fileindx != []:
                    loaded[ds] = {}
                    loaded[ds]['resumeindx'] = fileindx
                    loaded[ds]['filelist'] = metadata[ds]['filelist']
            if loaded == {}: 
                raise FileExistsError("All the files have been processed for this process!")
    else:
        loaded = metadata
    return loaded

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

def submitjobs(client, dsindx=None) -> int:
    """Run jobs based on client settings.
    If a valid client is found and future mode is true, submit simultaneously run jobs.
    If not, fall back into a loop mode. Note that even in this mode, any dask computations will be managed by client explicitly or implicitly.
    """
    loaded = loadmeta(dsindx)
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
    cluster = LocalCluster(processes=daskcfg.get('SPAWN_PROCESS', False), threads_per_worker=daskcfg.get('THREADS_NO', 4))
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

