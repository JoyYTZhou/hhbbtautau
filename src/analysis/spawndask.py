from dask.distributed import Client, LocalCluster
from dask.distributed import as_completed
import json as json
import gc
from itertools import islice

from .custom import switch_selections
from .processor import Processor
from utils.filesysutil import glob_files, initLogger
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

logger = initLogger(__name__.split('.')[-1], rs.PROCESS_NAME)
evtselclass = switch_selections(rs.SEL_NAME)
with open("src/data/data.json", 'r') as data:
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
    logger.info(f"Processing filename {fn}")
    print(f"Processing filename {fn}")
    try: 
        proc.runfile(fn, i)
        logger.info(f"Execution finished for file index {i} in {dataset}!")
        return True
    except ValueError as e:
        logger.error(f"ValueError encountered for file index {i} in {dataset}: {e}", exc_info=True)
        return False
    except TypeError as e:
        logger.error(f"TypeError encountered for file index {i} in {dataset}: {e}", exc_info=True)
        return False

def runfutures(client):
    futures = submitjobs(client)
    if futures is not None: process_futures(futures)
    
def loadmeta():
    """Load metadata from input file"""
    if rs.INPUTFILE_PATH.endswith('.json'):
        with open(rs.INPUTFILE_PATH, 'r') as samplepath:
            metadata = json.load(samplepath)
        loaded = metadata
        if rs.RESUME: 
            if isinstance(rs.DSINDX, int):
                sliced_dict = dict(islice(metadata.items(), rs.DSINDX, None))
            elif isinstance(rs.DSINDX, list):
                sliced_dict = {key: metadata[key] for key in rs.DSINDX if key in metadata}
            elif isinstance(rs.DSINDX, str):
                pass
            loaded = sliced_dict
    elif rs.INPUTFILE_PATH.startswith('/store/user/'):
        loaded = realmeta[rs.PROCESS_NAME]
        for dataset in loaded.keys():
            loaded[dataset]['filelist'] = glob_files(rs.INPUTFILE_PATH, startpattern=dataset, endpattern='.root')
    return loaded

def submitfutures(client):
    metadata = loadmeta()
    for dataset, info in metadata.items():
        futures = [client.submit(job, fn, i, dataset) for i, fn in enumerate(info['filelist'])]
        logger.info("Futures submitted!")
    return futures

def submitloops():
    """Put file processing in loops, i.e. one file by one file.
    Usually used for large file size."""
    metadata = loadmeta()
    for j, (dataset, info) in enumerate(metadata.items()):
        logger.info(f"Processing {dataset}...")
        logger.info(f"Expected to see {len(info['filelist'])} number of outputs")
        for i, file in enumerate(info['filelist']):
            if rs.RESUME and j==0:
                if i >= rs.FINDX: 
                    job(file, i, dataset)
                    gc.collect()
            else:
                job(file, i, dataset)
                gc.collect() 
    return None

def submitjobs(client):
    """Run jobs based on client settings.
    If a valid client is found and future mode is true, submit simultaneously run jobs.
    If not, fall back into a loop mode. Note that even in this mode, any dask computations will be managed by client.
    """
    result = None
    if client is None or (not daskcfg.SPAWN_FUTURE): result = submitloops()
    else: result = submitfutures(client)
    return result

def testsubmit():
    client = spawnclient()
    print(client.get_versions(check=True))
    with open(rs.INPUTFILE_PATH, 'r') as samplepath:
        metadata = json.load(samplepath)
    for dataset, info in metadata.items():
        logger.info(f"Processing {dataset}...")
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
                logger.info(error_msg)
                errors.append(error_msg)
            else:
                result = future.result()
                processed_results.append(result)
        except Exception as e:
            error_msg = f"Error processing future result: {e}"
            logger.info(error_msg)
            errors.append(error_msg)
    with open(results_file, 'w') as f:
        for result in processed_results:
            f.write(result + '\n')
    if errors:
        with open(errors_file, 'w') as f:
            for error in errors:
                f.write(error + '\n')
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
    cluster = LocalCluster(processes=daskcfg.SPAWN_PROCESS, threads_per_worker=daskcfg.THREADS_NO)
    cluster.adapt(minimum=1, maximum=3)
    client = Client(cluster)
    print("successfully created a dask client in local cluster!")
    print("===================================")
    print(client)
    return client