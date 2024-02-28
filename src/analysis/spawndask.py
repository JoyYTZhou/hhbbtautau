from dask.distributed import Client, LocalCluster
import dask.config
from dask.distributed import as_completed
from .selutility import Processor
import json as json
import logging
from .helper import *
import gc
from itertools import islice
from config.selectionconfig import runsetting as rs
from config.selectionconfig import dasksetting as daskcfg

def job(fn, i, dataset):
    proc = Processor(rs, dataset)
    proc.runfile(fn, i)

def runfutures(client):
    futures = submitjobs(client)
    if futures is not None: process_futures(futures)
    
def loadmeta(resume=False, dsindx=None, fileindx=None):
    with open(rs.INPUTFILE_PATH, 'r') as samplepath:
        metadata = json.load(samplepath)
    if resume: 
        pass
    return metadata

def submitfutures(client):
    metadata = loadmeta()
    for dataset, info in metadata.items():
        futures = [client.submit(job, fn, i, dataset) for i, fn in enumerate(info['filelist'])]
        logging.info("Futures submitted!")
    return futures

def submitloops():
    """Put file processing in loops, i.e. one file by one file.
    Usually used for large file size."""
    metadata = loadmeta()
    for dataset, info in metadata.items():
        logging.info(f"Processing {dataset}...")
        logging.info(f"Expected to see {len(info['filelist'])} number of outputs")
        for i, file in enumerate(info['filelist']):
            logging.info(f"Processing filename {file}")
            job(file, i, dataset)
            logging.info(f"Execution finished for filename {file}!")
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
        logging.info(f"Processing {dataset}...")
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
                logging.info(error_msg)
                errors.append(error_msg)
            else:
                result = future.result()
                processed_results.append(result)
        except Exception as e:
            error_msg = f"Error processing future result: {e}"
            logging.info(error_msg)
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