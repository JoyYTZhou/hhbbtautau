from dask.distributed import Client, LocalCluster
from lpcjobqueue import LPCCondorCluster
from config.selectionconfig import runsetting as rs
import dask.config
from dask.distributed import as_completed
from .selutility import Processor
import json as json
import logging
from .helper import *

def job(fn, i, dataset):
    proc = Processor(rs, dataset)
    proc.runfile(fn, i)

def runfutures(client):
    futures = submitfutures(client)
    if futures is not None: process_futures(futures)
    
def submitfutures(client):
    """Submit future concurrent tasks to client across a set of distributed workers."""
    with open(rs.INPUTFILE_PATH, 'r') as samplepath:
        metadata = json.load(samplepath)
    for dataset, info in metadata.items():
        logging.info(f"Processing {dataset}...")
        if client==None:
            logging.info("No client spawned! In test mode.")
            logging.info(f"Processing filename {info['filelist'][0]}")
            job(info['filelist'][0], 0, dataset)
            logging.info("Execution finished!")
            return None
        else:
            futures = [client.submit(job, fn, i, dataset) for i, fn in enumerate(info['filelist'])]
            logging.info("Futures submitted!")
            return futures

def testsubmit():
    client = spawnclient()
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
    if not rs.IS_CONDOR:
        client = spawnLocal()
    else:
        client = spawnCondor(default)
    return client 

def spawnCondor(default=False):
    """Spawn dask client for condor cluster"""
    print("Trying to submit jobs to condor via dask!")

    if default:
        cluster = LPCCondorCluster(ship_env=True)
        cluster.adapt(minimum=1)
        print(cluster.job_script())
    else:
        condor_args = {"ship_env": True, 
                    "processes": rs.PROCESS_NO,
                    "cores": rs.CORE_NO,
                    "memory": rs.MEMORY
                    }
        cluster = LPCCondorCluster(**condor_args)
        cluster.job_extra_directives = {
            'output': 'dask_output.$(ClusterId).$(ProcId).out',
            'error': 'daskr_error.$(ClusterId).$(ProcId).err',
            'log': 'dask_log.$(ClusterId).log',
        }
        cluster.adapt(minimum=rs.MIN_WORKER, maximum=rs.MAX_WORKER)

    client = Client(cluster)
    print("One client created in LPC Condor!")
    print("===================================")
    print(client)

    return client

def spawnLocal():
    """Spawn dask client for local cluster"""
    cluster = LocalCluster(processes=False, threads_per_worker=2)
    cluster.adapt(minimum=0, maximum=6)
    client = Client(cluster)
    print("successfully created a dask client in local cluster!")
    print("===================================")
    print(client)
    return client