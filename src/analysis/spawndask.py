from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
from lpcjobqueue import LPCCondorCluster
from config.selectionconfig import runsetting as rs
import dask.config
from .selutility import Processor
import json as json
import logging
from .helper import *

def job(fn, i, dataset):
    proc = Processor(rs, dataset)
    proc.runfile(fn, i)

def submitfutures(client):
    with open(rs.INPUTFILE_PATH, 'r') as samplepath:
        metadata = json.load(samplepath)
    for dataset, info in metadata.items():
        logging.info(f"Processing {dataset}...")
        if client==None:
            logging.info("No client spawned! In test mode.")
            logging.info(f"Processing filename {info['filelist'][0]}")
            job(info['filelist'][0], 0, dataset)
            logging.info("Execution finished!")
        else:
            futures = [client.submit(job, fn, i, dataset) for i, fn in enumerate(info['filelist'])]
            logging.info("Futures submitted!")
            return futures
 
def spawnclient():
    """Spawn appropriate client based on runsetting."""
    if not rs.IS_CONDOR:
        client = spawnLocal()
    else:
        client = spawnCondor()
    return client 

def spawnCondor():
    """Spawn dask client for condor cluster"""
    print("Trying to submit jobs to condor via dask!")

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