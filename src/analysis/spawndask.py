from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
from lpcjobqueue import LPCCondorCluster
from config.selectionconfig import runsetting as rs
import dask.config

def spawnclient():
    if not rs.IS_CONDOR:
        client = spawnLocal()
    else:
        client = spawnCondor()
    return client 

def spawnCondor():
    """Spawn dask client for condor cluster"""

    print("Trying to submit jobs to condor via dask!")
    cluster = LPCCondorCluster(ship_env=True)
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