from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
from lpcjobqueue import LPCCondorCluster
from config.selectionconfig import runsetting as rs
import dask.config

def spawnclient():
    if not rs.IS_CONDOR:
        client = spawnLocal()
    else:
        process=rs.PROCESS_NAME
        if rs.SUBMIT_DASK: client = spawnCondor()
    return client 

def spawnCondor():
    """Spawn dask client for condor cluster"""
    print("Trying to submit jobs to condor via dask!")

    cluster = LPCCondorCluster(ship_env=True)
    cluster.adapt(minimum=rs.MIN_WORKER, maximum=rs.MAX_WORKER)
    client = Client(cluster)
    print("One client created!")
    print("===================================")
    print(client)
    return client

def spawnLocal():
    """Spawn dask client for local cluster"""
    cluster = LocalCluster(processes=False, threads_per_worker=2)
    cluster.adapt(minimum=0, maximum=6)
    client = Client(cluster)
    print("successfully created a dask client!")
    print("===================================")
    print(client)
    return client