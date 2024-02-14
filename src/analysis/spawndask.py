from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
from lpcjobqueue import LPCCondorCluster
from analysis.helper import *
from config.selectionconfig import runsetting as rs
import dask.config

def spawnCondor():
    print("Trying to submit jobs to condor via dask!")

    cluster = LPCCondorCluster(ship_env=True)
    cluster.adapt(minimum=rs.MIN_WORKER, maximum=rs.MAX_WORKER)
    client = Client(cluster)
    print("One client created!")
    print("===================================")
    print(client)
    return client