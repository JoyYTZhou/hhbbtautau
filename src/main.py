# UPDATE TIME: 2024-02-01
# FROM JOY
import os
import time
from dask.distributed import Client, LocalCluster
from dask_jobqueue.htcondor import HTCondorCluster
from lpcjobqueue import LPCCondorCluster
import yaml
import dask.config
from analysis.helper import *
from analysis.spawndask import *

parent_dir = os.path.dirname(__file__)
files_needed = f"""{pjoin(parent_dir, 'src')}, 
    {pjoin(parent_dir, 'lpcsetup.sh')},
    {pjoin(parent_dir, 'setup.sh')},
    {pjoin(parent_dir, 'scripts')},
    {pjoin(parent_dir, 'dirsetup.sh')}
"""

def spawnclient():
    if not rs.IS_CONDOR:
        cluster = LocalCluster(processes=False, threads_per_worker=2)
        cluster.adapt(minimum=0, maximum=6)
        client = Client(cluster)
        print("successfully created a dask client!")
        print("===================================")
        print(client)
    else:
        process=rs.PROCESS_NAME
        if rs.SUBMIT_DASK: client = spawnCondor()
    return client 

def main():
    start_time = time.time()

    from analysis.selutility import Processor
    
    print("successfully imported everything!")

    proc = Processor(rs)

    if rs.TEST_MODE:
        proc.runmultiple(20,21)
    # else:
    #     proc.runmultiple()
        

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

if __name__ == '__main__':
    if rs.SPAWN_CLIENT:
        client = spawnclient()
    main()
    if rs.SPAWN_CLIENT: client.close()




