# UPDATE TIME: 2024-02-01
# FROM JOY
import os
import time
from dask.distributed import LocalCluster, Client
import yaml

os.environ['PARENT_DIR'] = os.path.dirname(__file__) 

from analysis.helper import *
from analysis.spawndask import *

def main():
    start_time = time.time()

    from analysis.selutility import Processor
    
    print("successfully imported everything!")

    proc = Processor(rs)
    
    if rs.SPAWN_CLIENT:
        client = spawnclient()
        proc.rundata(client)

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")
    if rs.SPAWN_CLIENT: client.close()

if __name__ == '__main__':
    main()




