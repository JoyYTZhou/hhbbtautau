# UPDATE TIME: 2024-02-01
# FROM JOY
from config.selectionconfig import runsetting as rs
import os
import time
from dask.distributed import Client, LocalCluster
from analysis.selutility import *

def main():
    if not os.getenv("IS_CONDOR", False): 
        cluster = LocalCluster(n_workers=3, threads_per_worker=8)
        client = Client(cluster)
        print("successfully created a dask client!")
        print("===================================")
        print(client)

    start_time = time.time()

    proc = Processor(rs)

    if rs.TEST_MODE:
        proc.runmultiple(1,2)
    else:
        proc.runmultiple()
        
    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

    client.close()

if __name__ == '__main__':
    main()



