# UPDATE TIME: 2024-02-18
# FROM JOY
import os
import time
import logging

PARENT_DIR = os.path.dirname(__file__) 
from config.selectionconfig import dasksetting as dask_cfg

# if not rs.SPAWN_CONDOR: os.environ['CONDOR_CONFIG'] = os.path.join(PARENT_DIR, ".condor_config")
logging.basicConfig(filename="daskworker.log", 
                    filemode='w', 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    level=logging.DEBUG)

def main():
    start_time = time.time()
    
    from analysis.spawndask import runfutures

    if dask_cfg.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient, testsubmit
        client = testsubmit()
        # client = spawnclient()
    else:
        client = None
    
    # runfutures(client)
    # client = testsubmit()

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

if __name__ == '__main__':
    main()




