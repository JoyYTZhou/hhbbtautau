# UPDATE TIME: 2024-02-18
# FROM JOY
import os
import time
import logging

PARENT_DIR = os.path.dirname(__file__) 
from config.selectionconfig import dasksetting as dask_cfg
from config.selectionconfig import runsetting as rs

logger = logging.getLogger("Selection")
logger.setLevel(logging.DEBUG)

debug_handler = logging.FileHandler(f"daskworker_{rs.PROCESS_NAME}.log")
debug_handler.setLevel(logging.DEBUG)

error_handler = logging.FileHandler(f"daskworker_{rs.PROCESS_NAME}.err")
error_handler.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

logger.addHandler(debug_handler)
logger.addHandler(error_handler)

# if not rs.SPAWN_CONDOR: os.environ['CONDOR_CONFIG'] = os.path.join(PARENT_DIR, ".condor_config")

def main():
    start_time = time.time()
    
    from analysis.spawndask import submitjobs

    if dask_cfg.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient
        client = spawnclient(default=False)
    else:
        client = None
    
    submitjobs(client)

    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

if __name__ == '__main__':
    main()




