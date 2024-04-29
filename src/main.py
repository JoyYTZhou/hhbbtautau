# UPDATE TIME: 2024-02-18
# FROM JOY
import os, time, gc

PARENT_DIR = os.path.dirname(__file__) 
from utils.filesysutil import checkx509
from config.selectionconfig import dasksetting as dask_cfg
from config.selectionconfig import runsetting as rs


def main():
    gc.enable()
    gc.set_debug(gc.DEBUG_LEAK)

    start_time = time.time()
    
    from analysis.spawndask import submitjobs

    if dask_cfg.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient
        client = spawnclient(default=False)
    else:
        client = None
        print("Not spawning client explicitly!")
    
    checkx509()
    submitjobs(client)
    end_time = time.time()
    print(f"Execution time is {end_time - start_time} seconds")

if __name__ == '__main__':
    main()




