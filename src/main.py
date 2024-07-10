# UPDATE TIME: 2024-05-10
# FROM JOY
import os, gc, argparse

PARENT_DIR = os.path.dirname(__file__) 
from utils.filesysutil import checkx509, display_top
from config.selectionconfig import dasksetting as dask_cfg

def main():
    gc.enable()
    from analysis.spawndask import submitjobs

    parser = argparse.ArgumentParser(description='Event selection options')
    parser.add_argument('--dsindx', type=int, help='an optional integer for dataset index', default=None)
    parser.add_argument('--diagnose', action='store_true', default=False, help='Enable memory diagnose')
    args = parser.parse_args()
    
    if args.diagnose:
        import tracemalloc
        tracemalloc.start()
        gc.set_debug(gc.DEBUG_LEAK)

    if dask_cfg.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient
        client = spawnclient(default=False)
    else:
        client = None
        print("Not spawning client explicitly!")
    checkx509()

    submitjobs(client, args.dsindx)
    
    if args.diagnose:
        snapshot = tracemalloc.take_snapshot()
        display_top(snapshot)

if __name__ == '__main__':
    main()




