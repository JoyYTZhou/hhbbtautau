# UPDATE TIME: 2024-05-10
# FROM JOY
import os, gc, argparse

PARENT_DIR = os.path.dirname(__file__) 
from utils.filesysutil import checkx509
from config.selectionconfig import dasksetting as dask_cfg
from config.selectionconfig import runsetting as rs

def main():
    gc.enable()
    from analysis.spawndask import submitjobs

    parser = argparse.ArgumentParser(description='Event selection options')
    parser.add_argument('--dsindx', type=int, help='an optional integer for dataset index', default=None)
    args = parser.parse_args()

    if dask_cfg.SPAWN_CLIENT:
        from analysis.spawndask import spawnclient
        client = spawnclient(default=False)
    else:
        client = None
        print("Not spawning client explicitly!")
    checkx509()

    submitjobs(client, args.dsindx)

if __name__ == '__main__':
    main()




