from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting
import argparse

def postprocess():
    DataLoader.hadd_cfs()
    DataLoader.hadd_roots()

def getcf():
    DataLoader.merge_cf()

def getobj():
    dl = DataLoader()
    dl.get_objs()

def programchoice():
    parser = argparse.ArgumentParser(description='Choose the post-processing options')
    parser.add_argument(name='--postprocess', action='store_false', help='Execute hadding procedure for the specified, processed datasets')
    parser.add_argument(name='--getcf', action='store_false', help='Get total cutflow table')
    parser.add_argument(name='--getobj', action='store_false', help='Get ')

    args = parser.parse_args()
    if not any(vars(args).values()):
        parser.print_help
        parser.exit
        
    return args

if __name__ == '__main__':
    # postprocess()
    getcf()
    