from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting
import argparse

def postprocess():
    DataLoader.hadd_cfs()
    DataLoader.hadd_roots()
    # DataLoader.hadd_csvouts()

def checkouts():
    DataLoader.check_cf()

def getcf():
    DataLoader.merge_cf()

def getobj():
    dl = DataLoader()
    dl.get_objs()

def programchoice() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Choose the post-processing options')
    parser.add_argument('--postprocess', action='store_true', help='Execute hadding procedure for the specified, processed datasets')
    parser.add_argument('--checkouts', action='store_true', help='Check the output files')
    parser.add_argument('--getcf', action='store_true', help='Get total cutflow table')
    parser.add_argument('--getobj', action='store_true', help='Get delimited object files')

    args = parser.parse_args()
    if not any(vars(args).values()):
        parser.print_help()
        parser.exit()

    return args

if __name__ == '__main__':
    args = programchoice()
    if args.postprocess: postprocess()
    if args.checkouts: checkouts()
    if args.getcf: getcf()
    if args.getobj: getobj()

