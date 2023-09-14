import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import argparse
from tqdm import tqdm
import glob

parser = argparse.ArgumentParser(
                    prog='looper',
                    description='loops over the events directory and performs selections',
                    epilog='==============================================')

parser.add_argument('-f', '--filename', required=True, type=str)
parser.add_argument('-d', '--isDirectory', required=True, type=bool)
args=parser.parse_args()

if args.isDirectory==True:
    filelist=glob.glob(args.filename+"/**/*.root", recursive=True)
    

