# UPDATE TIME: 2023-09-15
# FROM JOY
import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
import argparse
from tqdm import tqdm
import glob
import numpy as np
import uproot

# TODO: Change this to use cfg
parser = argparse.ArgumentParser(
                    prog='looper',
                    description='loops over the events directory and performs selections',
                    epilog='==============================================')

parser.add_argument('-f', '--filename', required=True, type=str)
parser.add_argument('-d', '--isDirectory', required=True, type=bool)
args=parser.parse_args()

if args.isDirectory==True:
    filelist=glob.glob(args.filename+"/**/*.root", recursive=True)
    
# TODO: Change this later so that metadata tag can be extracted from cfg file to include sample type
events = NanoEventsFactory.from_root(
    filelist,
    schemeclass=BaseSchema,
    metadata={"dataset": "ttbar"},).events()

# Create a root file for after object level selections



