# UPDATE TIME: 2023-09-15
# FROM JOY
import uproot
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from analysis.processing import *
from tqdm import tqdm
import glob
import json
import argparse

# TODO: Change this to use cfg
parser = argparse.ArgumentParser(
                    prog='looper',
                    description='loops over the events directory and performs selections',
                    epilog='==============================================')

parser.add_argument('-f', '--file', required=True, type=str, help="the path to json file containing the data samples")
parser.add_argument('-d', '--isDirectory', required=True, type=bool)
parser.add_argument('-a', '--all', required=True, default=True, type=bool)
args=parser.parse_args()

with open(args.file, 'r') as samplepath:
    data = json.load(samplepath)

if args.all:
    
# TODO: Change this later so that metadata tag can be extracted from cfg file to include sample type
events = NanoEventsFactory.from_root(
    filelist,
    schemeclass=BaseSchema,
    metadata={"dataset": "ttbar"},).events()

# Create a root file for after object level selections



