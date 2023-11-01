# UPDATE TIME: 2023-09-15
# FROM JOY
import uproot
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from analysis.processing import *
from tqdm import tqdm
import glob
import json
import argparse

parser = argparse.ArgumentParser(
    prog='looper',
    description='loops over the events directory and performs selections',
    epilog='==============================================')

parser.add_argument('-f', '--file', required=True, type=str,
                    help="the path to json file containing the data samples")
parser.add_argument('-d', '--isDirectory', required=True, type=bool)
parser.add_argument('-t', '--test', required=True, default=False, type=bool)
parser.add_argument('-o', '--output', required=True, type=str, help="output directory path")
args = parser.parse_args()

with open(args.file, 'r') as samplepath:
    data = json.load(samplepath)

cutflow = init_output()

if not args.test:
    for sample in ['Background', "Signal"]:
        for process in data[sample].keys():
            filelist = data[sample][process]
            events = NanoEventsFactory.from_root(
                filelist,
                schemaclass=BaseSchema,
                metadata={"dataset": process},).events()  
            p = hhbbtautauProcessor()
            out = p.process(events)
            p.postprocess(out, args.output, cutflow)
else:
    filelist = data['Background']['DYJets']
    events = NanoEventsFactory.from_root(
        filelist,
        schemaclass=BaseSchema,
        metadata={"dataset": 'DYJets'},).events() 
    p = hhbbtautauProcessor()
    out = p.process(events)
    p.postprocess(out, args.output, cutflow)

concat_output(cutflow, args.output)
