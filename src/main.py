# UPDATE TIME: 2023-09-15
# FROM JOY
import uproot
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from analysis.processing import *
from tqdm import tqdm
import glob
import json
import argparse
from config.selectionconfig import runsetting as rs

with open(rs.INPUTFILE_PATH, 'r') as samplepath:
    data = json.load(samplepath)

cutflow = init_output()

if not rs.TEST_MODE:
    # TODO: Place holder for now
    pass
else:
    fileset = {'DYJets': data['Background']['DYJets']}
    iterative_run = processor.Runner(
        executor=processor.IterativeExecutor(compression=None),
        schema=BaseSchema,
    )
    out = iterative_run(
        fileset,
        treename=rs.TREE_NAME,
        processor_instance=hhbbtautauProcessor(),
    )
    output_export(out)


