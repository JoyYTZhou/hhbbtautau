# UPDATE TIME: 2023-09-15
# FROM JOY
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from tqdm import tqdm
import glob
import json
import argparse
from config.selectionconfig import runsetting as rs
from analysis.dsmethods import extract_items
from analysis.runtask import *

with open(rs.INPUTFILE_PATH, 'r') as samplepath:
    data = json.load(samplepath)

if rs.TEST_MODE:
    if rs.FILE_SET_LOCAL:
        fileset = rs.FILE_SET
    else:
        fileset = extract_items(data['Background'], rs.PROCESS_NAME)
else:
    fileset = data['Background']
    fileset.update(data['Signal'])

del data

out = run_jobs(fileset, rs)
unwrap_col_acc(out)
