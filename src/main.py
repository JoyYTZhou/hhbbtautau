# UPDATE TIME: 2023-09-15
# FROM JOY
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from analysis.processing import *
from analysis.dsmethods import extract_items
from tqdm import tqdm
import glob
import json
import argparse
from config.selectionconfig import runsetting as rs

with open(rs.INPUTFILE_PATH, 'r') as samplepath:
    data = json.load(samplepath)

if rs.TEST_MODE:
    if rs.FILE_SET_LOCAL:
        fileset = rs.FILE_SET 
    else:
        fileset = extract_items(data['Background'], "DYJets") 
else:
    bkgFS = data['Background']
    sigFS = data['Signal']

if rs.SINGLE_FILE:
    pass






output_export(out, rs)
