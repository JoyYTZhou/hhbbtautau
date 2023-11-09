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

if not rs.TEST_MODE:
    # TODO: Place holder for now
    pass
else:
    if rs.SINGLE_FILE:
        filename = rs.SINGLE_PATH
        single_file = uproot.open(filename)
        events = NanoEventsFactory.from_root(
            single_file,
            entry_stop=10000,
            metadata={"dataset": "DYJets"},
            schemaclass=BaseSchema,
        ).events()
        p = hhbbtautauProcessor()
        out = p.process(events)
        out = p.postprocess(out)
    # Run multiple files using executors
    else:
        if rs.LOCAL_TEST:
            fileset = rs.FILE_SET
        else:
            fileset = rs.FILE_SET
            # fileset = {'DYJets': data['Background']['DYJets']}
        if rs.RUN_MODE == "iterative":
            iterative_run = processor.Runner(
                    executor=processor.IterativeExecutor(desc="Executing fileset",compression=None),
                    schema=BaseSchema,
                    chunksize=rs.CHUNK_SIZE
                )
            out = iterative_run(
                    fileset,
                    treename=rs.TREE_NAME,
                    processor_instance=hhbbtautauProcessor()
                )
output_export(out, rs)


