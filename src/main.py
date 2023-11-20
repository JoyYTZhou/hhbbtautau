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
            metadata={"dataset": rs.PROCESS_NAME},
            schemaclass=BaseSchema,
        ).events()
        p = hhbbtautauProcessor()
        out = p.process(events)
        p.postprocess(out)
    # Run multiple files using executors
    else:
        if rs.FILE_SET_LOCAL:
            fileset = rs.FILE_SET
        else:
            fileset = {rs.PROCESS_NAME: data['Background'][rs.PROCESS_NAME]}
        if rs.RUN_MODE == "iterative":
            iterative_run = processor.Runner(
                executor=processor.IterativeExecutor(
                    desc="Executing fileset", compression=None),
                schema=BaseSchema,
                chunksize=rs.CHUNK_SIZE,
                xrootdtimeout=rs.TIMEOUT
            )
            out = iterative_run(
                fileset,
                treename=rs.TREE_NAME,
                processor_instance=hhbbtautauProcessor()
            )
        elif rs.RUN_MODE == "future":
            futures_run = processor.Runner(
                executor=processor.FuturesExecutor(
                    compression=rs.COMPRESSION,
                    workers=rs.WORKERS,
                    recoverable=rs.RECOVERABLE,
                    merging=(rs.N_BATCHES, rs.MIN_SIZE, rs.MAX_SIZE)
                ),
                schema=BaseSchema,
                chunksize=rs.CHUNK_SIZE,
                xrootdtimeout=rs.TIMEOUT
            )
            out = futures_run(
                fileset,
                treename=rs.TREE_NAME,
                processor_instance=hhbbtautauProcessor(),
            )

output_export(out, rs)
