
import uproot
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.schemas import BaseSchema
from coffea import processor
from analysis.processing import hhbbtautauProcessor
from analysis.dsmethods import extract_process

def run_single(filename, process_name):
    """Run the processor on a single file.

    :param filename: path to the root file
    :type filename: string
    :param rs: run settings
    :type rs: DynaConf object
    :return: output
    :rtype: dict_accumulator
    """
    single_file = uproot.open(filename)
    events = NanoEventsFactory.from_root(
        single_file,
        entry_stop=None,
        metadata={"dataset": process_name},
        schemaclass=BaseSchema,
    ).events()
    p = hhbbtautauProcessor()
    out = p.process(events)
    p.postprocess(out)
    return out

def rerun_exceptions(exceptions):
    """Rerun the failed jobs.

    :param exceptions: Mapping of filename : exception
    :type exceptions: dict
    :return: None
    """
    second_out = {}
    for filename, exception in exceptions.items():
        if isinstance(exception, OSError) and "XRootD error: [ERROR] Operation expired" in str(exception):
            # Rerun the job for the failed file
            process_name = extract_process(filename) + "_"
            out = run_single(filename, process_name)
            # export the output
            

def run_jobs(fileset, rs):
    if rs.RUN_MODE == "future":
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
        out, exceptions = futures_run(
            fileset,
            treename=rs.TREE_NAME,
            processor_instance=hhbbtautauProcessor(),
        )
        rerun_exceptions(exceptions)
        
    elif rs.RUN_MODE == "iterative":
        iterative_run = processor.Runner(
            executor=processor.IterativeExecutor(
                desc="Executing fileset", compression=rs.COMPRESSION),
            schema=BaseSchema,
            chunksize=rs.CHUNK_SIZE,
            xrootdtimeout=rs.TIMEOUT,
        )
        out = iterative_run(
            fileset,
            treename=rs.TREE_NAME,
            processor_instance=hhbbtautauProcessor()
        )
    elif rs.RUN_MODE == "dask":
        pass
    else:
        raise TypeError("Unknown run mode: %s" % rs.RUN_MODE)
    return out