
import uproot
from coffea.nanoevents import NanoEventsFactory
from coffea.processor import dict_accumulator, column_accumulator, defaultdict_accumulator
from coffea.nanoevents.schemas import BaseSchema
from coffea import processor
from analysis.processing import hhbbtautauProcessor
from analysis.dsmethods import extract_process
from analysis.histbooker import accumulate_dicts
import pandas as pd
import re

def run_single(filename, process_name, post_process=True):
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
    if post_process: p.postprocess(out)
    return out

def rerun_exceptions(exceptions):
    """Rerun the failed jobs. For now only handles the case where the exception is an OSError with the message "XRootD error: [ERROR] Operation expired".

    :param exceptions: Mapping of filename : exception
    :type exceptions: dict
    :return: A dictionary accumulator containing the output of the rerun jobs
    :rtype: dict_accumulator()
    """
    exception_out = dict_accumulator()
    for filename, exception in exceptions.items():
        if isinstance(exception, OSError) and "XRootD error: [ERROR] Operation expired" in str(exception):
            # Rerun the job for the failed file
            process_name = extract_process(filename)
            out = run_single(filename, process_name, False)
            exception_out.add(out)
            del out
    return exception_out
            
def future_exec(rs):
    """Create a futures executor.
    
    :param rs: run settings
    :type rs: DynaConf object
    :return: futures executor
    :rtype: coffea.processor.FuturesExecutor
    """

    exec = processor.FuturesExecutor(
        compression=rs.COMPRESSION,
        workers=rs.WORKERS,
        recoverable=rs.RECOVERABLE,
        merging=(rs.N_BATCHES, rs.MIN_SIZE, rs.MAX_SIZE)
    )
    return exec

def init_out(fileset, rs):
    """Initialize the output accumulator for further accumulation

    :param fileset: fileset
    :type fileset: dict
    :param rs: run settings
    :type rs: DynaConf object
    :return: output accumulator
    :rtype: dict_accumulator
    """
    dataset, filelist = next(iter(fileset.items()))
    filename = filelist.pop(0)
    out = run_single(filename, dataset, False)
    
    for dataset in fileset:
        if filename in fileset[dataset]:
            fileset[dataset].remove(filename)
            break
    
    if not filelist:
        del fileset[dataset]

    return out

def future_runner_wrapper(fileset, rs):
    """Wrapper around the futures executor WITH RUNNER to handle the case where the job fails due to an XRootD error.

    :param fileset: fileset
    :type fileset: dict
    :param rs: run settings
    :type rs: DynaConf object
    :return: output (still accumulatable)
    :rtype: dict_accumulator
    """
    
    run = processor.Runner(
            future_exec(rs),
            schema=BaseSchema,
            chunksize=rs.CHUNK_SIZE,
            xrootdtimeout=rs.TIMEOUT
        )
    out = init_out(fileset, rs)
    while True:
        try: 
            result = run(
                fileset,
                treename=rs.TREE_NAME,
                processor_instance=hhbbtautauProcessor(),
            )
            if isinstance(result, tuple) and len(result) == 2:
                out, exceptions = result
                out.add(rerun_exceptions(exceptions))
                break
            else:
                out.add(result)
                break
        except (OSError, RuntimeError, FileNotFoundError) as e:
            filename = handle_error(e, fileset)
            if filename:
                out.add(rerun_exceptions(filename))
        return out

def handle_error(e, fileset):
    """Handle an error that occurred during the processing of a file.
    
    Apply an error-time correction to the fileset, and gracefully continue 
    processing the remaining files.
    
    :param e: exception object
    :type e: Exception
    :param fileset: fileset
    :type fileset: dict
    :return: filename of the file that caused the error
    :rtype: string
    """
    if isinstance(e, OSError):
        filename = re.search(r"in file (.*\.root)", str(e)).group(1)
        print(f"An XRootD error occurred with file {filename}: {e}")
    elif isinstance(e, RuntimeError):
        filename = re.search(r"Metadata for file (.*?) could not be accessed", str(e)).group(1)
        print(f"An error occurred with file {filename}: {e}")
    elif isinstance(e, FileNotFoundError):
        filename = re.search(r"/store/.*\.root", str(e)).group(0)
        print(f"An error occurred with file {filename}: {e}")
    else:
        return None

    for dataset in fileset:
        if filename in fileset[dataset]:
            fileset[dataset].remove(filename)
            break
    return filename

def run_jobs(fileset, rs):
    """Run the processor on a fileset.
    
    :param fileset: fileset
    :type fileset: dict
    :param rs: run settings
    :type rs: DynaConf object
    :return: output (still accumulatable)
    :rtype: dict_accumulator
    """
    if rs.RUN_MODE == "future":
       out = future_runner_wrapper(fileset, rs) 
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
        out = None
    else:
        raise TypeError("Unknown run mode: %s" % rs.RUN_MODE)


    return out