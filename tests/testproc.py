import os, json, cProfile, argparse, time, pstats, logging, tracemalloc, gc
from dask.distributed import Client, performance_report
from memory_profiler import memory_usage
from line_profiler import LineProfiler

from src.analysis.processor import Processor
from config.customEvtSel import switch_selections
from config.customProc import switch_processors
from src.utils.memoryutil import analyze_memory_status, force_release_memory
from src.utils.ioutil import setup_logging, check_open_files
from dask import config

pjoin = os.path.join

def main():
    # Force synchronous scheduler for debugging
    # config.set(scheduler='threads')
    # config.set(schedule='synchronous')
    # logging.debug("Dask config not explicitly set")
    # logging.debug("Set Dask to synchronous scheduler")

    parser = argparse.ArgumentParser(description='Debug processor on a single file')
    parser.add_argument('selection_name', type=str, help='Name of the selection to run')
    parser.add_argument('processor_name', type=str, help='Name of the processor to run')
    parser.add_argument('--profile', choices=['memory', 'line'], default='line',
                        help='Type of profiling to perform (memory or line)')

    args = parser.parse_args()
    logging.info(f"Running with selection: {args.selection_name}")

    file_dir = os.path.dirname(os.path.realpath(__file__))
    testinput = pjoin(file_dir, "testInputs", "custom_SKIM.json")
    with open(testinput, 'r') as f:
        preprocessed = json.load(f)
    
    logging.info(f"Loaded test input with {len(preprocessed['files'])} files")

    rtcfg_1 = {
        "OUTPUTDIR_PATH": "/uscms/home/joyzhou/nobackup/tests",
        "COPYDIR_PATH": "/uscms/home/joyzhou/nobackup/temp",
        "TRANSFER_PATH": "/store/user/joyzhou/temp",
        "DELAYED_OPEN": True,
        "REMOTE_LOAD": False,
        "FILTER_NAME": None,
        "DELAYED_WRITE": False,
    }

    eventselection = switch_selections(args.selection_name)
    processor_class = switch_processors(args.processor_name)

    transferP = "/store/user/joyzhou/temp"

    tracemalloc.start()
    logging.info("Started tracemalloc")

    proc = processor_class(rtcfg_1, preprocessed, transferP=transferP, evtselclass=eventselection)

    profiler = cProfile.Profile()
    profiler.enable()

    start_time = time.time()
    
    initial_memory = memory_usage(-1, interval=.1, timeout=1)[0]
    logging.info(f"Initial memory usage: {initial_memory} MiB")

    try:
        cpu_count = os.cpu_count()
        logging.debug("CPU count: %d", cpu_count)
        
        readkwargs = {'filter_name': ["Tau*", "Jet*", "Electron*", "Muon*", "Gen*", "LHE*", "HLT*", "MET"]}
        rc = proc.run(readkwargs=readkwargs)
        end_exec_time = time.time()
        logging.warning(f"Finished processing events in {(end_exec_time-start_time)/60:.2f} minutes")
    except Exception as e:
        logging.error(f"Error encountered: {str(e)}")
        raise
    finally:
        gc.collect()

        count, files = check_open_files()
        if count > 0:
            logging.warning(f"Found {count} open files: {files}")
            for file in files:
                logging.warning(f"File {file} still open.")

        post_gc_memory = memory_usage(-1, interval=.1, timeout=1)[0]
        logging.warning(f"Memory after garbage collection: {post_gc_memory} MiB")

        logging.warning("Analyzing remaining objects...")
        analyze_memory_status(use_pympler=True)

        del proc
        post_proc_memory = memory_usage(-1, interval=.1, timeout=1)[0]
        logging.warning(f"Memory after Processor deletion: {post_proc_memory} MiB")

    profiler.disable()

    # Write profiling results
    stats_filename = 'cprofile_output.txt'
    with open(stats_filename, 'w') as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()
    
    start_time = time.time()
    
    force_release_memory()
    post_release_memory = memory_usage(-1, interval=.1, timeout=1)[0]
    logging.warning(f"Memory after forced memory release: {post_release_memory} MiB")

    end_time = time.time()
    logging.warning(f"Finished releasing memory in {(end_time-start_time)/60:.2f} minutes")
    
    
if __name__ == '__main__':
    setup_logging()
    
    # Set up line profiler
    lp = LineProfiler()
    # Add the functions you want to profile
    lp.add_function(Processor.run)

    # Run the profiled version
    lp_wrapped = lp(main)
    lp_wrapped()

    # Write line profiler results
    lp_filename = 'line_profiler_output.txt'
    with open(lp_filename, 'w') as f:
        lp.print_stats(stream=f)


# ... existing code...