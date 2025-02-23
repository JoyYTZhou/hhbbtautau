import os, json, cProfile, argparse, time, pstats, logging, tracemalloc
from dask.distributed import Client, performance_report
from memory_profiler import memory_usage
from line_profiler import LineProfiler
import gc

from tests.debug_processor import DebugProcessor  # Import the debug version
from config.customEvtSel import switch_selections
from dask import config

pjoin = os.path.join

def setup_logging():
    logging.basicConfig(filename='debug.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s:%(message)s')

def log_memory_snapshot(snapshot, message):
    top_stats = snapshot.statistics('lineno')
    logging.debug(f"Memory snapshot: {message}")
    for stat in top_stats[:10]:
        logging.debug(stat)

def main():
    config.set(scheduler='synchronous')

    parser = argparse.ArgumentParser(description='Run processor on a single file')
    parser.add_argument('selection_name', type=str, help='Name of the selection to run')
    parser.add_argument('--profile', choices=['memory', 'line'], default='line',
                        help='Type of profiling to perform (memory or line)')

    args = parser.parse_args()

    file_dir = os.path.dirname(os.path.realpath(__file__))
    testinput = pjoin(file_dir, "testInputs", "DYJets_NANOAOD12.json")
    with open(testinput, 'r') as f:
        preprocessed = json.load(f)

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
    transferP = "/store/user/joyzhou/temp"

    tracemalloc.start()

    proc = DebugProcessor(rtcfg_1, preprocessed, transferP=transferP, evtselclass=eventselection)

    profiler = cProfile.Profile()
    profiler.enable()

    start_time = time.time()
    failed_files = None

    # Record initial memory usage
    initial_memory = memory_usage(-1, interval=.1, timeout=1)[0]
    logging.debug(f"Initial memory usage: {initial_memory} MiB")

    # Take initial tracemalloc snapshot
    snapshot1 = tracemalloc.take_snapshot()
    log_memory_snapshot(snapshot1, "Initial snapshot")

    try:
        logging.debug("Starting processing...")
        failed_files = proc.run_skims(write_npz=False)
        logging.debug("Processing completed.")
    except Exception as e:
        logging.error(f"Error encountered: {e}")
        raise e
    finally:
        # Take final tracemalloc snapshot
        snapshot2 = tracemalloc.take_snapshot()
        log_memory_snapshot(snapshot2, "Final snapshot")

        # Compare snapshots
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        logging.debug("[ Top 10 differences ]")
        for stat in top_stats[:10]:
            logging.debug(stat)

        # Record final memory usage
        final_memory = memory_usage(-1, interval=.1, timeout=1)[0]
        logging.debug(f"Final memory usage: {final_memory} MiB")
        logging.debug(f"Memory difference: {final_memory - initial_memory} MiB")

        # Force garbage collection
        gc.collect()
        post_gc_memory = memory_usage(-1, interval=.1, timeout=1)[0]
        logging.debug(f"Memory after garbage collection: {post_gc_memory} MiB")

    end_time = time.time()

    profiler.disable()

    stats_filename = 'cprofile_output.txt'
    with open(stats_filename, 'w') as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()

    print(f"Processing completed in {(end_time-start_time)/60:.2f} minutes")
    print(f"Failed files: {failed_files}")
       
if __name__ == '__main__':
    setup_logging()
    # Set up line profiler
    lp = LineProfiler()
    lp.add_function(DebugProcessor.run_skims)
    # lp.add_function(DebugProcessor.pipeline_files)
    lp.add_function(DebugProcessor.writeevts)
    lp.add_function(DebugProcessor.writedask)
    # lp.add_function(DebugProcessor.writeak)

    # Run the profiled version
    lp_wrapped = lp(main)
    lp_wrapped()

    # Write line profiler results
    lp_filename = 'line_profiler_output.txt'
    with open(lp_filename, 'w') as f:
        lp.print_stats(stream=f)
