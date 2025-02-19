import os, json, cProfile, argparse, time, pstats, logging
from memory_profiler import memory_usage, profile
from line_profiler import LineProfiler

from src.analysis.processor import Processor
from config.customEvtSel import switch_selections

pjoin = os.path.join

def setup_logging():
    logging.basicConfig(filename='debug.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s:%(message)s')
@profile
def main():
    parser = argparse.ArgumentParser(description='Run processor on a single file')
    parser.add_argument('selection_name', type=str, help='Name of the selection to run')

    args = parser.parse_args()
    
    file_dir = os.path.dirname(os.path.realpath(__file__))
    testinput = pjoin(file_dir, "testInputs", "DYJets_NANOAOD12.json")
    with open(testinput, 'r') as f:
        preprocessed = json.load(f)
    
    rtcfg_1 = {
        "OUTPUTDIR_PATH": "/uscms/home/joyzhou/nobackup/tests",
        "COPYDIR_PATH": "/store/user/joyzhou/temp",
        "DELAYED_OPEN": True,
        "REMOTE_LOAD": True,
        "FILTER_NAME": None,
        "DELAYED_WRITE": False,
    }
    
    eventselection = switch_selections(args.selection_name)
    transferP = "/store/user/joyzhou/temp"

    proc = Processor(rtcfg_1, preprocessed, transferP=transferP, evtselclass=eventselection)

    profiler = cProfile.Profile()
    profiler.enable()

    start_time = time.time()
    failed_files = None

    try:
        logging.debug("Starting processing...")
        failed_files = proc.runfiles(write_npz=False)
        logging.debug("Processing completed.")
    except Exception as e:
        logging.error(f"Error encountered: {e}")
    end_time = time.time()

    profiler.disable()

    stats_filename = 'cprofile_output.txt'
    with open(stats_filename, 'w') as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()

    print(f"Processing completed in {(end_time-start_time)/60:.2f} minutes")
    print(f"Failed files: {failed_files}")

    mem_usage_filename = 'memory_usage_output.txt'
    with open(mem_usage_filename, 'w') as f:
        mem_usage = memory_usage(proc=run_with_memory_profiler, interval=0.1, include_children=True, multiprocess=True)
        f.write("Memory usage (MB):\n")
        for mem in mem_usage:
            f.write(f"{mem}\n")
        f.write("\n")

@profile
def run_with_memory_profiler():
    main()

if __name__ == '__main__':
    setup_logging()
    lp = LineProfiler()
    lp.add_function(Processor.runfiles)
    lp.add_function(Processor.loadfile_remote)
    lp.add_function(Processor.loadfile_local)
    lp.add_function(Processor.writeevts)
    lp.add_function(Processor.writedask)
    lp.add_function(Processor.writeak)
    lp.add_function(Processor.writedf)
    lp.add_function(Processor.writepickle)

    lp.enable_by_count()
    run_with_memory_profiler()
    lp.disable_by_count()

    lp_filename = 'line_profiler_output.txt'
    with open(lp_filename, 'w') as f:
        lp.print_stats(stream=f)
