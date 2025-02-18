import os, json, cProfile, argparse, time, pstats
from collections import defaultdict
from memory_profiler import profile
from line_profiler import LineProfiler

from src.analysis.processor import Processor
from config.customEvtSel import switch_selections

pjoin = os.path.join

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
    failed_files = proc.runfiles(write_npz=False)
    end_time = time.time()

    profiler.disable()

    stats = pstats.Stats(profiler)
    stats.sort_stats(pstats.SortKey.TIME)

    stats_filename = 'cprofile_output.txt'
    with open(stats_filename, 'w') as f:
        stats.print_stats(stream=f)

    print(f"Processing completed in {(end_time-start_time)/60:.2f} minutes")
    print(f"Failed files: {failed_files}")

@profile
def run_with_memory_profiler():
    main()

if __name__ == '__main__':
    lp = LineProfiler()
    lp.add_function(run_with_memory_profiler)
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
