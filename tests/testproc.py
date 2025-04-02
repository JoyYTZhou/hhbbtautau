import os, json, cProfile, time, pstats, logging, tracemalloc, gc, sys
from dask.distributed import Client, performance_report
from memory_profiler import memory_usage
from line_profiler import LineProfiler

from src.analysis.processor import Processor
from config.customEvtSel import switch_selections
from config.customProc import switch_processors
from src.utils.memoryutil import analyze_memory_status, force_release_memory
from src.utils.ioutil import setup_logging, check_open_files
from dask import config
from src.utils.displayutil import RichArgumentParser, visualize_csv

pjoin = os.path.join

def get_test_config():
    """Return standard test configuration"""
    return {
        "OUTPUTDIR_PATH": "/uscms/home/joyzhou/nobackup/tests",
        "COPYDIR_PATH": "/uscms/home/joyzhou/nobackup/temp",
        "TRANSFER_PATH": "/store/user/joyzhou/temp",
        "FILTER_NAME": None,
        "DELAYED_WRITE": False,
    }
    
def load_test_input(processor_name, file_dir):
    """Load test input file based on processor name"""
    if processor_name == 'preselect':
        testinput = pjoin(file_dir, "testInputs", "custom_SKIM.json")
    else:
        testinput = pjoin(file_dir, "testInputs", "DYJets_NANOAOD12_2.json")
    
    with open(testinput, 'r') as f:
        return json.load(f)
        
def run_basic_test(selection_name, processor_name, transfer_files=True):
    """Run basic test without profiling

    Args:
        selection_name (str): Name of the selection to run
        processor_name (str): Name of the processor to run
        transfer_files (bool): Whether to transfer output files to remote storage.
                             If False, files remain local for visualization.
    """
    file_dir = os.path.dirname(os.path.realpath(__file__))
    preprocessed = load_test_input(processor_name, file_dir)
    rtcfg = get_test_config()
    
    logging.info(f"Running basic test with selection: {selection_name}")
    logging.info(f"Loaded test input with {len(preprocessed['files'])} files")

    eventselection = switch_selections(selection_name)
    processor_class = switch_processors(processor_name)
    
    # Set transfer path to None if transfer_files is False
    transfer_path = "/store/user/joyzhou/temp" if transfer_files else None
    proc = processor_class(rtcfg, preprocessed, transferP=transfer_path,
                         evtselclass=eventselection)
    
    logging.info("Processor class: %s", processor_class)
    logging.info("Event selection class: %s", eventselection)
    logging.info("File transfer mode: %s", "enabled" if transfer_files else "disabled")
    
    readkwargs = {'filter_name': ["Tau*", "Jet*", "Electron*", "Muon*", "Gen*", "LHE*", "HLT*", "MET"]}
    try:
        rc = proc.run(readkwargs=readkwargs)
        logging.info("Basic test completed successfully")

        # If files are not transferred, visualize the CSV files in output directory
        if not transfer_files:
            output_dir = pjoin(rtcfg['OUTPUTDIR_PATH'], preprocessed['metadata']['shortname'])
            logging.info(f"Visualizing CSV files in {output_dir}")
            for file in os.listdir(output_dir):
                if file.endswith('cutflow.csv'):
                    csv_path = os.path.join(output_dir, file)
                    visualize_csv(csv_path, title=file, max_rows=10)
    finally:
        del proc
        gc.collect()

def run_profiled_test(selection_name, processor_name, profile_type):
    """Run test with profiling"""
    file_dir = os.path.dirname(os.path.realpath(__file__))
    preprocessed = load_test_input(processor_name, file_dir)
    rtcfg = get_test_config()
    
    tracemalloc.start()
    logging.info(f"Running profiled test ({profile_type}) with selection: {selection_name}")

    eventselection = switch_selections(selection_name)
    processor_class = switch_processors(processor_name)
    
    proc = processor_class(rtcfg, preprocessed, transferP="/store/user/joyzhou/temp", 
                         evtselclass=eventselection)

    profiler = cProfile.Profile()
    profiler.enable()
    
    initial_memory = memory_usage(-1, interval=.1, timeout=1)[0]
    start_time = time.time()
    
    readkwargs = {'filter_name': ["Tau*", "Jet*", "Electron*", "Muon*", "Gen*", "LHE*", "HLT*", "MET"]}
    try:
        rc = proc.run(readkwargs=readkwargs)
        end_exec_time = time.time()
        logging.warning(f"Processing time: {(end_exec_time-start_time)/60:.2f} minutes")
    finally:
        # Memory analysis
        gc.collect()
        count, files = check_open_files()
        if count > 0:
            logging.warning(f"Found {count} open files")
        
        post_gc_memory = memory_usage(-1, interval=.1, timeout=1)[0]
        analyze_memory_status(use_pympler=True)
        
        del proc
        force_release_memory()
        
        # Save profiling results
        profiler.disable()
        with open('cprofile_output.txt', 'w') as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats(pstats.SortKey.TIME)
            stats.print_stats()

def main():
    setup_logging()

    parser = RichArgumentParser(description="Test Processor on a single input json file")

    parser.add_argument('selection_name', type=str, help='Name of the selection to run')
    parser.add_argument('processor_name', type=str, help='Name of the processor to run (skim/preselect)')
    parser.add_argument('--profile', choices=['memory', 'line', 'none'], default='none',
                        help='Type of profiling to perform (memory, line, or none)')
    parser.add_argument('--transfer-files', action='store_true',
                        help='Transfer output files to remote storage. If disabled, will visualize cutflow locally.')

    args = parser.parse_args()
   
    if args.profile == 'line':
        # Set up line profiler
        lp = LineProfiler()
        lp.add_function(Processor.run)

        # Wrap and run the test function
        def run_test():
            run_basic_test(args.selection_name, args.processor_name)
        lp_wrapped = lp(run_test)
        lp_wrapped()

        # Save line profiler results
        with open('line_profiler_output.txt', 'w') as f:
            lp.print_stats(stream=f)

    elif args.profile == 'memory':
        run_profiled_test(args.selection_name, args.processor_name, args.profile)

    else:  # args.profile == 'none'
        run_basic_test(args.selection_name, args.processor_name, args.transfer_files)

if __name__ == '__main__':
    main()
