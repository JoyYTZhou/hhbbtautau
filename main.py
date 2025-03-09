import os, gc, argparse, logging, time

PARENT_DIR = os.path.dirname(__file__) 
from src.utils.filesysutil import checkx509
from src.utils.ioutil import setup_logging
from src.utils.memoryutil import analyze_memory_status
from config.projectconfg import dasksetting, runsetting
from config.customEvtSel import switch_selections

def runselections():
    gc.enable()
    from src.analysis.spawnjobs import JobRunner

    parser = argparse.ArgumentParser(
            description='''Run event selections for data analysis.

    This script performs event selections based on the specified configuration and input file.
    It supports optional memory diagnostics to help identify memory usage issues.

    Arguments:
    - --input: Path to the input file containing data to be processed. See example input files in example/ directory.
    - --diagnose: Enable memory diagnostics to track memory usage during execution.
            '''
        )
    parser.add_argument('--input', type=str, help='input file path', default=None)
    parser.add_argument('--diagnose', action='store_true', default=False, help='Enable memory diagnose')
    args = parser.parse_args()
    
    if args.diagnose:
        setup_logging(console_level=logging.DEBUG, log_to_file=False)
        start_time = time.time()

    checkx509()
    
    selectionclass = switch_selections(runsetting.SEL_NAME)

    jr = JobRunner(runsetting, args.input, selectionclass, dasksetting)
    print("======================================================================")
    print("Enter Main Python program: Event selection Mode!")
    print("======================================================================")
    jr.submitjobs(client=None, proc_kwargs={})

    if args.diagnose:
        end_time = time.time()
        logging.warning(f"Finished processing events in {(end_time-start_time)/60:.2f} minutes")
        logging.debug("Analyzing memory usage and debug potential memory leak...")
        analyze_memory_status(use_pympler=True)


if __name__ == '__main__':
    runselections()