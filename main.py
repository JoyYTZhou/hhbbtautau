import os, gc, argparse

PARENT_DIR = os.path.dirname(__file__) 
from src.utils.filesysutil import checkx509, display_top
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
        import tracemalloc
        tracemalloc.start()

    checkx509()
    
    selectionclass = switch_selections(runsetting.SEL_NAME)

    jr = JobRunner(runsetting, args.input, selectionclass, dasksetting)
    print("======================================================================")
    print("Enter Main Python program: Event selection Mode!")
    print("======================================================================")
    jr.submitjobs(client=None, parquet=True)
    
    if args.diagnose:
        snapshot = tracemalloc.take_snapshot()
        display_top(snapshot)

if __name__ == '__main__':
    runselections()