import os, gc, argparse

PARENT_DIR = os.path.dirname(__file__) 
from src.utils.filesysutil import checkx509, display_top
from config.projectconfg import dasksetting, namemap, runsetting, selection
from config.customEvtSel import switch_selections

def runselections():
    gc.enable()
    from src.analysis.spawnjobs import JobRunner

    parser = argparse.ArgumentParser(description='Event selection options')
    parser.add_argument('--input', type=str, help='input file path', default=None)
    parser.add_argument('--diagnose', action='store_true', default=False, help='Enable memory diagnose')
    args = parser.parse_args()
    
    if args.diagnose:
        import tracemalloc
        tracemalloc.start()

    checkx509()
    
    selectionclass = switch_selections(runsetting.SEL_NAME)

    jr = JobRunner(runsetting, args.input, selectionclass, dasksetting)
    jr.submitjobs(client=None)
    
    if args.diagnose:
        snapshot = tracemalloc.take_snapshot()
        display_top(snapshot)

if __name__ == '__main__':
    runselections()