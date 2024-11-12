from config.projectconfg import cleansetting
from src.plotting.postprocessor import PostProcessor
import argparse, contextlib

@contextlib.contextmanager
def silence_output(file_path):
    with open(file_path, "w") as f:
        with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            yield

def __main__():
   description = """
   This script is a postprocessor for handling ROOT files. It supports various modes of operation:
   
   - check: Check the integrity of ROOT files in the specified groups.
   - hadd: Merge (hadd) ROOT files in the specified groups.
   - clean: Clean corrupted ROOT files.
   - yield: Calculate the yields from the ROOT files.

   Usage Examples:
   
   1. Check the integrity of ROOT files:
      python postprocess.py --mode check --group DYJets TTbar --year 2022PostEE
      
      Check all existing ROOT files:
      python postprocess.py --mode check

   2. Merge ROOT files:
      python postprocess.py --mode hadd --group DYJets TTbar

   3. Clean corrupted ROOT files:
      python postprocess.py --mode clean --group DYJets TTbar

   4. Calculate yields:
      python postprocess.py --mode yield --group DYJets TTbar
   """

   parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

   parser.add_argument('--mode', choices=['check', 'hadd', 'clean', 'yield'], required=True, 
                     help='Choose the mode to run the postprocessor. Check the roots, hadd the files, clean the (corrupted) files, or get the yields.')
   parser.add_argument('--group', type=str, nargs='+', required=False, default=None, 
                     help='Group of the files to be hadded, e.g. DYJets TTbar etc. If not provided, will postprocess all groups.')
   parser.add_argument('--year', type=str, nargs='+', required=False, default=None, 
                       help='Year of the files to be hadded, e.g. 2022PostEE, 2023 etc. If not provided, will postprocess all years.')
   parser.add_argument("--silence", action="store_true", help="Silence all output to console.")

   args = parser.parse_args()
   
   pp = PostProcessor(cleansetting, groups=args.group, years=args.year)

   if args.mode == 'check':
      pp.check_roots()

   if args.mode == 'hadd':
      pp()
   
   if args.mode == 'clean':
      pp.clean_roots()
   
   if args.mode == 'yield':
      pp.get_yield()
        
if __name__ == '__main__':
   __main__()