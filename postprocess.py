from src.plotting.summary import PostProcessor, PostSkimProcessor
from src.plotting.summary import PostProcessor, PostSkimProcessor
import contextlib, logging, os
from src.utils.ioutil import setup_logging
from src.utils.displayutil import RichArgumentParser, create_table

@contextlib.contextmanager
def silence_output(file_path):
    with open(file_path, "w") as f:
        with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            yield
   
luminosity = {"2022PostEE": 41.5 * 1000, "2023Summer": 32.7 * 1000}

DATA_DIR = os.environ.get('DATA_DIR', None)
if DATA_DIR is None:
   raise EnvironmentError("DATA_DIR environment variable not set. Please set it to the data directory.")

CONDOR_BASE = os.environ.get('CONDOR_BASE', None)
if CONDOR_BASE is None:
   raise EnvironmentError("CONDOR_BASE environment variable not set. Please set it to the condor base directory.")

USER = os.environ.get('USER', None)
if USER is None:
   raise EnvironmentError("USER environment variable not set. Please set it to the user directory.")

def __main__():
   description = """
    Postprocessor for handling ROOT files with various operational modes:
   
    Supports checking file integrity, merging files, cleaning corrupted files,
    and calculating yields across different data groups and years.
   """

   examples = [
        {
            "cmd": "python postprocess.py --mode check --group DYJets TTbar --year 2022PostEE",
            "desc": "Check integrity of ROOT files for specific groups and year"
        },
        {
            "cmd": "python postprocess.py --mode hadd --group DYJets TTbar --year 2022PostEE",
            "desc": "Merge ROOT/CSV files for specific groups and year"
        },
        {
            "cmd": "python postprocess.py --mode clean --group DYJets TTbar",
            "desc": "Clean corrupted ROOT files"
        },
        {
            "cmd": "python postprocess.py --mode yield --group DYJets TTbar",
            "desc": "Calculate yields for specific groups"
        }
    ]

   parser = RichArgumentParser(
      description=description,
      examples=examples
   )
   parser.add_argument('--dirname', type=str, required=True, 
                        help='Directory containing the output and cutflow files to process')
   parser.add_argument('--mode', choices=['check', 'hadd', 'clean', 'yield'], required=True, 
                        help='Choose the mode to run the postprocessor.')
   parser.add_argument('--group', type=str, nargs='+', required=False, default=None, 
                        help='Group of files to process. If not provided, will process all groups.')
   parser.add_argument('--year', type=str, nargs='+', required=False, default=None, 
                        help='Year of files to process. If not provided, will process all years.')
   parser.add_argument('--quiet', '-q', action='store_true', help='Suppress all output')
   parser.add_argument('--debug', '-d', action='store_true', help='Set logging to debug level')
   parser.add_argument('--skim', '-s', action='store_true', help='Postprocess skimmed files')
   parser.add_argument('--is_mc', '-m', action='store_true', help='Process MC files')

   args = parser.parse_args()

   cleansetting = {"DIRNAME": args.dirname, "DATA_DIR": DATA_DIR, 
                   "INPUTDIR": os.path.join(CONDOR_BASE, args.dirname),
                   "LOCALOUTPUT": f"/uscms/home/{USER}/nobackup/hadded/{args.dirname}",
                   "TRANSFERPATH": f"/uscms/home/{USER}/nobackup/{args.dirname}_hadded", 
                   "IS_MC": args.is_mc}
   
   create_table(cleansetting, "PostProcessor Settings")

   if args.quiet:
      console_level = logging.ERROR
   else:
      console_level = logging.INFO
   
   if args.debug:
      console_level = logging.DEBUG
   
   setup_logging(console_level=console_level, 
                 file_level=logging.DEBUG,
                 log_to_file=True)
   
   if args.skim:
      pp = PostSkimProcessor(cleansetting, luminosity, groups=args.group, years=args.year)
   else:
      pp = PostProcessor(cleansetting, luminosity, groups=args.group, years=args.year)
   
   if args.mode == 'check':
      pp.check_results()

   if args.mode == 'hadd':
      pp.hadd_results()
   
   if args.mode == 'clean':
      pp.clean_results()
   
   if args.mode == 'yield':
      pp.get_yield()
        
if __name__ == '__main__':
   __main__()