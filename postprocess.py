from src.plotting.summary import PostSkimProcessor, PostPreselProcessor
import contextlib, logging, os
from src.utils.ioutil import setup_logging
from src.utils.displayutil import RichArgumentParser, create_table

@contextlib.contextmanager
def silence_output(file_path):
    with open(file_path, "w") as f:
        with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            yield
   
luminosity = {"2022PreEE": 41.5/2 * 1000, "2022PostEE": 41.5 * 1000/2, "2023Summer": 32.7 * 1000}

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
            "cmd": "python postprocess.py -d vbfskim --mode check --group DYJets TTbar --year 2022PostEE -m",
            "desc": "Check integrity of MC ROOT files for specific groups and year"
        },
        {
            "cmd": "python postprocess.py -d vbfskim --mode check --year 2022PostEE",
            "desc": "Check integrity of Data ROOT files for a specific year"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode hadd --year 2022PostEE -m",
            "desc": "Merge ROOT/CSV files for all MC groups and a specific year"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode hadd --year 2022PostEE",
            "desc": "Merge ROOT/CSV files for specific Data groups and year"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode clean --group DYJets TTbar -m",
            "desc": "Clean corrupted MC ROOT files"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode clean --group SingleMuon",
            "desc": "Clean corrupted Data ROOT files"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode yield --group DYJets TTbar -m",
            "desc": "Calculate yields for specific MC groups"
        },
        {
            "cmd": "python postprocess.py -d tightskim --mode yield --group SingleMuon",
            "desc": "Calculate yields for specific Data groups"
        }
    ]

   parser = RichArgumentParser(
      description=description,
      examples=examples
   )
   parser.add_argument('-d', '--dirname', type=str, required=True,
                        help='Directory containing the output and cutflow files to process')
   parser.add_argument('--mode', choices=['check', 'hadd', 'clean', 'yield'], required=True, 
                        help='Choose the mode to run the postprocessor.')
   parser.add_argument('--group', type=str, nargs='+', required=False, default=None, 
                        help='Group of files to process. If not provided, will process all groups.')
   parser.add_argument('--year', type=str, nargs='+', required=False, default=None, 
                        help='Year of files to process. If not provided, will process all years.')
   parser.add_argument('-q', '--quiet', action='store_true', help='Suppress all output')
   parser.add_argument('--debug', action='store_true', help='Set logging to debug level')
   parser.add_argument('-m', '--is_mc', action='store_true', help='Process MC files')

   args = parser.parse_args()

   cleansetting = {"DIRNAME": args.dirname, "DATA_DIR": DATA_DIR, 
                   "INPUTDIR": os.path.join(CONDOR_BASE, args.dirname),
                   "LOCALOUTPUT": f"/uscms/home/{USER}/nobackup/hadded/{args.dirname}",
                   "TRANSFERPATH": os.path.join(CONDOR_BASE, f"{args.dirname}_hadded"), 
                   "IS_MC": args.is_mc}
   
   create_table(cleansetting, "PostProcessor Settings")

   if args.quiet:
      console_level = logging.WARNING
      file_level = logging.INFO
   else:
      console_level = logging.INFO
      file_level = logging.DEBUG
   
   if args.debug:
      console_level = logging.DEBUG
      file_level = logging.DEBUG

   setup_logging(console_level=console_level, 
               file_level=file_level,
               log_to_file=True)
   
   groups = args.group
   years = args.year
   
   if groups is not None:
      if groups[0].lower() == 'all':
         groups = None
   
   if years is not None:
      if years[0].lower() == 'all':
         years = None
   
   if 'skim' in args.dirname.lower():
      is_skim = True
   else: 
      is_skim = False

   if is_skim:
      pp = PostSkimProcessor(cleansetting, luminosity, groups=groups, years=years)
   else:
      pp = PostPreselProcessor(cleansetting, luminosity, groups=groups, years=years)
   
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