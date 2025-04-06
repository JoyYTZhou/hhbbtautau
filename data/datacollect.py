#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, shutil, argparse, re, gzip, logging
from rich.console import Console
from rich.table import Table
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI
from src.utils.filesysutil import FileSysHelper, pjoin
from src.utils.displayutil import RichArgumentParser
import warnings

warnings.filterwarnings("ignore", module="coffea*")
warnings.filterwarnings("ignore", module="numba.*")

mc_dir = "availableMC"
data_dir = "availableData"

class QueryRunner:
    """Class to run the query on dataset strings and preprocess the dataset.
    Currently only supports MC datasets. Dependent on DataDiscoveryCLI from coffea."""
    def __init__(self, dataset, year, is_mc, out_path, skip_choose) -> None:
        """Initialize the QueryRunner object.
        
        Parameters
        - `dataset`: str, the dataset name key in the json file to query and preprocess.
                    If None, query over all keys in input files.
        - `year`: str, the year to process (e.g., '2022', '2023'). If None, process all years.
        - `is_mc`: bool, whether to process Monte Carlo (True) or collision data (False)
        """
        # Initialize DAS client
        self.ddc = DataDiscoveryCLI()
        self.ddc.do_regex_sites(r"T[123]_(US)_\w+")

        # Set instance variables
        self._isMC = is_mc
        self.year = year

        # Get input directory and files
        base_dir = mc_dir if is_mc else data_dir

        if year:
            # Single year processing
            json_files = {year: pjoin(base_dir, f"{year}.json")}
        else:
            # Multi-year processing
            json_files = {
                f.split('/')[-1].split('.')[0]: f
                for f in FileSysHelper.glob_files(base_dir, "*.json")
            }

        # Load JSON data
        self.mcstrings = {
            year: self._load_json(filename)
            for year, filename in json_files.items()
        }

        self.display_mcstrings()

        self.dataset = dataset

        self.outpath = out_path

        self.skip_choose = skip_choose
        
        FileSysHelper.checkpath(self.outpath, createdir=True)

    def display_mcstrings(self):
        """Display mcstrings data in a formatted rich table."""
        console = Console()

        # Create table
        table = Table(title="Dataset Information")

        # Add columns
        table.add_column("Year", style="cyan")
        table.add_column("Dataset", style="green")
        table.add_column("Sample Name", style="yellow")
        table.add_column("Short Name", style="magenta")
        table.add_column("Sample Type", style="blue")

        # Add rows
        for year, year_data in self.mcstrings.items():
            for dataset, samples in year_data.items():
                for sample_name, details in samples.items():
                    table.add_row(
                        str(year),
                        str(dataset),
                        str(sample_name),
                        str(details.get('shortname', 'N/A')),
                        'MC' if self._isMC else 'Data'
                    )

        # Print table
        console.print(table)
        
    @staticmethod
    def _load_json(filepath):
        """Helper method to load JSON file"""
        with open(filepath, 'r') as f:
            return json.load(f)

    def __call__(self, query_dir=None) -> None:
        """Run the query on the dataset and preprocess the dataset."""
        if query_dir is None:
            self.query_from_dasgo()
        else:
            FileSysHelper.checkpath(query_dir, createdir=False, raiseError=True)
            for year, year_data in self.mcstrings.items():
                year_dir = pjoin(query_dir, year)
                if FileSysHelper.checkpath(year_dir, createdir=False, raiseError=False):
                    datasets_to_process = [self.dataset] if self.dataset else year_data.keys()
                    for dataset in datasets_to_process:
                        if FileSysHelper.checkpath(pjoin(year_dir, dataset), createdir=False, raiseError=False):
                            self.query_from_dir(query_dir, dataset, year, self.outpath)
                        else:
                            logging.warning(f"No custom skims for {year} {dataset} have been produced.")
                else:
                    logging.warning(f"No custom skims for {year} have been produced.")
    
    def __add_MC_meta(self):
        """Add 'is_mc' metadata flag to MC datasets.
        If self.dataset is specified, only add to that dataset, otherwise add to all datasets."""
        if not self._isMC:
            return

        # Iterate through all years
        for year, year_data in self.mcstrings.items():
            # If dataset is specified, only process that dataset
            datasets_to_process = [self.dataset] if self.dataset else year_data.keys()

            # Process each dataset
            for dataset in datasets_to_process:
                # Skip if dataset doesn't exist in this year
                if dataset not in year_data:
                    continue

                # Add is_mc flag to each dataset name entry
                for datasetname in year_data[dataset]:
                    year_data[dataset][datasetname]["is_mc"] = True

    
    def query_from_dasgo(self) -> None:
        """Query the available files from the DASGO. Produce a json.gz file with the query results (files, redirectors, uuids etc.)"""
        self.__add_MC_meta()
        if self.skip_choose:
            strategy = 'round-robin'
        else:
            strategy = 'manual'
        for year, year_data in self.mcstrings.items():
            suffix = year
            
            if self.dataset is not None:
                # If dataset is specified, only process that dataset
                datasets_to_process = [self.dataset]
            else:
                # Process all datasets in this year
                datasets_to_process = year_data.keys()
            
            # If not MC, collect all datasets to process together
            if not self._isMC:
                # Collect all datasets in the year
                datasets_to_process = year_data.keys()

                # Load all dataset definitions
                for dataset in datasets_to_process:
                    self.ddc.load_dataset_definition(dataset_definition=year_data[dataset], query_results_strategy='all', replicas_strategy=strategy)

                # Preprocess all data datasets together
                out_name = f"Data_{suffix}"
                self.ddc.do_preprocess(output_file=out_name,
                    step_size=80000,
                    align_to_clusters=False,
                    recalculate_steps=False,
                    files_per_batch=1,
                    file_exceptions=(OSError, IndexError),
                    save_form=False,
                    allow_empty_datasets=True,
                    scheduler_url=None)
            
                shutil.move(f"{out_name}_available.json.gz", f"preprocessed/{out_name}.json.gz")
        
            # If MC, process each dataset separately
            else:
                for dataset in datasets_to_process:
                    # Reset DataDiscoveryCLI for each dataset
                    self.ddc = DataDiscoveryCLI()
                    self.ddc.do_regex_sites(r"T[123]_(US)_\w+")

                    # Load dataset definition
                    self.ddc.load_dataset_definition(dataset_definition=year_data[dataset], query_results_strategy='all', replicas_strategy=strategy)

                    # Preprocess the dataset
                    out_name = f'{dataset}_{suffix}'
                    self.ddc.do_preprocess(output_file=out_name,
                        step_size=80000,
                        align_to_clusters=False,
                        recalculate_steps=False,
                        files_per_batch=1,
                        file_exceptions=(OSError, IndexError),
                        save_form=False,
                        allow_empty_datasets=True,
                        scheduler_url=None)

                    shutil.move(f"{out_name}_available.json.gz", f"preprocessed/{out_name}.json.gz")
    
    def query_from_dir(self, query_dir, dataset, year, outpath) -> None:
        """Query the available files from the query_dir, e.g. a directory containing custom skim files. 
        Right now this does not do preprocessing.
        
        Parameters
        - `query_dir`: str, the directory containing the custom skim files (currently only supports root files)
        """
        queryed_result = {}

        pattern = re.compile(r'_(\d+)\.root$')

        sub_dictionary = self.mcstrings[year][dataset] 

        for datasetname in sub_dictionary.keys():
            queryed_result[datasetname] = {"files": {}}
            queryed_result[datasetname]["metadata"] = sub_dictionary[datasetname]
            queryed_result[datasetname]["metadata"]["is_mc"] = self._isMC
            shortname = sub_dictionary[datasetname]['shortname'] 
            root_files = FileSysHelper.glob_files(pjoin(query_dir, year, dataset), f'{shortname}*.root')
            for root_file in root_files:
                match = pattern.search(root_file)
                if match:
                    index = match.group(1)
                    queryed_result[datasetname]["files"][root_file] = {"uuid": index, "object_path": "Events"}

        with gzip.open(f"{outpath}/{dataset}_{year}.json.gz", 'wt') as file:
            json.dump(queryed_result, file)

if __name__ == "__main__":
    program_description = """Run the preprocessor on the dataset strings in the json file.
    The preprocessor will query the dataset strings and preprocess the dataset.
    If the --skip flag is set, the program will only dump the query results to a json file.
    If the --query flag is set, the program will query the custom skims in the directory.
    """
    parser = RichArgumentParser(description=program_description)

    parser.add_argument('-d', '--dataset', type=str, required=False, default=None,
                        help='group name of the dataset to run program on, e.g. TTbar, DYJets, etc. Note that this must match the key in the json input file.')
    parser.add_argument('-y', '--year', type=str, required=False, default=None, help='year of the dataset to run program on, e.g. 2022PostEE, 2023, etc.')
    parser.add_argument('-s', '--skip', action='store_true', required=False, help='whether to skip preprocess.')
    parser.add_argument('-q', '--query', type=str, required=False, default=None, help='directory containing custom skim.')
    parser.add_argument('--is_mc', action='store_true', help='specify if processing Monte Carlo samples (if set) or collision data (if not set)')
    parser.add_argument('-o', '--outpath', type=str, required=False, default='skimmed', help='output path for collecting skimmed data')
    parser.add_argument('--skip_choose', action='store_true', help='skip the choose step in the preprocessor')
    args = parser.parse_args()

    groups = args.dataset
    years = args.year
   
    if groups is not None:
        if groups[0].lower() == 'all':
            groups = None
    
    if years is not None:
        if years[0].lower() == 'all':
            years = None

    qr = QueryRunner(groups, years, is_mc=args.is_mc, out_path=args.outpath, skip_choose=args.skip_choose)
    if args.skip:
        qr.dump_query()
    else:
        qr(args.query)