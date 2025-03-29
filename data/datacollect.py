#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, shutil, argparse, re, gzip, logging
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI
from src.utils.filesysutil import FileSysHelper, pjoin

mc_dir = "availableMC"
data_dir = "availableData"

class QueryRunner:
    """Class to run the query on dataset strings and preprocess the dataset.
    Currently only supports MC datasets. Dependent on DataDiscoveryCLI from coffea."""
    def __init__(self, dataset, year, is_mc, out_path) -> None:
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

        self.dataset = dataset

        self.outpath = out_path
        
        FileSysHelper.checkpath(self.outpath, createdir=True)

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
                    for dataset in year_data.keys():
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
        for year, year_data in self.mcstrings.items():
            suffix = year
            
            if dataset is not None:
                # If dataset is specified, only process that dataset
                datasets_to_process = [self.dataset]
            else:
                # Process all datasets in this year
                datasets_to_process = year_data.keys()
            
            # Process each dataset
            for dataset in datasets_to_process:
                self.ddc.load_dataset_definition(dataset_definition=self.mcstrings[dataset], query_results_strategy='all', replicas_strategy='manual')
    
            if not self._isMC:
                out_name = f"Data_{suffix}"
            else:
                if len(self.dataset) > 1:
                    out_name = f'{suffix}'
                else:
                    out_name = f'{self.dataset[0]}_{suffix}'

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
        
            self.ddc = DataDiscoveryCLI()
            self.ddc.do_regex_sites(r"T[123]_(US)_\w+")
    
    def query_from_dir(self, query_dir, dataset, year, outpath) -> None:
        """Query the available files from the query_dir, e.g. a directory containing custom skim files. 
        Right now this does not do preprocessing.
        
        Parameters
        - `query_dir`: str, the directory containing the custom skim files (currently only supports root files)
        """
        queryed_result = {}

        pattern = re.compile(r'_(\d+)\.root$')

        for datasetname in self.mcstrings[dataset].keys():
            queryed_result[datasetname] = {"files": {}}
            queryed_result[datasetname]["metadata"] = self.mcstrings[dataset][datasetname]
            queryed_result[datasetname]["metadata"]["is_mc"] = self._isMC
            shortname = self.mcstrings[dataset][datasetname]['shortname'] 
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

    parser = argparse.ArgumentParser(description=program_description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d', '--dataset', type=str, required=False, default=None,
                        help='group name of the dataset to run program on, e.g. TTbar, DYJets, etc. Note that this must match the key in the json input file.')
    parser.add_argument('-y', '--year', type=str, required=False, default=None, help='year of the dataset to run program on, e.g. 2022PostEE, 2023, etc.')
    parser.add_argument('-s', '--skip', action='store_true', required=False, help='whether to skip preprocess.')
    parser.add_argument('-q', '--query', type=str, required=False, default=None, help='directory containing custom skim.')
    parser.add_argument('--is_mc', action='store_true', help='specify if processing Monte Carlo samples (if set) or collision data (if not set)')
    parser.add_argument('-o', '--outpath', type=str, required=False, default='skimmed', help='output path for collecting skimmed data')
    args = parser.parse_args()

    qr = QueryRunner(args.dataset, args.year, is_mc=args.is_mc)
    if args.skip:
        qr.dump_query()
    else:
        qr(args.query)