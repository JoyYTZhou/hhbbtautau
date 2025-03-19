#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, shutil, argparse, re, gzip
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI
from src.utils.filesysutil import FileSysHelper, pjoin

class QueryRunner:
    """Class to run the query on dataset strings and preprocess the dataset.
    Currently only supports MC datasets. Dependent on DataDiscoveryCLI from coffea."""
    def __init__(self, dataset, infile) -> None:
        """Initialize the QueryRunner object.
        
        Parameters
        - `dataset`: str, the dataset name key in the json file to query and preprocess. If none, query over all keys in input files."""
        self.ddc = DataDiscoveryCLI()
        self.ddc.do_regex_sites(r"T[123]_(US)_\w+")
        with open(infile, 'r') as file:
            self.mcstrings = json.load(file)
        self.name = infile.split('/')[-1].split('.')[0]
        if dataset is None:
            self.dataset = list(self.mcstrings.keys())
        else:
            self.dataset = [dataset]

    def __call__(self, query_dir=None) -> None:
        """Run the query on the dataset and preprocess the dataset."""
        if query_dir is None:
            self.query_from_dasgo()
        else:
            FileSysHelper.checkpath(query_dir, createdir=False, raiseError=True)
            for dataset in self.dataset:
                if FileSysHelper.checkpath(pjoin(query_dir, self.name, dataset), createdir=False, raiseError=False):
                    self.query_from_dir(query_dir, dataset, self.name)
                else:
                    print(f"No custom skims for {self.name} {dataset} have been produced.")
    
    def query_from_dasgo(self) -> None:
        """Query the available files from the DASGO. Produce a json.gz file with the query results (files, redirectors, uuids etc.)"""
        suffix = self.name
        for dataset in self.dataset:
            self.ddc.load_dataset_definition(dataset_definition=self.mcstrings[dataset], query_results_strategy='all', replicas_strategy='manual')
        
        out_name = f'{dataset}_{suffix}' if len(self.dataset) == 1 else 'Data' #! A hack for now

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
    
    def query_from_dir(self, query_dir, dataset, year) -> None:
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
            shortname = self.mcstrings[dataset][datasetname]['shortname'] 
            root_files = FileSysHelper.glob_files(pjoin(query_dir, year, dataset), f'{shortname}*.root')
            for root_file in root_files:
                match = pattern.search(root_file)
                if match:
                    index = match.group(1)
                    queryed_result[datasetname]["files"][f'{root_file}:Events'] = {"uuid": index}
        
        FileSysHelper.checkpath('skimmed', createdir=True)

        with gzip.open(f"skimmed/{dataset}_{year}.json.gz", 'wt') as file:
            json.dump(queryed_result, file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run preprocessor on fileset')    
    parser.add_argument('-d', '--dataset', type=str, required=False, default=None, 
                        help='group name of the dataset to run program on, e.g. TTbar, DYJets, etc. Note that this must match the key in the json input file. If not specified, run over all available datasets found in json input file.')
    parser.add_argument('-i', '--infile', type=str, required=True, help='path of the json file containing the dataset query string')
    parser.add_argument('-s', '--skip', action='store_true', required=False, help='whether to skip preprocess.')
    parser.add_argument('-q', '--query', type=str, required=False, default=None, help='directory containing custom skim.')

    args = parser.parse_args()
    qr = QueryRunner(args.dataset, args.infile)
    if args.skip:
        qr.dump_query()
    else:
        qr(args.query)