#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, shutil, argparse, re, gzip
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI
from src.utils.filesysutil import FileSysHelper, pjoin

class QueryRunner:
    """Class to run the query on the dataset and preprocess the data."""
    def __init__(self, dataset) -> None:
        self.ddc = DataDiscoveryCLI()
        self.ddc.do_regex_sites(r"T[123]_(US)_\w+")
        self.dataset = dataset

    def __call__(self, infile='availableQuery.json', query_dir=None) -> None:
        with open(infile, 'r') as file:
            mcstrings = json.load(file)
        
        if query_dir is None:
            self.query_from_dasgo(mcstrings)
        else:
            FileSysHelper.checkpath(query_dir, createdir=False, raiseError=True)
            self.query_from_dir(query_dir, mcstrings)
    
    def query_from_dasgo(self, metaquery) -> None:
        """Query the available files from the DASGO."""
        self.ddc.load_dataset_definition(dataset_definition=metaquery[self.dataset], query_results_strategy='all', replicas_strategy='manual')

        self.ddc.do_preprocess(output_file=self.dataset,
            step_size=10000,
            align_to_clusters=False,
            recalculate_steps=False,
            files_per_batch=1,
            file_exceptions=(OSError, IndexError),
            save_form=False,
            allow_empty_datasets=True,
            scheduler_url=None)
        
        shutil.move(f"{self.dataset}_available.json.gz", f"preprocessed/{self.dataset}.json.gz")
    
    def query_from_dir(self, query_dir, metaquery) -> None:
        """Query the available files from the query_dir, e.g. a directory containing custom skim files. 
        Right now this does not do preprocessing."""
        queryed_result = {}

        pattern = re.compile(r'_(\d+)\.root$')

        for datasetname in metaquery[self.dataset].keys():
            queryed_result[datasetname] = {"files": {}}
            queryed_result[datasetname]["metadata"] = metaquery[self.dataset][datasetname]
            shortname = metaquery[self.dataset][datasetname]['shortname'] 
            root_files = FileSysHelper.glob_files(pjoin(query_dir, self.dataset), f'{shortname}*.root')
            for root_file in root_files:
                match = pattern.search(root_file)
                if match:
                    index = match.group(1)
                    queryed_result[datasetname]["files"][f'{root_file}:Events'] = {"uuid": index}
        
        FileSysHelper.checkpath('skimmed', createdir=True)

        with gzip.open(f"skimmed/{self.dataset}.json.gz", 'wt') as file:
            json.dump(queryed_result, file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run preprocessor on fileset')    
    parser.add_argument('-d', '--dataset', type=str, required=True, 
                        help='group name of the dataset to run program on, e.g. TTbar, DYJets, etc. Note that this must match the key in the availableQuery.json file.')
    parser.add_argument('-s', '--skip', action='store_true', required=False, help='whether to skip preprocess.')
    parser.add_argument('-q', '--query', type=str, required=False, default=None, help='directory containing custom skim.')

    args = parser.parse_args()
    qr = QueryRunner(args.dataset)
    if args.skip:
        qr.dump_query()
    else:
        qr(query_dir=args.query)