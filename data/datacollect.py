#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, shutil, argparse, re, gzip
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI
from src.utils.filesysutil import FileSysHelper, pjoin

class QueryRunner:
    """Class to run the query on the dataset and preprocess the data."""
    def __init__(self) -> None:
        self.ddc = DataDiscoveryCLI()
        self.ddc.do_regex_sites(r"T[123]_(US)_\w+")

    def __call__(self, dataset, infile='availableQuery.json', query_dir=None) -> None:
        with open(infile, 'r') as file:
            mcstrings = json.load(file)
        
        if query_dir is None:
            self.query_from_dasgo(dataset, mcstrings)
        else:
            FileSysHelper.checkpath(query_dir, createdir=False, raiseError=True)
            self.query_from_dir(dataset, query_dir, mcstrings)
    
    def query_from_dasgo(self, dataset, metaquery) -> None:
        """Query the available files from the DASGO."""
        self.ddc.load_dataset_definition(dataset_definition=metaquery[dataset], query_results_strategy='all', replicas_strategy='manual')

        self.ddc.do_preprocess(output_file=dataset,
            step_size=10000,
            align_to_clusters=False,
            recalculate_steps=False,
            files_per_batch=1,
            file_exceptions=(OSError, IndexError),
            save_form=False,
            allow_empty_datasets=True,
            scheduler_url=None)
        
        shutil.move(f"{dataset}_available.json.gz", f"preprocessed/{dataset}.json.gz")
    
    def query_from_dir(self, dataset, query_dir, metaquery) -> None:
        """Query the available files from the query_dir, e.g. a directory containing custom skim files. 
        Right now this does not do preprocessing."""
        queryed_result = {}
        root_files = FileSysHelper.glob_files(pjoin(query_dir, dataset), '*.root')

        pattern = re.compile(r'_(\d+)\.root$')

        for datasetname in metaquery[dataset].keys():
            queryed_result[datasetname] = {"files": {}}
            queryed_result[datasetname]["metadata"] = metaquery[dataset][datasetname]
            for root_file in root_files:
                match = pattern.search(root_file)
                if match:
                    index = match.group(1)
                    queryed_result[datasetname]["files"][f'{root_file}:Events'] = {"uuid": index}
        
        FileSysHelper.checkpath('skimmed', createdir=True)

        with gzip.open(f"skimmed/{dataset}.json.gz", 'wt') as file:
            json.dump(queryed_result, file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run preprocessor on fileset')    
    parser.add_argument('-d', '--dataset', type=str, required=True, help='dataset to run program on')
    parser.add_argument('-s', '--skip', action='store_true', required=False, help='whether to skip preprocess.')
    parser.add_argument('-q', '--query', type=str, required=False, default=None, help='directory containing custom skim.')

    args = parser.parse_args()
    qr = QueryRunner()
    if args.skip:
        qr.dump_query(args.dataset)
    else:
        qr(args.dataset, query_dir=args.query)