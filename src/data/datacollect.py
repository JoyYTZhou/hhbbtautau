#!/usr/bin/env python
# adapted from: https://github.com/bu-cms/bucoffea/blob/83daf25146d883df5131d0b50a51c0a6512d7c5f/bucoffea/helpers/dasgowrapper.py

import json, glob, shutil, argparse
import pandas as pd
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI

class QueryRunner:
    """Class to run the query on the dataset and preprocess the data."""
    def __init__(self) -> None:
        self.ddc = DataDiscoveryCLI()
        self.ddc.do_regex_sites(r"T[123]_(US)_\w+")

    def __call__(self, dataset, infile='availableQuery.json') -> None:
        self.run_mc_query(dataset, infile)
        self.preprocess_query(dataset)

    def run_query(self, query) -> dict:
        """Run a query and return the result as a list of strings.
        
        Parameters
        - `query`: dataset name, can contain wildcards."""
        return self.ddc.load_dataset_definition(dataset_definition=query, query_results_strategy='all', replicas_strategy='manual')
        
    def run_mc_query(self, dataset, infile):
        with open(infile, 'r') as file:
            mcstrings = json.load(file)

        self.run_query(mcstrings[dataset])
        
    def preprocess_query(self, dataset):
        self.ddc.do_preprocess(output_file=dataset,
            step_size=10000,
            align_to_clusters=False,
            recalculate_steps=False,
            files_per_batch=1,
            file_exceptions=(OSError, IndexError),
            save_form=False,
            scheduler_url=None)
        
        shutil.move(f"{dataset}_available.json.gz", f"preprocessed/{dataset}.json.gz")

def divide_samples(inputfn, outputfn, dict_size=5):
    """Divide the ds into smaller list as value per key.

    :param inputfn: path to json file containing dataset names. With structure:
        {processname: [list of filenames]}
    :type inputfn: string
    :param outputfn: path to output json file containing full dataset paths
    :type outputfn: string
        {
            processname1: [list of 5 filenames]
            processname2: [list of 5 filenames]
            processname3: [list of 5 filenames]
            ...
        }
    :param dict_size: size of the list as keys
    :type dict_size: int
    """
    with open(inputfn, 'r') as jsonfile:
        dsjson = json.load(jsonfile)

    complete_dict = {}

    for process, processlist in dsjson.items():
        complete_dict.update({process: {}})
        for dsname, itemlist in processlist.items():
            n_dicts = len(itemlist) // dict_size + (len(itemlist) % dict_size > 0)

        # Create the smaller dictionaries
            smaller_dicts = {f"{dsname}_{i+1}": itemlist[i*dict_size:(i+1)*dict_size] for i in range(n_dicts)}
            complete_dict[process].update(smaller_dicts)

    with open(outputfn, 'w') as jsonfile:
        json.dump(complete_dict, jsonfile)

def produceCSV(datadir):
    json_files = glob.glob(f"{datadir}/*.json")
    df = []
    for file_path in json_files:
        if not file_path.endswith('total.json'):
            with open(file_path, 'r') as file:
                data = json.load(file)
                for name, attributes in data.items():
                    df.append({
                        'name': name,
                        'string': attributes['string'][0] if attributes['string'] else None,
                        'xsection': attributes.get('xsection'),
                        '# raw events': attributes.get('Raw Events'),
                        '# weighted events': attributes.get('Wgt Events')
                    })
    all_df = pd.DataFrame(df)
    all_df.to_csv('compiled_weight.csv', index=False)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run preprocessor on fileset')    
    parser.add_argument('-d', type=str, required=True, help='dataset to run program on')

    args = parser.parse_args()
    qr = QueryRunner()
    qr(args.d)