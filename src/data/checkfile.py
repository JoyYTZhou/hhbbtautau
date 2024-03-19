import uproot as uproot
import json
from tqdm import tqdm
from data.datacollect import dasgo_query, xrootd_format
from utils.filehelper import *

def output_branches(in_fipath, out_fipath, checklist):
    """ Write the branches to a file.

    :param in_fipath: path to the root file
    :param out_fipath: path to the output file
    :param checklist: list of objects to find branches for
    """
    branches = find_branches(in_fipath, checklist)
    with open(out_fipath, 'w') as f:
        for object, object_branches in branches.items():
            f.write(f"{object} branches:\n")
            for branch in object_branches:
                f.write("%s\n" % branch)
            f.write("\n")

if __name__ == "__main__":
    checklist = ['Electron_', 'Muon_', 'Tau_', 'Jet_', 'FatJet_']
    inputfile = "MCsamplepath.json"
    with open (inputfile, 'r') as f:
        data = json.load(f)
        filedicts = {}
        filedicts.update(data['Signal'])
        filedicts.update(data['Background'])
        for process, datasets in tqdm(filedicts.items(), desc="Processing files"):
            for dataset in datasets:
                try:
                    sample = dasgo_query(f"dataset={dataset}")[0]
                    finame = sample.split('/')[1]
                    print(f"Writing to file {finame}.txt")
                    sample = dasgo_query(f"file dataset={sample}")[0]
                    sample = xrootd_format(sample)
                    output_branches(sample, f"objectchecks/{finame}.txt", checklist)
                except IndexError:
                    print(f"No data returned for dataset {dataset}")

