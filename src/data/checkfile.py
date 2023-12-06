import uproot as uproot
import json
from tqdm import tqdm

def find_branches(file_path, object_list):
    # Open the root file
    file = uproot.open(file_path)

    # Get the tree. Assuming the tree name is "Events"
    tree = file["Events"]

    # Get all branch names
    branch_names = tree.keys()

    # Initialize a dictionary to hold the branches for each object
    branches = {}

    # Filter branch names that start with each object
    for object in object_list:
        branches[object] = [name for name in branch_names if name.startswith(object)]

    return branches

def output_branches(in_fipath, out_fipath, checklist):
    branches = find_branches(in_fipath, checklist)
    with open(out_fipath, 'w') as f:
        for object, object_branches in branches.items():
            f.write(f"{object} branches:\n")
            for branch in object_branches:
                f.write("%s\n" % branch)
            f.write("\n")

if __name__ == "__main__":
    checklist = ['Electron_', 'Muon_', 'Tau_', 'Jet_', 'FatJet_']
    inputfile = "completepath.json"
    with open (inputfile, 'r') as f:
        data = json.load(f)
        for dataset, filelist in tqdm(data['Signal'].items(), desc="Processing files"):
            output_branches(filelist[0], f"objectchecks/{dataset}.txt", checklist)
        for dataset, filelist in tqdm(data['Background'].items(), desc="Processing files"):
            output_branches(filelist[0], f"objectchecks/{dataset}.txt", checklist)

