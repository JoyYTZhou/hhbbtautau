# adapted from https://github.com/bu-cms/projectcoffea/blob/master/projectcoffea/helpers/dataset.py
###############################################################################
# This file contains methods 


import re

import numpy as np
import re
from analysis.mathutility import projectcoffea_path
import os
import socket
import subprocess
from collections import defaultdict
pjoin = os.path.join

def extract_items(ds, start_string):
    """Extract items from a dictionary where the keys start with a certain string.

    :param ds: The original dictionary.
    :type ds: dict
    :param start_string: The string that the keys should start with.
    :type start_string: str
    :return: A new dictionary with the extracted items.
    :rtype: dict
    """
    return {key: value for key, value in ds.items() if key.startswith(start_string)}


def short_name(dataset):
    """Shorten name for NANOAOD datasets for better extractability.

    :param dataset: name of the dataset 
    :type dataset: str
    """

    _, name, conditions, _ = dataset.split("/")

    # Remove useless info
    name = name.replace("_TuneCP5","")
    name = name.replace("_TuneCUETP8M1","")
    name = name.replace("_13TeV","")
    name = name.replace("-pythia8","")
    name = name.replace("madgraphMLM","MLM")
    name = name.replace("madgraph","mg")
    name = name.replace("amcnloFXFX","FXFX")
    name = name.replace("powheg","pow")

    # Detect extension
    m=re.match(r".*(ext\d+).*",conditions);
    if m:
        name = name + "_" + m.groups()[0]
    m=re.match(r".*(ver\d+).*",conditions);
    if m:
        name = name + "_" + m.groups()[0]
    if 'new_pmx' in conditions:
        name = name + '_new_pmx'
    if 'Run3Summer22' in conditions:
        name = name + "_2022"
    elif "Run3Summer23" in conditions:
        name = name + "_2023"

    m = re.match(r"Run(\d+[A-Z]*)", conditions)
    if m:
        name = name + "_" + m.groups()[0]

    return name


def load_lists():
    """Generate file list per dataset

    :return: Mapping of dataset : [files]
    :rtype: dict
    """
    files = [
        projectcoffea_path(f"data/datasets/datasets_nanoaod_v7_2016.txt"),
        projectcoffea_path(f"data/datasets/datasets_nanoaod_v7_2017.txt"),
        projectcoffea_path(f"data/datasets/datasets_nanoaod_v7_2018.txt")
    ]
    lines = []
    for fpath in files:
        with open(fpath,"r") as f:
            lines.extend(f.readlines())

    lines = filter(lambda x: "NANOAOD" in x and not x.startswith("#"), lines)
    return lines



def files_from_ac(regex):
    """Generate file list per dataset from T2_DE_RWTH

    :param regex: Regular expression to match datasets
    :type regex: string
    :return: Mapping of dataset : [files]
    :rtype: dict
    """
    path = projectcoffea_path('data/datasets/crabfiles.yml')

    with open(path, 'r') as stream:
        try:
            fileset = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    for dataset, files in fileset.items():
        if not re.match(regex, dataset):
            continue
        for ifile in reversed(files):
            if not len(ifile):
                files.remove(ifile)
        fileset[dataset] = files
    return fileset

def find_files(directory, regex):
    fileset = {}
    for path, _, files in os.walk(directory):

        files = list(filter(lambda x: x.endswith('.root'), files))
        if not len(files):
            continue
        dataset = path.split('/')[-3]
        if not re.match(regex, dataset):
            continue
        files = [pjoin(path,x) for x in files]
        fileset[dataset] = files
    return fileset

def eosls(path):
    return subprocess.check_output(['xrdfs', 'root://cmseos.fnal.gov','ls','-l',path]).decode('utf-8')

def eosfind(path):
    cmd = ['eos', 'root://cmseos.fnal.gov/', 'find',  '--size', path]
    return subprocess.check_output(cmd).decode('utf-8')


def find_files_eos(directory, regex):
    fileset = defaultdict(list)
    lines = eosfind(re.sub('.*/store/','/store/',directory)).splitlines()
    # For files, lines are formatted as
    # path=(File path starting with /eos/uscms) size=(Size in bits)
    # For folders, the 'size' part is left out, so they can easily be filtered
    for line in lines:
        parts = line.split()

        # Ignore lines representing directories
        if len(parts) < 2:
            continue
        # Ensure we are not missing a part
        if len(parts) > 2:
            raise RuntimeError(f'Encountered malformed line: {line}')

        # The info we care about
        path = parts[0].replace('path=','')
        if not path.endswith('.root'):
            continue

        dataset = path.split('/')[9]
        if not re.match(regex, dataset):
            continue
        fileset[dataset].append(re.sub('.*/store','root://cmsxrootd-site.fnal.gov//store', path))
    return fileset

def files_from_eos(regex):
    """Generate file list per dataset from EOS

    :param regex: Regular expression to match datasets
    :type regex: string
    :return: Mapping of dataset : [files]
    :rtype: dict
    """

    host = socket.gethostname()
    if 'lxplus' in host:
        topdir = '/eos/cms/store/group/phys_exotica/monojet/aalbert/nanopost/'
        tag = '16Jul19'

        fileset_16jul = find_files(pjoin(topdir, tag), regex)

        topdir = '/eos/user/a/aalbert/nanopost/'
        tag = '10Aug19'

        fileset_10aug = find_files(pjoin(topdir, tag), regex)

        fileset = {}
        keys = set(list(fileset_16jul.keys()) + list(fileset_10aug.keys()))
        for key in keys:
            if key in fileset_10aug:
                fileset[key] = fileset_10aug[key]
            else:
                fileset[key] = fileset_16jul[key]
    elif 'lpc' in host:
        topdir = '/eos/uscms/store/user/aandreas/nanopost/'
        tag = '03Sep20v7'
        fileset = find_files_eos(pjoin(topdir, tag), regex)

    return fileset

def is_lo_znunu(dataset):
    return bool(re.match(r'Z(\d*)Jet.*(mg|MLM|madgraph).*', dataset))

def is_lo_z(dataset):
    return bool(re.match(r'(DY|Z)(\d*)Jet.*(mg|MLM|madgraph).*', dataset))

def is_lo_z_ewk(dataset):
    return bool(re.match(r'EWKZ2Jets_ZTo.', dataset))

def is_lo_w(dataset):
    return bool(re.match(r'W(\d*)Jet.*(mg|MLM).*', dataset))

def is_lo_w_ewk(dataset):
    return bool(re.match(r'EWKW(Minus|Plus)2Jets_WToLNu.', dataset))

def is_lo_g(dataset):
    return bool(re.match(r'GJets.*HT.*', dataset))

def is_lo_g_ewk(dataset):
    return bool(re.match(r'GJets.*EWK.*', dataset))

def is_nlo_g(dataset):
    return bool(re.match(r'G(\d)*Jet.*(amc|NLO).*', dataset))

def is_nlo_g_ewk(dataset):
    return bool(re.match(r'AJJ.*amc.*', dataset))

def is_nlo_z(dataset):
    return bool(re.match(r'(DY|Z)(\d*)Jet.*FXFX.*', dataset))

def is_nlo_w(dataset):
    return bool(re.match(r'W(\d*)Jet.*FXFX.*', dataset))

def has_v_jet(dataset):
    return bool(re.match(r'(WW|WZ|ZZ|TTJets|TTToHadronic|.*WToQQ|.*ZToQQ).*', dataset))

def is_data(dataset):
    tags = ['EGamma','MET','SingleElectron','SingleMuon','SinglePhoton','JetHT']
    if any([dataset.startswith(itag) for itag in tags ]):
        return True
    if re.match('QCD_data_(\d)+',dataset):
        return True
    return False


def extract_year(dataset):
    """Extract the year from a dataset name

    :param regex: dataset
    :type regex: string
    :return: Name of the year
    :rtype: int
    """ 
    for x in [2,3,4]:
        if f"202{x}" in dataset:
            return 2010+x
    raise RuntimeError("Could not determine dataset year")

def rand_dataset_dict(keys, year):
    '''
    Creates a map of dataset names -> short dataset names for randomized parameter samples
    '''
    if year==2016:
        conditions = 'RunIISummer16'
    elif year==2017:
        conditions = 'RunIIFall17'
    elif year==2018:
        conditions = 'RunIIAutumn18'
    else:
        raise RuntimeError("Cannot recognize year: {year}")

    datasets = [x.replace("GenModel_","") for x in keys if "GenModel" in x]

    return {x : short_name(f"/{x}/{conditions}/NANOAODSIM") for x in datasets}