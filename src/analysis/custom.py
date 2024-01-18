# adapted from https://github.com/bu-cms/projectcoffea/monojet/monojetProcessor.py

import os
import numpy as np
import pandas as pd
from analysis.selutility import Object
from analysis.dsmethods import *
from config.selectionconfig import settings as sel_cfg
import operator as opr

output_cfg = sel_cfg.signal.outputs

def lepton_selection(events, channelname):
    channel_cfg = sel_cfg.signal[channelname].selections
    electron = Object(events, "Electron", output_cfg.Electron, channel_cfg.electron)
    muon = Object(events, "Muon", output_cfg.Muon, channel_cfg.muon)
    tau = Object(events, "Tau", output_cfg.Tau, channel_cfg.tau)
    electron_mask = electron.ptmask(opr.ge)
    

def trigger_selection(selection, events, cfg):

    """Add trigger selections to coffea processor selection.

    :param events: events from root NANOAOD files
    :type events: coffea.nanoevents.methods.base.NanoEventsArray
    :param selection: selection critiera in boolean mask vectors in a compact manner
    :type selection: coffea.processor.PackedSelection
    :param cfg: Configuration
    :type cfg: DynaConf object
    :return: Selection object
    :rtype: coffea.processor.PackedSelection (https://coffeateam.github.io/coffea/api/coffea.processor.PackedSelection.html)
    """
    return 0

def unwrap_col_acc(acc_output):
    """Unwrap the column accumulator in the acc_output['Objects'] to a list of numpy arrays.
    :param acc_output: accumulated output from coffea.processor/runner.
        Nested as the following data structure:
        {
            dataset: {
                channelname: {
                    "Cutflow": int_accumulator,
                    "Objects": column_accumulator
                }
            }
        }
    :type acc_output: coffea.processor.dict_accumulator
    :return: None
    """
    for dataset in list(acc_output.keys()):
        for channelname, output in acc_output[dataset].items():
            acc_output[dataset][channelname]["Objects"] = {kinematic: col_acc.value if col_acc.value.size else [np.nan]
                                                           for kinematic, col_acc in output['Objects'].items()}


def object_export(acc_output, rt_cfg, output=False, suffix=None):
    """ Export the object observables to csv file based on settings in the run configuration
    :param acc_output: accumulated output from coffea.processor/runner.
    :rtype acc_output: coffea.processor.dict_accumulator
    :param rt_cfg: run time configuration object
    :rtype rt_cfg: dynaconf object
    :param output: whether to output
    :rtype output: bool
    :return: object observables dataframe
    :rtype: dict, dict
    """
    obj_df = init_output()
    for dataset, output_item in acc_output.items():
        for channelname, acc in output_item.items():
            temp_df = pd.DataFrame.from_dict(
                acc['Objects'], orient='index').transpose()
            temp_df['Dataset'] = dataset
            obj_df[channelname].append(temp_df)
    dirname = None if not output else os.path.join(rt_cfg.OUTPUTDIR_PATH, "object")
    obj_df = concat_output(obj_df, axis=0, suffix=suffix, dir_name=dirname)
    if output:
        return None
    else:
        return obj_df

def combine_cutflow(cf_df, acc):
    """Combine the cutflow dataframe to a single dataframe.
    :param cf_df: dictionary of list of cutflow dataframe
    :type cf_df: dict
    :param acc: accumulated output from coffea.processor/runner.
    :type acc: coffea.processor.dict_accumulator
    :param rt_cfg: run time configuration object
    :rtype rt_cfg: dynaconf object
    :return: None
    """
    for dataset, output_item in acc.items():
        for channelname, acc in output_item.items():
            cf_df[channelname].append(pd.DataFrame.from_dict(
                acc['Cutflow'], orient='index', columns=[dataset]))

def init_output():
    """Initialize the output dataframe list.

    :return: dictionary of empty dataframe lists
    :rtype: dict
    """
    cf_df_list = {channel: [] for channel in cfg.signal.channelnames}
    return cf_df_list


def concat_output(df_list, axis=0, sum_col=False, suffix=None, dir_name=None, index=None):
    """Concatnate output dataframe list based on selection channel.

    :param df_list: dictionary of dataframe lists
    :type df_list: dict
    :param axis: axis to concatenate, defaults to 0
    :type axis: int, optional
    :param sum_col: whether to sum the columns with the same column name, defaults to False
    :type sum_col: bool, optional 
    :param suffix: suffix to add to the datapath, defaults to None
    :type suffix: str, optional
    :param dir_name: directory name to export the concatenated dataframe, defaults to None
    :type dir_name: str, optional
    :param index: index of the dataframe, defaults to None
    :type index: list, optional
    :return: dictionary of concatenated dataframes, with channels as keys
    :rtype: dict
    """
    result = {}
    for channelname, df_list in df_list.items():
        df_concat = pd.concat(df_list, axis=axis)
        if sum_col:
            result[channelname] = df_concat.groupby(df_concat.columns, axis=1).sum()
        else:
            result[channelname] = df_concat
        if index is not None:
            result[channelname].index = index
        if dir_name is not None:
            finame = f"{channelname}_{suffix}.csv" if suffix is not None else f"{channelname}.csv"
            result[channelname].to_csv(os.path.join(dir_name, finame), mode='a')
    return result

def divide_ds(ds, dict_size):
    """Divide the ds into chunks of dict_size.

    :param ds: dataset with dataset name and a list of files as value
    :type ds: dict
    :param dict_size: size of the dictionary
    :type dict_size: int
    :return: dictionary of evenly divided files
        {
            datasetname1: [20 files],
            datasetname2: [20 files],
            ...
        }
    :rtype: dict
    """

    # Get the list of items from the original dictionary
    items = list(ds.values())[0]
    dsname = list(ds.keys())[0]

    # Calculate the number of smaller dictionaries needed
    n_dicts = len(items) // dict_size + (len(items) % dict_size > 0)

    # Create the smaller dictionaries
    smaller_dicts = {
        f"{dsname}{i+1}": items[i*dict_size:(i+1)*dict_size] for i in range(n_dicts)}
    return smaller_dicts
