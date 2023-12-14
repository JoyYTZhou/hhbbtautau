# adapted from https://github.com/bu-cms/projectcoffea/monojet/monojetProcessor.py

import copy
import os
import numpy as np
import pandas as pd
import coffea.processor as processor
from config.selectionconfig import settings as cfg
from coffea.nanoevents import NanoEventsFactory, BaseSchema
from analysis.selutility import trigger_selections, lepton_selections, pair_selections, jet_selections
from analysis.dsmethods import *
from analysis.histbooker import hbbtautau_accumulate, hhtobbtautau_accumulator


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


class hhbbtautauProcessor(processor.ProcessorABC):
    def __init__(self, blind=True):
        self._year = None
        self._blind = False
        self._configure()
        self._accumulator = hhtobbtautau_accumulator(cfg)
        self._dataset = None

    @property
    def accumulator(self):
        return self._accumulator

    def _configure(self, events=None):
        """Configure the processor."""

        cfg.DYNACONF_WORKS = "merge_configs"
        cfg.MERGE_ENABLED_FOR_DYNACONF = True

        if events is not None:
            self._dataset = events.metadata['dataset']

    def process(self, events):
        """Process events.
        :return: selected_events[channelname] = dict_accumulator({
            "Cutflow": accu_int(),
            "Objects": dict_accumulator(object_accs)
        })
        """

        if not cfg.size:
            return self.accumulator.identity()
        self._configure(events)

        # Triggers
        trigger_selections(events, cfg)

        # Lepton selections
        events_dict, cutflow_dict, object_dict = lepton_selections(events, cfg)
        # Pair selections
        pair_selections(events_dict, cutflow_dict, object_dict, cfg)
        # Jet selections
        jet_selections(events_dict, cutflow_dict, object_dict, cfg)
        # Fill histograms
        output = self.accumulator.identity()

        hbbtautau_accumulate(output, cfg, cutflow_dict, object_dict)

        return {self._dataset: output}

    def postprocess(self, acc_output):
        # output still accumulatable
        uniqueds = set(ds.split('_')[0] for ds in acc_output.keys())
        for ds in uniqueds:
            # Initialize a new accumulator for the combined process
            combined_accumulator = self.accumulator.identity()
            # Iterate over the datasets in acc_output
            for dataset in list(acc_output.keys()):
                # If the dataset starts with the current prefix, add its output to the combined accumulator
                if dataset.startswith(ds):
                    combined_accumulator.add(acc_output[dataset])
                # Remove the original accumulator
                    del acc_output[dataset]
            acc_output[ds] = combined_accumulator

def unwrap_col_acc(acc_output):
    """Unwrap the column accumulator in the acc_output.
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


def output_export(acc_output, rt_cfg, output=False):
    """ Export the accumulator to csv file based on settings in the run configuration
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
    :rtype acc_output: coffea.processor.dict_accumulator
    :param rt_cfg: run time configuration object
    :rtype rt_cfg: dynaconf object
    :param output: whether to output
    :rtype output: bool
    :return: cutflow and object dataframes
    :rtype: dict, dict
    """
    cf_df = init_output(cfg.signal.channelnames)
    obj_df = init_output(cfg.signal.channelnames)
    for dataset, output in acc_output.items():
        for channelname, acc in output.items():
            cf_df[channelname].append(pd.DataFrame.from_dict(
                acc['Cutflow'], orient='index', columns=[dataset]))
            obj_df = pd.DataFrame.from_dict(
                acc['Objects'], orient='index').transpose()
            obj_df['Dataset'] = dataset
            obj_df[channelname].append(obj_df)
    dirname = None if not output else rt_cfg.OUTPUTDIR_PATH
    cf_df = concat_output(
        cf_df, axis=1, dir_name=os.path.join(dirname, "cutflow"))
    obj_df = concat_output(
        obj_df, axis=0, dir_name=os.path.join(dirname, "object"))
    if output: return None
    else: return cf_df, obj_df


def init_output(channelnames):
    """Initialize the output dataframe list.

    :param channelnames: list of channel names
    :type channelnames: list
    :return: dictionary of empty dataframe lists
    :rtype: dict
    """
    cf_df_list = {channel: [] for channel in channelnames}
    return cf_df_list


def concat_output(cf_df_list, axis=0, dir_name=None, index=None):
    """Concatenate the output dataframe list.

    :param cf_df_list: dictionary of dataframe lists
    :type cf_df_list: dict
    :param axis: axis to concatenate, defaults to 0
    :type axis: int, optional
    :param dir_name: directory name to export the concatenated dataframe, defaults to None
    :type dir_name: str, optional
    :param index: index of the dataframe, defaults to None
    :type index: list, optional
    :return: dictionary of concatenated dataframes, with channels as keys
    :rtype: dict
    """
    for channelname, df_list in cf_df_list.items():
        cf_df_list[channelname] = pd.concat(df_list, axis=axis)
        if index is not None:
            cf_df_list[channelname].index = index
        if dir_name is not None:
            finame = f"{channelname}.csv"
            cf_df_list[channelname].to_csv(os.path.join(dir_name, finame))
    return cf_df_list


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
