# adapted from https://github.com/bu-cms/projectcoffea/monojet/monojetProcessor.py

import copy
import os
import numpy as np
import pandas as pd
import coffea.processor as processor
from config.selectionconfig import settings as cfg
from analysis.selutility import lepton_selections, pair_selections, jet_selections
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
        # TODO: Add triggers

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
        # TODO: Simplify this acc_output
        for dataset in acc_output.keys():
            for channelname, output in acc_output[dataset].items():
                acc_output[dataset][channelname]["Objects"] = {kinematic: col_acc.value if col_acc.value.size else [np.nan]
                                                               for kinematic, col_acc in output['Objects'].items()}


def output_export(acc_output, rt_cfg):
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
    """
    cf_df_list = init_output(cfg.signal.channelnames)
    obj_df_list = init_output(cfg.signal.channelnames)
    for dataset, output in acc_output.items():
        for channelname, acc in output.items():
            cf_df_list[channelname].append(pd.DataFrame.from_dict(
                acc['Cutflow'], orient='index', columns=[dataset]))
            obj_df = pd.DataFrame.from_dict(
                acc['Objects'], orient='index').transpose()
            obj_df['Dataset'] = dataset
            obj_df_list[channelname].append(obj_df)
    concat_output(cf_df_list, axis=1, dir_name=os.path.join(
        rt_cfg.outputdir_path, "cutflow"))
    concat_output(obj_df_list, axis=0, dir_name=os.path.join(
        rt_cfg.outputdir_path, "object"))


def init_output(channelnames):
    cf_df_list = {channel: [] for channel in channelnames}
    return cf_df_list


def concat_output(cf_df_list, axis=0, dir_name=None, index=None):
    for channelname, df_list in cf_df_list.items():
        cf_df_list[channelname] = pd.concat(df_list, axis=axis)
        if index is not None:
            cf_df_list[channelname].index = index
        if dir_name is not None:
            finame = f"{channelname}.csv"
            cf_df_list[channelname].to_csv(os.path.join(dir_name, finame))
