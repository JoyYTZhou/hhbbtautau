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
        self._year=None
        self._blind=False
        self._configure()
        self._accumulator = hhtobbtautau_accumulator(cfg)
        self._dataset = None

    @property
    def accumulator(self):
        return self._accumulator

    def _configure(self, events=None):
        """Configure the processor."""

        cfg.DYNACONF_WORKS="merge_configs"
        cfg.MERGE_ENABLED_FOR_DYNACONF=True
        
        if events is not None:
            self._dataset = events.metadata['dataset']

    def process(self, events):
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

        return output

    def postprocess(self, acc_output, dir_name, cf_list):
        for channel in acc_output.keys():
            fi_savename = "_".join([self._dataset, channel])
            channel_df =  pd.DataFrame.from_dict(acc_output[channel]['Objects'])
            channel_df['Process'] = self._dataset
            channel_df.to_csv(os.path.join(dir_name, fi_savename))
            cf_df = pd.DataFrame.from_dict(acc_output[channel]['Cutflow'])
            cf_df.index = [self._dataset]
            cf_list[channel].append(cf_df)

def init_output():
    cf_df_list = {} 
    for i in range(cfg.signal.channelno):
        lepcfgname = "signal.channel"+str(i+1)
        channelname = cfg[lepcfgname+".name"]
        cf_df_list[channelname] = []
    return cf_df_list

def concat_output(cf_df_list, dir_name = None):
    for channelname, df_list in cf_df_list.items():
        cf_df_list[channelname] = pd.concat(df_list)
        if dir_name is not None:
            finame = f"{channelname}.csv"
            cf_df_list[channelname].to_csv(os.path.join(dir_name, finame)) 

     
    