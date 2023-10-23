# adapted from https://github.com/bu-cms/projectcoffea/monojet/monojetProcessor.py

import copy
import re
import numpy as np
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

    @property
    def accumulator(self):
        return self._accumulator

    def _configure(self, events=None):
        """Configure the processor."""

        cfg.DYNACONF_WORKS="merge_configs"
        cfg.MERGE_ENABLED_FOR_DYNACONF=True


    def process(self, events):
        if not cfg.size:
            return self.accumulator.identity()
        # self._configure(events)
        dataset = events.metadata['dataset']

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

    def postprocess(self, accumulator):
        return accumulator