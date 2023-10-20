import re
import copy

from coffea.processor import dict_accumulator, column_accumulator, defaultdict_accumulator
import numpy as np
import awkward as ak
import hist
import vector as vec
from collections import ChainMap


from analysis.mathutility import *
from analysis.selutility import LV
from analysis.dsmethods import *


def empty_colacc_int():
    return column_accumulator(np.array([],dtype=np.uint64))
def empty_colacc_int64():
    return column_accumulator(np.array([],dtype=np.int64))
def empty_colacc_float64():
    return column_accumulator(np.array([],dtype=np.float64))
def empty_colacc_float32():
    return column_accumulator(np.array([],dtype=np.float32))
def empty_colacc_float16():
    return column_accumulator(np.array([],dtype=np.float16))
def empty_colacc_bool():
    return column_accumulator(np.array([],dtype=np.bool))
def accu_int():
    return defaultdict_accumulator(int)

def hhtobbtautau_accumulator(cfg):
    """ Construct an accumulator for hhtobbtautau analysis.
    
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: nested accumulator with cutflow int accumulator and object properties col accumulators in each channel
    :rtype: coffea.processor.dict_accumulator
    """

    selected_events = {}
    for i in range(cfg.signal.channelno):
        object_dict = {}
        lepcfgname = "signal.channel"+str(i+1)
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if not lepselname.electron.veto and (lepselname.electron.veto is not None):
                book_single_col_accu(lepselname.electron.outputs, object_dict)
        if not lepselname.muon.veto and (lepselname.muon.veto is not None):
                book_single_col_accu(lepselname.muon.outputs, object_dict)
        if not lepselname.tau.veto and (lepselname.tau.veto is not None):
            if lepselname.tau.count == 1:
                book_single_col_accu(lepselname.tau.outputs, object_dict)
            else:
                for i in range(lepselname.tau.count): 
                    subfix = f"_{i+1}"
                    book_single_col_accu(lepselname.tau.outputs, object_dict, subfix)
        # book column accumulators for combinatorials of objects kinematics
        if lepselname.pair is not None:
            object_dict["dRll"] = empty_colacc_float32()
            object_dict["mll"] = empty_colacc_float32()
            object_dict["ptll"] = empty_colacc_float32()
        if cfg.signal.commonsel is not None and cfg.signal.commonsel.ak4jet is not None:
            object_dict["ptjj"] = empty_colacc_float32()
            object_dict["mjj"] = empty_colacc_float32()
            object_dict["dRjj"] = empty_colacc_float32()
            # TODO: add lepton jet
        selected_events[channelname] = dict_accumulator({
            "Cutflow": accu_int(),
            "Objects": dict_accumulator(object_dict)
        })
    combined_accumulator = dict_accumulator(selected_events)
    
    return combined_accumulator

def hbbtautau_accumulate(output, cfg, events_dict, cutflow_dict):
    """ Fill in the accumulator for the current process. 
    :param output: output accumulator
    :type output: self.accumulator.identity
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param events_dict: dictionary containing selected events in the format of coffea.nanoevents.NanoEvents.array
    :type events_dict: dict{channel: coffea.nanoevents.NanoEvents.array}
    :param cutflow_dict: dictionary containing cutflow in each channel
    :type cutflow_dict: dict{channelname:{
            selection: int
            }
    """
    for keyname, event in events_dict.items():
        lepcfgname = f"signal.{keyname}"
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if not lepselname.electron.veto and (lepselname.electron.veto is not None):
            write_single_col_accu(lepselname.electron.outputs, event, output[channelname]["Objects"])
        if not lepselname.muon.veto and (lepselname.muon.veto is not None):
            write_single_col_accu(lepselname.muon.outputs, event, output[channelname]["Objects"])
        if not lepselname.tau.veto and (lepselname.tau.veto is not None):
            if lepselname.tau.count==1:
                write_single_col_accu(lepselname.tau.outputs, event, output[channelname]["Objects"])
            else:
                ptsortmask = ak.argsort(event["Tau_pt"])
                for i in range(lepselname.tau.count):
                    write_single_col_accu(lepselname.tau.outputs, event, output[channelname]["Objects"], ptsortmask, i, f"_{i+1}")
        if lepselname.pair is not None:
            pairname = lepselname.pair.name
            if pairname.find("M") != -1 and pairname.find("T") != -1:
                # Select the most energetic tauh candidate
                tau_LV = LV("Tau", event)[:,0]
                # TODO: this is for current purposes, in future, muons should not be selected using pt
                muon_LV = LV("Muon", event)[:,0]
                write_mergedLV(output[channelname], tau_LV, muon_LV)
            elif pairname.find("E") != -1 and pairname.find("T") != -1:
                tau_LV = LV("Tau", event)[:,0]
                electron_LV = LV("Electron", event)[:,0]
                write_mergedLV(output[channelname], tau_LV, electron_LV)
            elif pairname.count("T") == 2:
                tau1_LV = LV("Tau", event)[:,0]
                tau2_LV = LV("Tau", event)[:,1]
                write_mergedLV(output[channelname], tau1_LV, tau2_LV)
        output[channelname]["Cutflow"] += cutflow_dict[keyname]

def book_single_col_accu(cfg, object_dict, subfix=""):
    """Book accumulators of an object in the object_dict based on cfg
    :param cfg: nested dictionary stucture with data type as key
    :type cfg: dict
    :param object_dict: dictionary of col accumuators
    :type object_dict: dict
    :param subfix: for naming purposes in output accumulator
    :type subfix: string
    """
    if cfg.float is not None:
        for outputname in cfg.float:
            object_dict["".join([outputname, subfix])] = empty_colacc_float32()
    if cfg.uint is not None:
        for outputname in cfg.uint:
            object_dict["".join([outputname, subfix])] = empty_colacc_int()
    if cfg.bool is not None:
        for outputname in cfg.bool:
            object_dict["".join([outputname, subfix])] = empty_colacc_bool()

def write_single_col_accu(cfg, event, object_dict, sort_by=None, index=None, subfix=""):
    """Write to accumulators of different data types for one object in the object_dict based on cfg
    :param cfg: nested dictionary stucture with data type as key
    :type cfg: dict
    :param event: events loaded directly from NANOAOD files and contain all the fields
    :type event: coffea.nanoevents.NanoEvents.array
    :param object_dict: a dictionary of col accumulators only
    :type object_dict: dict
    :param sort_by: a mask to sort the awkward array for the events field
    :type sort_by: awkward.highlevel.Array (True/False)
    :param index: the index of the object in each event (should there be multiple per events)
    :type index: int
    :param subfix: for naming purposes in output accumulator
    :type subfix: string
    """ 
    if cfg.float is not None:
        for outputname, nanoaodname in cfg.float.items():
            object_dict["".join([outputname, subfix])] += ak_to_colacc(event[nanoaodname], sort_by, index)
    if cfg.uint is not None:
        for outputname, nanoaodname in cfg.uint.items():
            object_dict["".join([outputname, subfix])] += ak_to_colacc(event[nanoaodname], sort_by, index)
    if cfg.bool is not None:
        for outputname, nanoaodname in cfg.bool.items():
            object_dict["".join([outputname, subfix])] += ak_to_colacc(event[nanoaodname], sort_by, index)
                
            
def write_mergedLV(output, leptonLV1, leptonLV2, keyname = "Objects"):
    """ Write the various properties of LV(l+l) to the output accumulator
    :param output: output accumulator
    :type output: self.accumulator.identity
    :param leptonLV1: Lorentz four-vector of the first vector
    :type leptonLV1: vector.backends.awkward.MomentumRecord4D
    :param leptonLV2: Lorentz four-vector of the second vector
    :type leptonLV2: vector.backends.awkward.MomentumRecord4D
    :param keyname: keyname in the output accumulator to which the values will be written
    :type keyname: string
    """
    ll_LV = leptonLV1 + leptonLV2
    output[keyname]["ptll"] += column_accumulator(ak.to_numpy(ll_LV.pt))
    output[keyname]["mll"] += column_accumulator(ak.to_numpy(ll_LV.M))
    output[keyname]["dRll"] += column_accumulator(ak.to_numpy(leptonLV1.deltaR(leptonLV2)))
            
def ak_to_colacc(var_array, sort_by=None, index=None):
    """ Converts variable-sized array (e.g., events["Electron_pt"]) to an accumulator
    :param var_array: akward array containing (preferably one) variable property per event
    :type var_array: awkward.highlevel.Array (type='n * var * float32', n being the number of events)
    :param sort_by: a mask to sort the awkward array for the events field
    :type sort_by: awkward.highlevel.Array (True/False)
    :param index: the index of the object in each event (should there be multiple per events)
    :type index: int
    :return: col_accumulator
    """
    if sort_by is not None:
        var_array = var_array[sort_by]
    if index is not None:
        return column_accumulator(ak.to_numpy(var_array[:,index]))
    return column_accumulator(ak.to_numpy(ak.flatten(var_array)))

    
def fitfun(x, a, b, c):
    return a * np.exp(-b * x) + c

def write_Jets(output, leptonLVs, jetLV, subfix="", keyname="Objects"):
    """ Write the various properties of ak4 jets to the output accumulator.
    :param output: output accumulator
    :type output: self.accumulator.identity
    :param leptonLVs: dictionary of lepton names: lepton LV's
    :type leptonLVs: dict
    :param jetLV: one ak4 jet LV
    :type jetLV: vector.backends.awkward.MomentumRecord4D
    :param subfix: for naming purposes in output accumulator to distinguish between different jets
    :type subfix: string
    """
    output[keyname]["Jet_pt"+subfix] += jetLV.pt
    output[keyname]["Jet_mass"+subfix] += jetLV.M
    output[keyname]["Jet_phi"+subfix] += jetLV.phi
    output[keyname]["Jet_eta"+subfix] += jetLV.eta
    for lepname, lepLV in leptonLVs.items():
        output[keyname]["".join(["dR_", lepname, "j", subfix])] = lepLV.deltaR(jetLV)
        output[keyname]["".join(["M_", lepname, "j", subfix])] = (lepLV+jetLV).M
        output[keyname]["".join(["pt_", lepname, "j", subfix])] = (lepLV+jetLV).pt
 

