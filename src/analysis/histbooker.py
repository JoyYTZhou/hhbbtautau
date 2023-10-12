import re
import copy

import coffea.processor as processor
import numpy as np
import awkward as ak
import hist
import vector as vec
import numba


from analysis.mathutility import *
from analysis.selutility import LV
from analysis.dsmethods import *


def empty_colacc_int():
    return processor.column_accumulator(np.array([],dtype=np.uint64))
def empty_colacc_int64():
    return processor.column_accumulator(np.array([],dtype=np.int64))
def empty_colacc_float64():
    return processor.column_accumulator(np.array([],dtype=np.float64))
def empty_colacc_float32():
    return processor.column_accumulator(np.array([],dtype=np.float32))
def empty_colacc_float16():
    return processor.column_accumulator(np.array([],dtype=np.float16))
def empty_colacc_bool():
    return processor.column_accumulator(np.array([],dtype=np.bool))
def accu_int():
    return processor.defaultdict_accumulator(int)

def hhtobbtautau_accumulator(cfg):
    """ Construct an accumulator for hhtobbtautau analysis.
    
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """

    selected_events = {}
    for i in range(cfg.signal.channelno):
        event_dict = {}
        lepcfgname = "signal.channel"+str(i+1)
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if (lepselname.electron != None) and not lepselname.electron.veto:
            book_single_col_accu(lepselname.electron.outputs, event_dict)
        if (lepselname.muon != None) and not lepselname.muon.veto:
            book_single_col_accu(lepselname.muon.outputs, event_dict)
        if (lepselname.tau != None) and not lepselname.tau.veto:
            if lepselname.tau.count == 1:
                book_single_col_accu(lepselname.tau.outputs, event_dict)
            else:
                for i in range(lepselname.tau.count): 
                    subfix = f"_{i}"
                    book_single_col_accu(lepselname.tau.outputs, event_dict, subfix)
        if lepselname.pair != None:
            pairname = lepselname.pair.name
            event_dict["dR_"+str(pairname)] = empty_colacc_float32()
            event_dict["mass_"+str(pairname)] = empty_colacc_float32()
            event_dict["pt_"+str(pairname)] = empty_colacc_float32()
        if cfg.signal.commonsel != None and cfg.signal.commonsel.ak4jet != None:
            event_dict["ptjj"] = empty_colacc_float32()
            event_dict["mjj"] = empty_colacc_float32()
            event_dict["dRjj"] = empty_colacc_float32()
            # TODO: add lepton jet
        selected_events[channelname] = processor.dict_accumulator({
            "Cutflow": accu_int,
            "Objects": processor.defaultdict_accumulator(event_dict)
        })
    combined_accumulator = processor.dict_accumulator(selected_events)
    
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
        if not lepselname.electron.veto:
            for outputname, nanoaodname in lepselname.electron.outputs.items(): 
                output[channelname]["Objects"][outputname] += ak_to_np(event[nanoaodname])
        if not lepselname.muon.veto:
            for outputname, nanoaodname in lepselname.muon.outputs.items(): 
                output[channelname]["Objects"][outputname] += ak_to_np(event[nanoaodname])
        if not lepselname.tau.veto:
            for outputname, nanoaodname in lepselname.tau.outputs.items():
                if lepselname.tau.count==1:
                    output[channelname]["Objects"][outputname] += ak_to_np(event[nanoaodname])
                else:
                    ptsortmask = ak.argsort(event["Tau_pt"])
                    for i in range(lepselname.tau.count):
                        output[channelname]["Objects"][outputname+"_"+str(i+1)] += ak_to_np(event[nanoaodname][ptsortmask][:,i])
        if lepselname.pair != None:
            pairname = lepselname.pair.name
            if pairname.find("M") != -1 and pairname.find("T") != -1:
                # Select the most energetic tauh candidate
                tau_LV = LV("Tau", event)[:,0]
                muon_LV = ak.flatten(LV("Muon", event))
                write_mergedLV(output, tau_LV, muon_LV)
            elif pairname.find("E") != -1 and pairname.find("T") != -1:
                tau_LV = LV("Tau", event)[:,0]
                electron_LV = ak.flatten(LV("Electron", event))
                write_mergedLV(output, tau_LV, electron_LV)
            elif pairname.count("T") == 2:
                tau1_LV = LV("Tau", event)[:,0]
                tau2_LV = LV("Tau", event)[:,1]
                write_mergedLV(output, tau1_LV, tau2_LV)
        output[channelname]["Cutflow"] += cutflow_dict[keyname]

@numba.njit
def book_single_col_accu(cfg, event_dict, subfix=""):
    """Book accumulators of a single type in the event_dict based on cfg
    :param cfg: nested dictionary stucture with data type as key
    :type cfg: dict
    :param event_dict: dictionary of col accumuators
    :type event_dict: dict
    :param subfix: for naming purposes in output accumulator
    :type subfix: string
    """
    if cfg.float != None:
        for outputname in cfg.float:
            event_dict["".join([outputname, subfix])] = empty_colacc_float32()
    if cfg.uint != None:
        for outputname in cfg.uint:
            event_dict["".join([outputname, subfix])] = empty_colacc_int()
    if cfg.bool != None:
        for outputname in cfg.bool:
            event_dict["".join([outputname, subfix])] = empty_colacc_bool()
                
            
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
    output[keyname]["ptll"] += ll_LV.pt
    output[keyname]["mll"] += ll_LV.M
    output[keyname]["dRll"] += leptonLV1.deltaR(leptonLV2)
            
def ak_to_np(var_array):
    """ Converts variable-sized array (e.g., events["Electron_pt"]) to an accumulator
    :param var_array: akward array containing (preferably one) variable per event
    :type var_array: awkward.highlevel.Array (type='n * var * float32', n being the number of events)
    :return: col_accumulator
    """
    return processor.column_accumulator(ak.to_numpy(ak.flatten(var_array)))

    
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
 

