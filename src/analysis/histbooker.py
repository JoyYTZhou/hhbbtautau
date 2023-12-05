import re
import copy

from coffea.processor import dict_accumulator, column_accumulator, defaultdict_accumulator
import numpy as np
import awkward as ak
import hist
import vector as vec
from collections import ChainMap


from analysis.mathutility import *
from analysis.selutility import LV, LV_from_zipped
from analysis.dsmethods import *


def empty_colacc_uint():
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
    object_outputs = cfg.signal.outputs
    for i in range(cfg.signal.channelno):
        object_accs = {}
        lepcfgname = "signal.channel"+str(i+1)
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if not lepselname.electron.veto and (lepselname.electron.veto is not None):
            book_single_col_accu(object_outputs.Electron, "Electron", object_accs)
        if not lepselname.muon.veto and (lepselname.muon.veto is not None):
            book_single_col_accu(object_outputs.Muon, "Muon", object_accs)
        if not lepselname.tau.veto and (lepselname.tau.veto is not None):
            if lepselname.tau.count == 1:
                book_single_col_accu(object_outputs.Tau, "Tau", object_accs)
            else:
                for i in range(lepselname.tau.count): 
                    subfix = str(i+1)
                    book_single_col_accu(object_outputs.Tau, "Tau", object_accs, subfix)
        book_single_col_accu(object_outputs.FatJet, "FatJet", object_accs, "1")
        book_single_col_accu(object_outputs.FatJet, "FatJet", object_accs, "2")
        # book column accumulators for combinatorials of objects kinematics
        if lepselname.pair is not None:
            object_accs["dRll"] = empty_colacc_float32()
            object_accs["mll"] = empty_colacc_float32()
            object_accs["ptll"] = empty_colacc_float32()
        if cfg.signal.commonsel is not None and cfg.signal.commonsel.ak4jet is not None:
            object_accs["ptjj"] = empty_colacc_float32()
            object_accs["mjj"] = empty_colacc_float32()
            object_accs["dRjj"] = empty_colacc_float32()
            # TODO: add lepton jet
        selected_events[channelname] = dict_accumulator({
            "Cutflow": accu_int(),
            "Objects": dict_accumulator(object_accs)
        })
    combined_accumulator = dict_accumulator(selected_events)
    
    return combined_accumulator

def hbbtautau_accumulate(output, cfg, cutflow_dict, object_dict):
    """ Fill in the accumulator for the current process. 
    :param output: output accumulator
    :type output: self.accumulator.identity
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param cutflow_dict: dictionary containing cutflow in each channel
    :type cutflow_dict: dict{keyname:{
            selection: int
            }
    :param object_dict: dictionary of zipped lepton objects
    :type object_dict: dict{object name: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray}
    """
    outputscfg = cfg.signal.outputs
    for keyname in object_dict.keys():
        channelname = cfg.signal[keyname]["name"]
        lepselname = cfg.signal[keyname]["selections"]
        object_accu_dict = output[channelname]["Objects"]
        if not lepselname.electron.veto and (lepselname.electron.veto is not None):
            write_single_col_accu(outputscfg["Electron"], "Electron", 
                              object_dict[keyname], object_accu_dict)
        if not lepselname.muon.veto and (lepselname.muon.veto is not None):
            write_single_col_accu(outputscfg["Muon"], "Muon", 
                              object_dict[keyname], object_accu_dict)
        if not lepselname.tau.veto and (lepselname.tau.veto is not None):
            if lepselname.tau.count==1:
                write_single_col_accu(outputscfg["Tau"], "Tau", 
                              object_dict[keyname], object_accu_dict)
            else:
                ptsortmask = ak.argsort(object_dict[keyname]["Tau"]["pt"])
                for j in range(lepselname.tau.count):
                    write_single_col_accu(outputscfg.Tau, "Tau", 
                              object_dict[keyname], object_accu_dict, ptsortmask, j, str(j+1))
        if lepselname.pair is not None:
            pairname = lepselname.pair.name
            if pairname.find("M") != -1 and pairname.find("T") != -1:
                # Select the most energetic tauh candidate
                tau_LV = LV_from_zipped(object_dict[keyname]["Tau"])[:,0]
                muon_LV = LV_from_zipped(object_dict[keyname]["Muon"])[:,0]
                write_mergedLV(object_accu_dict, tau_LV, muon_LV)
            elif pairname.find("E") != -1 and pairname.find("T") != -1:
                tau_LV = LV_from_zipped(object_dict[keyname]["Tau"])[:,0]
                electron_LV = LV_from_zipped(object_dict[keyname]["Electron"])[:,0]
                write_mergedLV(object_accu_dict, tau_LV, electron_LV)
            elif pairname.count("T") == 2:
                tau1_LV = LV_from_zipped(object_dict[keyname]["Tau"])[:,0]
                tau2_LV = LV_from_zipped(object_dict[keyname]["Tau"])[:,1]
                write_mergedLV(object_accu_dict, tau1_LV, tau2_LV)
        output[channelname]["Cutflow"] += cutflow_dict[keyname]

def book_single_col_accu(object_cfg, object_name, object_accs, subfix=""):
    """Book accumulators of an object in the object_accs based on cfg
    :param object_cfg: a dictionary stucture {data_type: object_property}
    :type object_cfg: dict
    :param object_name: name of the object
    :type object_name: string
    :param object_accs: dictionary of col accumuators
    :type object_accs: dict
    :param subfix: for naming purposes in output accumulator
    :type subfix: string
    """
    if object_cfg.get("float", None) is not None:
        for property in object_cfg['float'].keys():
            object_accs["".join([object_name, '_', property, subfix])] = empty_colacc_float32()
    if object_cfg.get("uint", None) is not None:
        for property in object_cfg['uint'].keys():
            object_accs["".join([object_name, '_', property, subfix])] = empty_colacc_uint()
    if object_cfg.get("int", None) is not None:
        for property in object_cfg['int'].keys():
            object_accs["".join([object_name, '_', property, subfix])] = empty_colacc_int64()
    if object_cfg.get("bool", None) is not None:
        for property in object_cfg['bool'].keys():
            object_accs["".join([object_name, '_', property, subfix])] = empty_colacc_bool()

def write_single_col_accu(cfg, object_name, object_dict, object_accs, sort_by=None, index=None, subfix=""):
    """Write to accumulators of different data types for one object in the object dictionary based on cfg
    :param cfg: nested dictionary stucture of a single object with the data type as key
    :type cfg: dict
    :param object_name: name of the object
    :type object_name: str
    :param object_dict: dictionary of zipped lepton objects
    :type object_dict: dict{object name: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray}
    :param object_accs: a dictionary of col accumulators only
    :type object_accs: dict
    :param sort_by: a mask to sort the awkward array for the events field
    :type sort_by: awkward.highlevel.Array (True/False/None)
    :param index: the index of the object in each event (should there be multiple per events)
    :type index: int
    :param subfix: for naming purposes in output accumulator
    :type subfix: string
    """ 
    vars_dict = dict(ChainMap(*cfg.values()))
    zipped_object = object_dict[object_name]
    for var in vars_dict.keys():
        object_accs["".join([object_name, "_", var, subfix])] += ak_to_colacc(zipped_object[var], sort_by, index)
        
def write_mergedLV(output, leptonLV1, leptonLV2):
    """ Write the various properties of LV(l+l) to the output accumulator
    :param output: output accumulator
    :type output: self.accumulator.identity
    :param leptonLV1: Lorentz four-vector of the first vector
    :type leptonLV1: vector.backends.awkward.MomentumRecord4D
    :param leptonLV2: Lorentz four-vector of the second vector
    :type leptonLV2: vector.backends.awkward.MomentumRecord4D
    """
    ll_LV = leptonLV1 + leptonLV2
    output["ptll"] += column_accumulator(ak.to_numpy(ll_LV.pt))
    output["mll"] += column_accumulator(ak.to_numpy(ll_LV.M))
    output["dRll"] += column_accumulator(ak.to_numpy(leptonLV1.deltaR(leptonLV2)))
            
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

def accumulate_dicts(dict_list):
    """ Accumulate a list of dictionaries into one dictionary.
    
    :param dict_list: list of dictionaries with accumulators as values
    :type dict_list: list
    :return: accumulated dictionary
    :rtype: dict
    """
    result = dict_accumulator()
    for d in dict_list:
        result.add(d)
    return result
    
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
 

