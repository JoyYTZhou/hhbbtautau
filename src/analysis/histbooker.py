import re
import copy

import coffea.processor as processor
import numpy as np
import awkward as ak
import hist
import vector as vec


from src.analysis.mathutility import *
from src.analysis.selutility import LV
from src.analysis.dsmethods import *


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

def defaultdict_accumulator_of_empty_colacc_int64():
    return processor.defaultdict_accumulator(empty_colacc_int64)
def defaultdict_accumulator_of_empty_colacc_float64():
    return processor.defaultdict_accumulator(empty_colacc_float64)
def defaultdict_accumulator_of_empty_colacc_float32():
    return processor.defaultdict_accumulator(empty_colacc_float32)
def defaultdict_accumulator_of_empty_colacc_float16():
    return processor.defaultdict_accumulator(empty_colacc_float16)
def defaultdict_accumulator_of_empty_colacc_bool():
    return processor.defaultdict_accumulator(empty_colacc_bool)


def hhtobbtautau_accumulator(cfg):
    """ Construct an accumulator for hhtobbtautau analysis.
    
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """

    selected_events = {}
    if cfg.signal.channelno >= 1:
        for i in range(cfg.signal.channelno):
            event_dict = {}
            lepcfgname = "signal.channel"+str(i)
            channelname = cfg[lepcfgname+".name"]
            lepselname = cfg[lepcfgname+".selections"]
            if not lepselname.electron.veto:
                for outputname in lepselname.electron.outputs:
                    event_dict[outputname] = empty_colacc_float32
            if not lepselname.muon.veto:
                for outputname in lepselname.muon.outputs:
                    event_dict[outputname] = empty_colacc_float32
            if not lepselname.tau.veto:
                if lepselname.tau.count == 1:
                    event_dict["Tau_pt"] = empty_colacc_float32
                    event_dict["Tau_mass"] = empty_colacc_float32
                    event_dict["Tau_eta"] = empty_colacc_float32
                    event_dict["Tau_dz"] = empty_colacc_float32
                    event_dict["Tau_phi"] = empty_colacc_float32
                else:
                    for i in range(1,lepselname.tau.count+1): 
                        tauname = "Tau"+str(i) 
                        event_dict[tauname+"_pt"] = empty_colacc_float32
                        event_dict[tauname+"_mass"] = empty_colacc_float32
                        event_dict[tauname+"_eta"] = empty_colacc_float32
                        event_dict[tauname+"_phi"] = empty_colacc_float32
                        event_dict[tauname+"_dz"] = empty_colacc_float32
            if lepselname.pair != None:
                pairname = lepselname.pair.name
                event_dict["dR_"+str(pairname)] = empty_colacc_float32
                event_dict["mass_"+str(pairname)] = empty_colacc_float32
                event_dict["pt_"+str(pairname)] = empty_colacc_float32
            
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
    :return output: accumulated accumulator
    :param output: self.accumulator
    """
    for keyname, event in events_dict.items():
        lepcfgname = f"signal.{keyname}"
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if not lepselname.electron.veto:
            for outputname, nanoaodname in lepselname.electron.outputs: 
                output["Objects"][outputname] += event[nanoaodname]
        if not lepselname.muon.veto:
            for outputname, nanoaodname in lepselname.muon.outputs: 
                output["Objects"][outputname] += event[nanoaodname]
        if not lepselname.tau.veto:
            for outputname, nanoaodname in lepselname.tau.outputs:
                if lepselname.tau.count==1:
                    output["Objects"][outputname] += event[nanoaodname]
                else:
                    pass
                    # TODO: place holder for now
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
    output[keyname]["ptll"] = ll_LV.pt
    output[keyname]["mll"] = ll_LV.M
    output[keyname]["dRll"] = leptonLV1.deltaR(leptonLV2)
            
def ak_to_np(var_array):
    """ Converts variable-sized array (e.g., events["Electron_pt"]) to an accumulator
    :param var_array: akward array containing one variable per event
    :type var_array: awkward.highlevel.Array (type='n * var * float32', n being the number of events)
    :return: col_accumulator
    """
    return processor.column_accumulator(ak.to_numpy(ak.flatten(var_array)))
    
    
    
def fitfun(x, a, b, c):
    return a * np.exp(-b * x) + c



def theory_weights_vbf(weights, df, evaluator, gen_v_pt, mjj):
    if df['is_lo_w']:
        theory_weights = evaluator["qcd_nlo_w_2017_2d"](mjj, gen_v_pt) * evaluator["ewk_nlo_w"](gen_v_pt)
    elif df['is_lo_w_ewk']:
        theory_weights = evaluator["qcd_nlo_w_ewk"](gen_v_pt, mjj)
    elif df['is_lo_z']:
        w_ewk = evaluator["ewk_nlo_z"](gen_v_pt)
        if df['is_lo_znunu']:
            w_qcd = evaluator["qcd_nlo_znn_2017_2d"](gen_v_pt, mjj)
        else:
            w_qcd = evaluator["qcd_nlo_z_2017_2d"](mjj, gen_v_pt)
        theory_weights = w_ewk * w_qcd
    elif df['is_lo_z_ewk']:
        theory_weights = evaluator["qcd_nlo_z_ewk"](gen_v_pt, mjj)
    elif df['is_nlo_w']:
        theory_weights = evaluator["ewk_nlo_w"](gen_v_pt)
    elif df['is_nlo_z']:
        theory_weights = evaluator["ewk_nlo_z"](gen_v_pt)
    elif df['is_lo_g']:
        theory_weights = evaluator["qcd_nlo_g_2017_2d"](mjj, gen_v_pt) * evaluator["ewk_nlo_g"](gen_v_pt)
    else:
        theory_weights = np.ones(df.size)

    # Guard against invalid input pt
    invalid = (gen_v_pt <=0) | np.isinf(gen_v_pt) | np.isnan(gen_v_pt)
    theory_weights[invalid] = 1

    weights.add('theory', theory_weights)

    return weights

def pileup_weights(weights, df, evaluator, cfg):

    if cfg.SF.PILEUP.MODE == 'nano':
        pu_weight = df['puWeight']
    elif cfg.SF.PILEUP.MODE == 'manual':
        pu_weight = evaluator['pileup'](df['Pileup_nTrueInt'])
    else:
        raise RuntimeError(f"Unknown value for cfg.PILEUP.MODE: {cfg.PILEUP.MODE}.")

    # Cap weights just in case
    pu_weight[np.abs(pu_weight)>5] = 1
    weights.add("pileup", pu_weight)
    return weights

def photon_trigger_sf(weights, photons, df):
    """MC-to-data photon trigger scale factor.

    The scale factor is obtained by separately fitting the
    trigger turn-on with a sigmoid function in data and MC.
    The scale factor is then the ratio of the two sigmoid
    functions as a function of the photon pt.

    :param weights: Weights object to write information into
    :type weights: WeightsContainer
    :param photons: Photon candidates
    :type photons: JaggedCandidateArray
    :param df: Data frame
    :type df: LazyDataFrame
    """
    year = extract_year(df['dataset'])
    x = photons.pt.max()
    if year == 2016:
        sf =  np.ones(df.size)
    elif year == 2017:
        sf = sigmoid(x,0.335,217.91,0.065,0.996) / sigmoid(x,0.244,212.34,0.050,1.000)
    elif year == 2018:
        sf = sigmoid(x,1.022, 218.39, 0.086, 0.999) / sigmoid(x, 0.301,212.83,0.062,1.000)

    sf[np.isnan(sf) | np.isinf(sf)] == 1

    weights.add("trigger_photon", sf)

def candidate_weights(weights, df, evaluator, muons, electrons, photons, cfg):
    year = extract_year(df['dataset'])
    # Muon ID and Isolation for tight and loose WP
    # Function of pT, eta (Order!)
    weight_muons_id_tight = evaluator['muon_id_tight'](muons[df['is_tight_muon']].pt, muons[df['is_tight_muon']].abseta).prod()
    weight_muons_iso_tight = evaluator['muon_iso_tight'](muons[df['is_tight_muon']].pt, muons[df['is_tight_muon']].abseta).prod()

    if cfg.SF.DIMUO_ID_SF.USE_AVERAGE:
        tight_dimuons = muons[df["is_tight_muon"]].distincts()
        t0 = (evaluator['muon_id_tight'](tight_dimuons.i0.pt, tight_dimuons.i0.abseta) \
             * evaluator['muon_iso_tight'](tight_dimuons.i0.pt, tight_dimuons.i0.abseta)).prod()
        t1 = (evaluator['muon_id_tight'](tight_dimuons.i1.pt, tight_dimuons.i1.abseta) \
             * evaluator['muon_iso_tight'](tight_dimuons.i1.pt, tight_dimuons.i1.abseta)).prod()
        l0 = (evaluator['muon_id_loose'](tight_dimuons.i0.pt, tight_dimuons.i0.abseta) \
             * evaluator['muon_iso_loose'](tight_dimuons.i0.pt, tight_dimuons.i0.abseta)).prod()
        l1 = (evaluator['muon_id_loose'](tight_dimuons.i1.pt, tight_dimuons.i1.abseta) \
             * evaluator['muon_iso_loose'](tight_dimuons.i1.pt, tight_dimuons.i1.abseta)).prod()
        weights_2m_tight = 0.5*( l0 * t1 + l1 * t0)
        weights.add("muon_id_iso_tight", weight_muons_id_tight*weight_muons_iso_tight*(tight_dimuons.counts!=1) + weights_2m_tight*(tight_dimuons.counts==1))
    else:
        weights.add("muon_id_iso_tight", weight_muons_id_tight*weight_muons_iso_tight )

    weights.add("muon_id_loose", evaluator['muon_id_loose'](muons[~df['is_tight_muon']].pt, muons[~df['is_tight_muon']].abseta).prod())
    weights.add("muon_iso_loose", evaluator['muon_iso_loose'](muons[~df['is_tight_muon']].pt, muons[~df['is_tight_muon']].abseta).prod())

    # Electron ID and reco
    # Function of eta, pT (Other way round relative to muons!)

    # For 2017, the reco SF is split below/above 20 GeV
    if year == 2017:
        high_et = electrons.pt>20
        ele_reco_sf = evaluator['ele_reco'](electrons.etasc[high_et], electrons.pt[high_et]).prod()
        ele_reco_sf *= evaluator['ele_reco_pt_lt_20'](electrons.etasc[~high_et], electrons.pt[~high_et]).prod()
    else:
        ele_reco_sf = evaluator['ele_reco'](electrons.etasc, electrons.pt).prod()
    weights.add("ele_reco", ele_reco_sf)
    # ID/iso SF is not split
    # in case of 2 tight electrons, we want to apply 0.5*(T1L2+T2L1) instead of T1T2
    weights_electrons_tight = evaluator['ele_id_tight'](electrons[df['is_tight_electron']].etasc, electrons[df['is_tight_electron']].pt).prod()
    if cfg.SF.DIELE_ID_SF.USE_AVERAGE:
        tight_dielectrons = electrons[df["is_tight_electron"]].distincts()
        l0 = evaluator['ele_id_loose'](tight_dielectrons.i0.etasc, tight_dielectrons.i0.pt).prod()
        t0 = evaluator['ele_id_tight'](tight_dielectrons.i0.etasc, tight_dielectrons.i0.pt).prod()
        l1 = evaluator['ele_id_loose'](tight_dielectrons.i1.etasc, tight_dielectrons.i1.pt).prod()
        t1 = evaluator['ele_id_tight'](tight_dielectrons.i1.etasc, tight_dielectrons.i1.pt).prod()
        weights_2e_tight = 0.5*( l0 * t1 + l1 * t0)
        weights.add("ele_id_tight", weights_electrons_tight*(tight_dielectrons.counts!=1) + weights_2e_tight*(tight_dielectrons.counts==1))
    else:
        weights.add("ele_id_tight", weights_electrons_tight)
    weights.add("ele_id_loose", evaluator['ele_id_loose'](electrons[~df['is_tight_electron']].etasc, electrons[~df['is_tight_electron']].pt).prod())

    # Photon ID and electron veto
    if cfg.SF.PHOTON.USETNP:
        weights.add("photon_id_tight", evaluator['photon_id_tight_tnp'](np.abs(photons[df['is_tight_photon']].eta)).prod())
    else:
        weights.add("photon_id_tight", evaluator['photon_id_tight'](photons[df['is_tight_photon']].eta, photons[df['is_tight_photon']].pt).prod())

    if year == 2016:
        csev_weight = evaluator["photon_csev"](photons.abseta, photons.pt).prod()
    elif year == 2017:
        csev_sf_index = 0.5 * photons.barrel + 3.5 * ~photons.barrel + 1 * (photons.r9 > 0.94) + 2 * (photons.r9 <= 0.94)
        csev_weight = evaluator['photon_csev'](csev_sf_index).prod()
    elif year == 2018:
        csev_weight = evaluator['photon_csev'](photons.pt, photons.abseta).prod()
    csev_weight[csev_weight==0] = 1
    weights.add("photon_csev", csev_weight)

    return weights

def data_driven_qcd_dataset(dataset):
    """Dataset name to use for data-driven QCD estimate"""
    year = extract_year(dataset)
    return f"QCD_data_{year}"

def photon_impurity_weights(photon_pt, year):
    """Photon impurity as a function of pt

    :param photon_pt: Photon pt
    :type photon_pt: 1D array
    :param year: Data-taking year
    :type year: int
    :return: Weights
    :rtype: 1Darray
    """
    if year == 2017:
        a = 6.35
        b = 4.61e-3
        c = 1.05
    elif year==2018:
        a = 11.92
        b = 8.28e-3
        c = 1.55
    elif year==2016:
        return np.ones(photon_pt.size)

    # Remember to multiply by 1e-2,
    # because fit is done to percentages
    return 1e-2*exponential(photon_pt, a, b, c)