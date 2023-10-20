#!/usr/bin/env python

import numpy as np
import awkward as ak
from analysis.dsmethods import *
import coffea.processor as processor
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents.methods import vector
import vector as vec
import pandas as pd
import uproot

def lepton_selections(events, cfg):
    """ Preselect candidate events for a target process with major object selections.

    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param filename: filename of the root file to be written with the filtered events
    :type filename: string
    :return: dictionary of coffea nanoevents array with major object selections. 
             keys = channel names, values = events array
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    :return: cutflow dictionary. 
             Keys = Channel names, values = {
                 Keys = Selection name, Values = Event number
             }
    :rtype: dict{dict: int}
    """

    events_dict = {}
    cutflow_dict = {}
    
    # Set up selections for the major candidates
    for i in range(cfg.signal.channelno):
        # Create lepton objects
        muons, electrons, taus = lep_properties(events)
        # Create lepselection object: coffea.analysis_tool.PackedSelection
        lepselection = PackedSelection()
        lepcfgname = "signal.channel"+str(i+1)
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        # select electrons
        eselect = lepselname.electron
        if eselect.veto is not None:
            if not eselect.veto:
                electronmask = (electrons.pt>eselect.ptLevel) & \
                            (abs(electrons.eta)<eselect.absetaLevel) & \
                            (electrons.bdtid>=eselect.BDTLevel) & \
                            (abs(electrons.dxy)<eselect.absdxyLevel) & \
                            (abs(electrons.dz)<eselect.absdzLevel)
                filter_electrons = electrons[electronmask]
                lepselection.add("ElectronSelection", ak.to_numpy(ak.num(filter_electrons)==eselect.count))
            # apply vetos    
            else:
                electronmask = (electrons.pt>eselect.ptLevel)
                filter_electrons = electrons[electronmask]
                lepselection.add("ElectronSelection", ak.to_numpy(ak.num(filter_electrons)==0))

        # select muons
        mselect = lepselname.muon
        if (mselect.veto is not None):
            if not mselect.veto:
                # TODO: figure out a way to construct the filtered muons quickly from fields
                muonmask = (muons.pt>mselect.ptLevel) & \
                            (abs(muons.eta)<mselect.absetaLevel) & \
                            (abs(muons.dxy)<mselect.absdxyLevel) & \
                            (abs(muons.dz)<mselect.absdzLevel) & \
                            (muons.iso<mselect.isoLevel) & \
                            (muons.tightid==mselect.IDLevel)
                filter_muons = muons[muonmask]
                lepselection.add("MuonSelection", ak.to_numpy(ak.num(filter_muons)==mselect.count))
            else:
                muonmask = (muons.pt>mselect.ptLevel)
                filter_muons = muons[muonmask]
                lepselection.add("MuonSelection", ak.to_numpy(ak.num(filter_muons)==0))
            
        # select taus
        tselect = lepselname.tau
        if tselect.veto is not None:
            if not tselect.veto:
                taumask = (taus.pt>tselect.ptLevel) & \
                        (abs(taus.eta)<tselect.absetaLevel) & \
                        (taus.idvsjet>=tselect.IDvsjetLevel) & \
                        (taus.idvsmu>=tselect.IDvsmuLevel) & \
                        (taus.idvse>=tselect.IDvseLevel) & \
                        (abs(taus.dz)<tselect.absdzLevel) 
                # Add sort_by_truth array mask 
                filter_taus = taus[taumask]
                lepselection.add("TauSelection", ak.to_numpy(ak.num(filter_taus)>=tselect.count))
            else:
                # TODO: for now a placeholder
                pass

        # Evaluate the selection collections at this point to identify individual leptons selected
        keyname = "".join(["channel",str(i+1)])
        cutflow_dict[keyname] = {}
        cutflow_dict[keyname]["Total"] = len(events)
        for i, sel in enumerate(lepselection.names):
            cutflow_dict[keyname][sel] = lepselection.all(*(lepselection.names[:i+1])).sum()
        events_dict[keyname] = events[lepselection.all(*(lepselection.names))]
        events = events[~(lepselection.all(*(lepselection.names)))]

    return events_dict, cutflow_dict

def pair_selections(events_dict, cutflow_dict, cfg):
    """ Place pair selections on candidate events belonging to parallel target processes.

    :param events_dict: events with object preselections in different channels organized by a dictionary.
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """ 

    for channelname, events in events_dict.items():  
        lepcfgname = f"signal.{channelname}"
        lepselname = cfg[lepcfgname+".selections"]
        filter_muons, filter_electrons, filter_taus = lep_properties(events)
        
        # select pair properties
        if lepselname.pair is not None:
            pairselect = lepselname.pair
            pairname = pairselect.name
            if pairname.find("M") != -1 and pairname.find("T")!= -1:
                dR = (filter_muons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True: 
                # TODO: OS can be changed for simplicity
                    OS = (filter_muons[:,0]["charge"] * filter_taus["charge"] < 0)
                    pairmask = OS & dR
                else: 
                    SS = (filter_muons[:,0]["charge"] * filter_taus["charge"] > 0)
                    pairmask = SS & dR
            elif pairname.find("E") != -1 and pairname.find("T")!= -1:
                dR = (filter_electrons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True:
                    OS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] < 0)
                    pairmask = dR & OS
                else:
                    SS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] > 0)
                    pairmask = dR & SS
            elif pairname.count("T") == 2:
                # TODO: place holder for now for this channel
                pass
            events = events[ak.any(pairmask, axis=1)] 
        events_dict[channelname] = events
        cutflow_dict[channelname]["Pair Selection"] = len(events)

def jet_selections(events_dict, cutflow_dict, cfg):
    """ Place jet selections on candidate events belonging to parallel target processes.   
    
    :param events_dict: dictionary of coffea nanoevents array with major object selections and pair selections
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cutflow_dict: dictionary of cutflows in different channels
    :type cutflow_dict: dict{channelname:{
            selection: int
            }
    :param cfg: configuration object
    :type cfg: DynaConf object
    """
    for channelname, events in events_dict.items():  
        lepcfgname = f"signal.{channelname}"
        lepselname = cfg[lepcfgname+".selections"] 
        comselname = cfg["signal.commonsel"]
        if comselname.ak4jet is not None:
            ak4s, ak8s = jet_properties(events)
            jetselect = comselname.ak4jet
            # Basic jet check
            ak4mask = (ak4s.pt > jetselect.ptLevel) & \
                        (abs(ak4s.eta) < jetselect.absetaLevel) 
            filter_ak4s = ak4s[ak4mask]
            ak4mask = (ak.num(filter_ak4s) >= jetselect.count)
            events = events[ak4mask]
            filter_ak4s = LV("Jet", events)
            # Overlap check 
            if not lepselname.electron.veto and (lepselname.electron.veto is not None):
                electronLV = LV("Electron", events)
                dRmask = (electronLV[:,0].deltaR(filter_ak4s) > jetselect.dRLevel)
                events = events[ak.sum(dRmask, axis=1) > jetselect.count]
            if not lepselname.muon.veto and (lepselname.muon.veto is not None):
                muonLV = LV("Muon", events)
                dRmask = (muonLV[:,0].deltaR(filter_ak4s) > jetselect.dRLevel)
                events = events[ak.sum(dRmask, axis=1) > jetselect.count]
            if not lepselname.tau.veto and (lepselname.tau.veto is not None):
                for i in range(lepselname.tau.count):
                    tauLV = LV("Tau", events)
                    dRmask = (tauLV[:,i].deltaR(filter_ak4s) > jetselect.dRLevel)
                    events = events[ak.sum(dRmask, axis=1) > jetselect.count] 
        # TODO: Fill in fatjet selection
        events_dict[channelname] = events
        cutflow_dict[channelname]["Jet selections"] = len(events)
        
        
def write_rootfiles(events_dict, cfg, filename): 
    """ Write dictionary of events in Coffea array to rootfile with a given filename.
    
    :param events_dict: dictionary of coffea nanoevents array with major object selections and pair selections
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param filename: filename of the root file to write events to
    :type filename: str
    :return: message about writing # events to a particular file
    :rtype: str
    
    """    
    with uproot.update(filename) as rootfile:
        for i, events in events_dict.items():
            channelname = cfg["signal.channel"+str(i)+".name"]
            muons, electrons, taus = lep_properties(events)
            ak4s, ak8s = jet_properties(events) 
            rootfile[channelname].extend({
                "Muon": muons,
                "Electron": electrons,
                "Tau": taus,
                "Jet": ak4s,
                "FatJet": ak8s
            })
    msg = f"Writing {len(events)} to {filename}"
    
    return msg
        

def lep_properties(events, extra=None):
    """ Return a collection of dictionaries containing the properties of leptons.
    
    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array 
    :return: dictionaries of properties
    :rtype: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray

    """
    # A collection of dictionaries, each dictionary describing a single muon candidate property
    muons = ak.zip({
        "pt": events.Muon_pt, # type events.Muon_pt: high-level awkward array
        "eta": events.Muon_eta,
        "phi": events.Muon_phi,
        "mass": events.Muon_mass,
        "charge": events.Muon_charge,
        "dxy": events.Muon_dxy,
        "dz": events.Muon_dz,
        "iso": events.Muon_pfRelIso04_all,
        "tightid": events.Muon_tightId
    }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    # A collection of dictionaries, each dictionary describing a single electron candidate property 
    electrons = ak.zip({
        "pt": events.Electron_pt, # type events.Electron_pt: high-level awkward array
        "eta": events.Electron_eta,
        "phi": events.Electron_phi,
        "mass": events.Electron_mass,
        "charge": events.Electron_charge,
        "dxy": events.Electron_dxy,
        "dz": events.Electron_dz,
        "bdtid": events.Electron_mvaIso,
    }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    # A collection of dictionaries, each dictionary describing a single tau candidate property
    taus = ak.zip({
        "pt": events.Tau_pt, # type events.Tau_pt: high-level awkward array
        "eta": events.Tau_eta,
        "phi": events.Tau_phi,
        "mass": events.Tau_mass,
        "charge": events.Tau_charge,
        "dxy": events.Tau_dxy,
        "dz": events.Tau_dz,
        "idvsjet": events.Tau_idDeepTau2018v2p5VSjet,
        "idvsmu": events.Tau_idDeepTau2018v2p5VSmu,
        "idvse": events.Tau_idDeepTau2018v2p5VSe,
    }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    return muons, electrons, taus

def jet_properties(events, extra=None):
    """ Returns the selected jet properties
    
    :param events: events
    :type events: coffea.nanoevents.methods.base.NanoEventsArray
    :param cfg: configuration file with the next level directly point to jet properties
    :type cfg: DynaConf object
    :return: AK4 and AK8 jets
    :rtype: ak.array
    """
    
    # A collection of dictionaries, each dictionary describing a single muon candidate property
    ak4s = ak.zip({
        "pt": events.Jet_pt, # type events.Muon_pt: high-level awkward array
        "eta": events.Jet_eta,
        "phi": events.Jet_phi,
        "mass": events.Jet_mass,
        "deepJetbtagger": events.Jet_btagDeepFlavB,
        "deepCSVbtagger": events.Jet_btagDeepB,
        "jetID": events.Jet_jetId,
        "deepJetCvsB": events.Jet_btagDeepFlavCvB
    }, with_name = "PtEtaPhiMCandidate", behavior = vector.behavior)

    ak8s = ak.zip({
        "pt": events.FatJet_pt,
        "eta": events.FatJet_eta,
        "phi": events.FatJet_phi,
        "mass": events.FatJet_mass,
        "jetID": events.FatJet_jetId,
        "QCD": events.FatJet_particleNetMD_QCD,
        "Xbb": events.FatJet_particleNetMD_Xbb,
        "Xcc": events.FatJet_particleNetMD_Xcc,
        "Xqq": events.FatJet_particleNetMD_Xqq,
        "Hbb": events.FatJet_btagHbb
    }, with_name = "PtEtaPhiMCandidate", behavior = vector.behavior)
    
    return ak4s, ak8s

def CF_from_selections(selection, events):
    """

    """

def LV(field_name, events, sortbypt=True):
    """ Extract four-momentum vectors of an object from NANOAOD file with methods in vector.
    
    :param field_name: the name of the object in NANOAOD format (the prefix)
    :type field_name: string
    :param events: events
    :type events: coffea.nanoevents.methods.base.NanoEventsArray
    :param sortbypt: whether sort the Lorentz vectors in each event by pt
    :type sortbypt: bool
    :return: unflattened PtEtaPhiM four vector for events
    :rtype: vector.backends.awkward.MomentumArray4D
    """
    object_ak = ak.zip({
        "pt": events[field_name+"_pt"],
        "eta": events[field_name+"_eta"],
        "phi": events[field_name+"_phi"],
        "M": events[field_name+"_mass"]
    })
    if sortbypt:
        object_ak = object_ak[ak.argsort(object_ak.pt)]
    object_LV = vec.Array(object_ak)
    return object_LV


def overlap_check(object1, object2):
    """
    Check overlap between two objects. Note: Object 1 should only contain one item per event. 

    :param object1: Contains ID of the leptons
    :type object1: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray
    :param object2: Contains ID of the jet
    :type object2: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray

    """
    
    return 0
    
def events_todf(events, objects):
    """
    Converts filtered events array (after object selections, preferably) into pandas dataframe.
    This can be useful for applying combinatorial selections
    
    :param events: filtered events array
    :type events: coffea.nanoevents.methods.base.NanoEventsArray
    :param objects: selected objects
    :type objects: list
    :return df: filtered events dataframe
    :type df: pandas.DataFrame
    """
