#!/usr/bin/env python

import numpy as np
import awkward as ak
from awkward import JaggedArray, ArrayBuilder
from HHtobbtautau.src.analysis.dsmethods import *
from coffea.nanoevents.methods import candidate
import coffea.processor as processor
from coffea.nanoevents.methods import vector
import pandas as pd
import uproot

def setup_majorcandidates(events, cfg):
    """ Preselect candidate events for a target process with major object selections.

    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param filename: filename of the root file to be written with the filtered events
    :type filename: string
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """

    muons, electrons, taus = lep_properties(events)
    events_dict = {}
    
    # Set up selections for the major candidates
    if cfg.signal.channelno>=1:
        for i in range(cfg.signal.channelno):
            # Create lepselection object: coffea.process.PackedSelection
            lepselection = processor.PackedSelection()
            lepcfgname = "signal.channel"+str(i)
            channelname = cfg[lepcfgname+".name"]
            lepselname = cfg[lepcfgname+".selections"]
            # select electrons
            if lepselname.electron != None:
                eselect = lepselname.electron
                
                electronmask = (electrons.pt>eselect.ptLevel) & \
                               (abs(electrons.eta)<eselect.absetaLevel) & \
                               (electrons.bdtid>eselect.BDTLevel) & \
                               (abs(electrons.dxy)<eselect.absdxyLevel) & \
                               (abs(electrons.dz)<eselect.abdzLevel)
                               
                filter_electrons = electrons[electronmask]
                lepselection.add("ElectronSelection", (ak.num(filter_electrons)==eselect.count))
            
            # select muons
            if lepselname.muon != None:
                mselect = lepselname.muon
                muonmask = (muons.pt>mselect.ptLevel) & \
                            (abs(muons.eta)<mselect.absetaLevel) & \
                            (muons.bdtid>mselect.BDTLevel) & \
                            (abs(muons.dxy)<mselect.absdxyLevel) & \
                            (abs(muons.dz)<mselect.absdzLevel) & \
                            (muons.iso<mselect.isoLevel) & \
                            (muons.tightid>mselect.IDLevel)
                            
                filter_muons = muons[muonmask]
                lepselection.add("MuonSelection", (ak.num(filter_muons)==mselect.count))
                
            # select taus
            if lepselname.tau != None:
                if lepselname.muon!="double":
                    tselect = lepselname.tau
                    
                    taumask = (taus.pt>tselect.ptLevel) & \
                              (abs(taus.eta)<tselect.absetaLevel) & \
                              (taus.idvsjet>tselect.IDvsjetLevel) & \
                              (taus.idvsmu>tselect.IDvsmuLevel) & \
                              (taus.idvse>tselect.IDvseLevel) & \
                              (abs(taus.dz)<tselect.absdzLevel)
                    
                    filter_taus = taus[taumask]
                    lepselection.add("TauSelection", (ak.num(filter_taus)>=tselect.count))
                else:
                    tselect1 = lepselname.tau1
                    tselect2 = lepselname.tau2
                    
                    taumask = (taus)
            else:
                    lepselection.add("TauSelection", ak.num(taus)==0)
            
            # Evaluate the selection collections at this point to identify individual leptons selected
            filtered_events = events[lepselection.all(*(lepselection.names))]
            events_dict[i] = filtered_events
            events = events[~(lepselection.all(*(lepselection.names)))]
    
    return events_dict

def pair_selections(events_dict, cfg):
    """ Place pair selections on candidate events belonging to parallel target processes.

    :param events_dict: events with object preselections in different channels organized by a dictionary.
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """ 
    
    pair_dict = {}
    for i, events in events_dict.items():  
        lepcfgname = "signal.channel"+str(i) 
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        filter_muons, filter_electrons, filter_taus = lep_properties(events)
        
        # select pair properties
        if lepselname.pair != None:
            pairselect = lepselname.pair
            if pairselect.find("M") != -1 and pairselect.find("T")!= -1:
                dR = (filter_muons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True: 
                    OS = (filter_muons[:,0]["charge"] * filter_taus["charge"] < 0)
                    pairmask = OS & dR
                else: 
                    SS = (filter_muons[:,0]["charge"] * filter_taus["charge"] > 0)
                    pairmask = SS & dR
            elif pairselect.find("E") != -1 and pairselect.find("T")!= -1:
                dR = (filter_electrons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True:
                    OS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] < 0)
                    pairmask = dR & OS
                else:
                    SS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] > 0)
                    pairmask = dR & SS
            events = events[ak.any(pairmask, axis=1)]
        pair_dict[i] = events
        
    return pair_dict

def jet_selections(events_dict, cfg):
    """ Place jet selections on candidate events belonging to parallel target processes.   
    
    :param events_dict: dictionary of coffea nanoevents array with major object selections and pair selections
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array} 
    
    """
    jet_dict = {}
    for i, events in events_dict.items():  
        lepcfgname = "signal.channel"+str(i) 
        channelname = cfg[lepcfgname+".name"]
        lepselname = cfg[lepcfgname+".selections"]
        if lepselname.jet != None:
            jetselect = lepselname.jet
            ak4s, ak8s = jet_properties(events)
            
            ak4mask = (ak4s.pt > jetselect.pt) & \
                        (abs(ak4s.eta) < jetselect.absetaLevel) 
            filter_ak4s = ak4s[ak4mask]
            ak4mask = (ak.num(filter_ak4s) >= jetselect.count)
            events = events[ak4mask]
            jet_dict[i] = events
        else:
            pass
    
    return jet_dict
        
        
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
    }, with_name="PtEtaPhiECandidate", behavior=vector.behavior)

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
    }, with_name="PtEtaPhiTCandidate", behavior=candidate.behavior)

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
