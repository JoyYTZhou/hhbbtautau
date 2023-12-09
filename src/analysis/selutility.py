#!/usr/bin/env python

import awkward as ak
from analysis.dsmethods import *
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents.methods import vector
import vector as vec
import uproot
from collections import ChainMap

def trigger_selections(events, cfg):
    """ Tirgger select candidate events for a target process with major object selections.

    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array
    :param cfg: configuration object
    :type cfg: DynaConf object
    """
    for obj_prop, value in cfg.signal.triggersel.items():
        obj_mask = events[obj_prop] >= value
        events = events[obj_mask]
     
def lepton_selections(events, cfg):
    """ Preselect candidate events for a target process with major object selections.

    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return events_dict: dictionary of coffea nanoevents array with major object selections. 
             keys = channel names, values = events array
    :rtype events_dict: dict{int: coffea.nanoevents.NanoEvents.array}
    :return cutflow_dict: cutflow dictionary. 
             Keys = Channel names, values = {
                 Keys = Selection name, Values = Event number
             }
    :rtype cutflow_dict: dict{dict: int}
    :return object_dict: dictionary of zipped objects
    :rtype object_dict: dict{object name: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray}
    """

    events_dict = {}
    cutflow_dict = {}
    object_dict = {}

    # Set up selections for the major candidates
    for i in range(cfg.signal.channelno):
        keyname = "".join(["channel", str(i+1)])
        # Create lepton objects
        muons, electrons, taus = zip_lepproperties(cfg.signal.outputs, events)
        # Create lepselection object: coffea.analysis_tool.PackedSelection
        lepselection = PackedSelection()
        lepcfgname = "signal.channel"+str(i+1)
        lepselname = cfg[lepcfgname+".selections"]
        # select electrons
        eselect = lepselname.electron
        if eselect.veto is not None:
            if not eselect.veto:
                electronmask = (electrons.pt > eselect.ptLevel) & \
                               (abs(electrons.eta) < eselect.absetaLevel) & \
                               (electrons.bdtid >= eselect.BDTLevel) & \
                               (abs(electrons.dxy) < eselect.absdxyLevel) & \
                               (abs(electrons.dz) < eselect.absdzLevel)
                electrons = electrons[electronmask]
                lepselection.add("ElectronSelection", ak.to_numpy(
                    ak.num(electrons) == eselect.count))
            # apply vetos
            else:
                electronmask = (electrons.pt > eselect.ptLevel)
                electrons = electrons[electronmask]
                lepselection.add("ElectronSelection",
                                 ak.to_numpy(ak.num(electrons) == 0))

        # select muons
        mselect = lepselname.muon
        if (mselect.veto is not None):
            if not mselect.veto:
                # TODO: figure out a way to construct the filtered muons quickly from fields
                muonmask = (muons.pt > mselect.ptLevel) & \
                           (abs(muons.eta) < mselect.absetaLevel) & \
                           (abs(muons.dxy) < mselect.absdxyLevel) & \
                           (abs(muons.dz) < mselect.absdzLevel) & \
                           (muons.iso < mselect.isoLevel) & \
                           (muons.tightid == mselect.IDLevel)
                muons = muons[muonmask]
                lepselection.add("MuonSelection", ak.to_numpy(
                    ak.num(muons) == mselect.count))
            else:
                muonmask = (muons.pt > mselect.ptLevel)
                muons = muons[muonmask]
                lepselection.add(
                    "MuonSelection", ak.to_numpy(ak.num(muons) == 0))

        # select taus
        tselect = lepselname.tau
        if tselect.veto is not None:
            if not tselect.veto:
                taumask = (taus.pt > tselect.ptLevel) & \
                    (abs(taus.eta) < tselect.absetaLevel) & \
                    (taus.idvsjet >= tselect.IDvsjetLevel) & \
                    (taus.idvsmu >= tselect.IDvsmuLevel) & \
                    (taus.idvse >= tselect.IDvseLevel) & \
                    (abs(taus.dz) < tselect.absdzLevel)
                taus = taus[taumask]
                lepselection.add("TauSelection", ak.to_numpy(
                    ak.num(taus) == tselect.count))
            else:
                # TODO: for now a placeholder
                pass

        # Evaluate the selection collections at this point to identify individual leptons selected
        cutflow_dict[keyname] = {}
        cutflow_dict[keyname]["Total"] = len(events)
        for i, sel in enumerate(lepselection.names):
            cutflow_dict[keyname][sel] = lepselection.all(
                *(lepselection.names[:i+1])).sum()
        all_lep_mask = lepselection.all(*(lepselection.names))
        events_dict[keyname] = events[all_lep_mask]
        object_dict[keyname] = {"Electron": electrons[all_lep_mask],
                                "Muon": muons[all_lep_mask],
                                "Tau": taus[all_lep_mask]}
        events = events[~(lepselection.all(*(lepselection.names)))]

    return events_dict, cutflow_dict, object_dict


def pair_selections(events_dict, cutflow_dict, object_dict, cfg):
    """ Place pair selections on candidate events belonging to parallel target processes.

    :param events_dict: events with object preselections in different channels organized by a dictionary.
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cfg: configuration object
    :type cfg: DynaConf object
    :return: dictionary of coffea nanoevents array with major object selections
    :rtype: dict{int: coffea.nanoevents.NanoEvents.array}
    """

    for keyname, events in events_dict.items():
        lepcfgname = f"signal.{keyname}"
        lepselname = cfg[lepcfgname+".selections"]
        filter_muons = object_dict[keyname]['Muon']
        filter_electrons = object_dict[keyname]['Electron']
        filter_taus = object_dict[keyname]["Tau"]

        # select pair properties
        if lepselname.pair is not None:
            pairselect = lepselname.pair
            pairname = pairselect.name
            if pairname.find("M") != -1 and pairname.find("T") != -1:
                dR = (filter_muons[:, 0].delta_r(
                    filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True:
                    # TODO: OS can be changed for simplicity
                    OS = (filter_muons[:, 0]["charge"]
                          * filter_taus["charge"] < 0)
                    pairmask = OS & dR
                else:
                    SS = (filter_muons[:, 0]["charge"]
                          * filter_taus["charge"] > 0)
                    pairmask = SS & dR
            elif pairname.find("E") != -1 and pairname.find("T") != -1:
                dR = (filter_electrons[:, 0].delta_r(
                    filter_taus) >= pairselect.dRLevel)
                if pairselect.OS == True:
                    OS = (filter_electrons[:, 0]["charge"]
                          * filter_taus["charge"] < 0)
                    pairmask = dR & OS
                else:
                    SS = (filter_electrons[:, 0]["charge"]
                          * filter_taus["charge"] > 0)
                    pairmask = dR & SS
            elif pairname.count("T") == 2:
                # TODO: place holder for now for this channel
                pairmask = ak.ones_like(filter_taus['charge'])
            filter_taus = filter_taus[pairmask]
            object_dict[keyname].update({"Tau": filter_taus})
            apply_mask_on_all(object_dict[keyname], ak.any(pairmask, axis=1))
            events = events[ak.any(pairmask, axis=1)]
        events_dict[keyname] = events
        cutflow_dict[keyname]["Pair Selection"] = len(events)


def jet_selections(events_dict, cutflow_dict, object_dict, cfg):
    """ Place jet selections on candidate events belonging to parallel target processes.   

    :param events_dict: dictionary of coffea nanoevents array with major object selections and pair selections
    :type events_dict: dict{coffea.nanoevents.NanoEvents.array}
    :param cutflow_dict: dictionary of cutflows in different channels
    :type cutflow_dict: dict{keyname:{
            selection: int
            }
    :param object_dict: dictionary of zipped lepton objects
    :type object_dict: dict{object name: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray}
    :param cfg: configuration object
    :type cfg: DynaConf object
    """
    for keyname, events in events_dict.items():
        lepcfgname = f"signal.{keyname}"
        lepselname = cfg[lepcfgname+".selections"]
        comselname = cfg["signal.commonsel"]
        if comselname.ak4jet is not None:
            ak4s, ak8s = jet_properties(events)
            jetselect = comselname.ak4jet
            # Basic jet check
            ak4mask = (ak4s.pt > jetselect.ptLevel) & \
                (abs(ak4s.eta) < jetselect.absetaLevel)
            ak4s = ak4s[ak4mask]
            ak4mask = (ak.num(ak4s) >= jetselect.count)
            events = events[ak4mask]
            ak4s = ak4s[ak4mask]
            ak8s = ak8s[ak4mask]
            apply_mask_on_all(object_dict[keyname], ak4mask)
            # Overlap check
            if not lepselname.electron.veto and (lepselname.electron.veto is not None):
                electronLV = LV_from_zipped(object_dict[keyname]['Electron'])
                dRmask = (ak.sum(electronLV[:, 0].deltaR(LV_from_zipped(ak4s)) > jetselect.dRLevel, 
                             axis=1) > jetselect.count)
                events = events[dRmask]
                ak4s = ak4s[dRmask]
                ak8s = ak8s[dRmask]
                apply_mask_on_all(object_dict[keyname], dRmask)
            if not lepselname.muon.veto and (lepselname.muon.veto is not None):
                muonLV = LV_from_zipped(object_dict[keyname]['Muon'])
                dRmask = (ak.sum(muonLV[:, 0].deltaR(LV_from_zipped(ak4s)) > jetselect.dRLevel,
                                 axis=1) > jetselect.count)
                events = events[dRmask]
                ak4s = ak4s[dRmask]
                ak8s = ak8s[dRmask]
                apply_mask_on_all(object_dict[keyname], dRmask)
            if not lepselname.tau.veto and (lepselname.tau.veto is not None):
                for i in range(lepselname.tau.count):
                    tauLV = LV_from_zipped(object_dict[keyname]['Tau'])
                    dRmask = (ak.sum(tauLV[:, i].deltaR(LV_from_zipped(ak4s)) > jetselect.dRLevel, axis=1) > jetselect.count)
                    events = events[dRmask]
                    ak4s = ak4s[dRmask]
                    ak8s = ak8s[dRmask]
                    apply_mask_on_all(object_dict[keyname], dRmask)
        # TODO: Fill in fatjet selection
        events_dict[keyname] = events
        cutflow_dict[keyname]["Jet selections"] = len(events)
        object_dict[keyname].update({"Jet": ak4s, 
                                     "FatJet": ak8s})


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
            muons, electrons, taus = zip_lepproperties(cfg.signal.outputs, events)
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

def apply_mask_on_all(object_dict, mask):
    """Apply mask on all the objects in a dict

    :param object_dict: {object_name: zipped object}
    :type object_dict: dict
    :param mask: mask to apply selections
    :type mask: ak.array
    """
    for name, zipped in object_dict.items():
        object_dict[name] = zipped[mask]

def zip_object(cfg, events, extra=None):
    """ Return a zipped object with the provided configuration
    
    :param cfg: configuration dictionary of an object {datatype: {name: nanoaod name}}
    :type cfg: configuration
    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array 
    :param extra: extra configuration dictionary
    :type extra: dict/None
    :return: zipped properties properties of an object
    :rtype: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray
    """

    vars_dict = dict(ChainMap(*cfg.values()))
    zipped_dict = {}
    for name, nanoaodname in vars_dict.items():
        zipped_dict.update({name: events[nanoaodname]})
    zipped_object = ak.zip(zipped_dict, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)
    return zipped_object
    
# TODO: this is not the right way to zip properties. Require constant changes. Very inconvenient
# TODO: use dict.update method to do this from cfg
def zip_lepproperties(propcfg, events, extra=None):
    """ Return a collection of dictionaries containing the properties of leptons.
    
    :param propcfg: a dictionary containing the properties of leptons
    :type propcfg: dict
        {
            object:{
                {datatype: {
                    name: nanoaod name}}}
        }
    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array 
    :return: zipped properties properties of leptons
    :rtype: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray
    """
    muons = zip_object(propcfg.Muon, events)
    electrons = zip_object(propcfg.Electron, events)
    taus = zip_object(propcfg.Tau, events)
    # A collection of dictionaries, each dictionary describing a single muon candidate property
    # muons = ak.zip({
    #     "pt": events.Muon_pt,  # type events.Muon_pt: high-level awkward array
    #     "eta": events.Muon_eta,
    #     "phi": events.Muon_phi,
    #     "mass": events.Muon_mass,
    #     "charge": events.Muon_charge,
    #     "dxy": events.Muon_dxy,
    #     "dz": events.Muon_dz,
    #     "iso": events.Muon_pfRelIso04_all,
    #     "tightid": events.Muon_tightId
    # }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    # # A collection of dictionaries, each dictionary describing a single electron candidate property
    # electrons = ak.zip({
    #     "pt": events.Electron_pt,  # type events.Electron_pt: high-level awkward array
    #     "eta": events.Electron_eta,
    #     "phi": events.Electron_phi,
    #     "mass": events.Electron_mass,
    #     "charge": events.Electron_charge,
    #     "dxy": events.Electron_dxy,
    #     "dz": events.Electron_dz,
    #     "bdtid": events.Electron_mvaIso,
    # }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    # # A collection of dictionaries, each dictionary describing a single tau candidate property
    # taus = ak.zip({
    #     "pt": events.Tau_pt,  # type events.Tau_pt: high-level awkward array
    #     "eta": events.Tau_eta,
    #     "phi": events.Tau_phi,
    #     "mass": events.Tau_mass,
    #     "charge": events.Tau_charge,
    #     "dxy": events.Tau_dxy,
    #     "dz": events.Tau_dz,
    #     "idvsjet": events.Tau_idDeepTau2018v2p5VSjet,
    #     "idvsmu": events.Tau_idDeepTau2018v2p5VSmu,
    #     "idvse": events.Tau_idDeepTau2018v2p5VSe,
    # }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)
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
        "pt": events.Jet_pt,  # type events.Muon_pt: high-level awkward array
        "eta": events.Jet_eta,
        "phi": events.Jet_phi,
        "mass": events.Jet_mass,
        "deepJetbtagger": events.Jet_btagDeepFlavB,
        "deepCSVbtagger": events.Jet_btagDeepB,
        "jetID": events.Jet_jetId,
        "deepJetCvsB": events.Jet_btagDeepFlavCvB
    }, with_name="PtEtaPhiMCandidate", behavior=vector.behavior)

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
    }, with_name="PtEtaPhiMCandidate", behavior=vector.behavior)

    return ak4s, ak8s

# TODO: combine the two methods
def LV_from_zipped(zippedLep, sortbypt=True):
    object_ak = ak.zip({
        "pt": zippedLep.pt,
        "eta": zippedLep.eta,
        "phi": zippedLep.phi,
        "M": zippedLep.mass,
    })
    if sortbypt:
        object_ak = object_ak[ak.argsort(object_ak.pt, ascending=False)]
    object_LV = vec.Array(object_ak) 
    return object_LV
    
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
        object_ak = object_ak[ak.argsort(object_ak.pt, ascending=False)]
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
