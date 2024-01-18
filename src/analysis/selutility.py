#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
from analysis.dsmethods import *
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.schemas import BaseSchema
from coffea.nanoevents.methods import vector
import vector as vec
import uproot
import json as json
import operator as opr 
from collections import ChainMap
from config.selectionconfig import settings as sel_cfg

output_cfg = sel_cfg.signal.outputs
class Processor:
    def __init__(self):
        self._selseq = None
        self._data = None
        self._cutflow = 0
    
    @property
    def data(self):
        return self._data
    
    @data.setter
    def data(self, rt_cfg):
        with open(rt_cfg.INPUTFILE_PATH, 'r') as samplepath:
            fileset = json.load(samplepath)
        value = fileset['Background']
        value.update(fileset['Signal']) 
        self._data = value
        
    @property
    def selseq(self):
        return self._selseq
    
    @selseq.setter
    def selseq(self, value):
        self._selseq = value
        
    def evalute(self, events, leftover=True):
        if self.selseq is None:
            raise ValueError("Selection sequence is not set.")
        # for name, sel in self.selseq.items():
    
    def loadfile(self, filename, dsname, rt_cfg):
        events = NanoEventsFactory.from_root(
            file=f"{filename}:{rt_cfg.TREE_NAME}",
            steps_per_file= rt_cfg.CHUNK_NO,
            uproot_options={"timeout": 100},
            delayed=True,
            metadata={"dataset": dsname},
            schemaclass=BaseSchema,
        ).events()
        return events
        
    def testrun(self, rt_cfg):
        """Run test selections on a single file.

        :return: output
        """
        self.data = rt_cfg
        filename = f"{self.data['Background']['DYJets_1']}:Events"
        events = NanoEventsFactory.from_root(
            filename,
            entry_stop=None,
            metadata={"dataset": "DYJets"},
            schemaclass=BaseSchema,
        ).events()


class EventSelections:
    def __init__(self) -> None:
        self._channelname = None
        self._selcfg = None
        self._filtersel = None
        self._leptonsel = None
        self._ak4jetsel = None
        self._ak8jetsel = None
        self._cutflow = None
    
    @property
    def channelname(self):
        return self._channelname
    @channelname.setter
    def channelname(self, value):
        self._channelname = value

    @property
    def selcfg(self):
        return self._selcfg
    @selcfg.setter
    def selcfg(self, value):
        self._selcfg = value

    @property
    def filtersel(self):
        return self._filtersel
    @filtersel.setter
    def filtersel(self, value):
        self._filtersel = value

    @property
    def leptonsel(self):
        return self._leptonsel
    @leptonsel.setter
    def leptonsel(self, value):
        self._leptonsel = value

    @property
    def ak4jetsel(self):
        return self._ak4jetsel
    @ak4jetsel.setter
    def ak4jetsel(self, value):
        self._ak4jetsel = value
 
    def lepselsetter(self, events):
        """Custom function to set the lepton selections for a given channel.
        :param events: events loaded from a .root file
        :type events: dask_awkward.lib.core.Array
        :return: Packed Lepton Selections
        :rtype: PackedSelection
        """
        packedlepsel = PackedSelection()
        
        electron = Object(events, "Electron", output_cfg.Electron, self.selcfg.electron)
        muon = Object(events, "Muon", output_cfg.Muon, self.selcfg.muon)
        tau = Object(events, "Tau", output_cfg.Tau, self.selcfg.tau)
        
        electron_mask = (electron.ptmask(opr.ge) & \
                        electron.absetamask(opr.le) & \
                        electron.absbdtmask(opr.ge)) if not electron.veto else electron.vetomask()
        electron.filter_dakzipped(electron_mask)
        elec_nummask = electron.numselmask(opr.eq)
        
        muon_mask = (muon.ptmask(opr.ge) & \
                    muon.absetamask(opr.le) & \
                    muon.custommask('iso', opr.le)) if not muon.veto else muon.vetomask()
        muon.filter_dakzipped(muon_mask)
        muon_nummask = muon.numselmask(opr.eq)
        
        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le)) if not tau.veto else tau.vetomask()
        tau_nummask = tau.numselmask(opr.eq)
       
        packedlepsel.add_multiple({"ElectronSelection": elec_nummask,
                               "MuonSelection": muon_nummask,
                               "TauSelection": tau_nummask})
        return packedlepsel
        
    def lepcaller(self, events):
        """Call the lepton selection function for a given channel."""
        pass
        
    def ak4selsetter(self, events):
        
   
    @property
    def ak8jetsel(self):
        return self._ak8jetsel
    @ak8jetsel.setter
    def ak8jetsel(self, value):
        self._ak8jetsel = value
    

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
            is_M = "M" in pairname
            is_T = "T" in pairname
            is_E = "E" in pairname
            if is_M and is_T:
                dR = (filter_muons[:, 0].delta_r(filter_taus) >= pairselect.dRLevel)
                OS = (filter_muons[:, 0]["charge"] * filter_taus["charge"] < 0)
                SS = (filter_muons[:, 0]["charge"] * filter_taus["charge"] > 0)
            elif is_E and is_T:
                dR = (filter_electrons[:, 0].delta_r(filter_taus) >= pairselect.dRLevel)
                OS = (filter_electrons[:, 0]["charge"] * filter_taus["charge"] < 0)
                SS = (filter_electrons[:, 0]["charge"] * filter_taus["charge"] > 0)
            elif pairname.count("T") == 2:
                # TODO: place holder for now for this channel
                dR = ak.ones_like(filter_taus['charge'])
                OS = ak.ones_like(filter_taus['charge'])
                SS = ak.ones_like(filter_taus['charge'])
            pairmask = dR & (OS if pairselect.OS else SS)
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
            ak4s, ak8s = zip_jetproperties(cfg.signal.outputs, events)
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


def apply_mask_on_all(object_dict, mask):
    """Apply mask on all the objects in a dict

    :param object_dict: {object_name: zipped object}
    :type object_dict: dict
    :param mask: mask to apply selections
    :type mask: ak.array
    """
    for name, zipped in object_dict.items():
        object_dict[name] = zipped[mask]


class Object():
    def __init__(self, name, events, objcfg, selcfg):
        """Construct an object from provided events with given selection configuration.
        :param name: name of the object
        :type name: str
        :param events: events loaded from a .root file
        :type events: dask_awkward.lib.core.Array
        :param objcfg: object properties configuration
        :type objcfg: dynaconf
        :param selcfg: object selection configuration
        :type selcfg: dynaconf
        """
        self._name = name
        self._veto = objcfg.get('veto', None)
        self.objcfg = objcfg
        self.selcfg = selcfg
        self.set_dakzipped(events)
        self.selection = PackedSelection()
    
    @property
    def name(self):
        return self._name
    @name.setter
    def name(self, value):
        self._name = value
    
    @property
    def veto(self):
        return self._veto
    @veto.setter
    def veto(self, value):
        self._veto = value
    
    @property
    def dakzipped(self):
        return self._dakzipped
    @dakzipped.setter
    def dakzipped(self, value):
        self._dakzipped = value

    def set_dakzipped(self, events):
        vars_dict = dict(ChainMap(*(self.objcfg.values())))
        zipped_dict = {}
        for name, nanoaodname in vars_dict.items():
            zipped_dict.update({name: events[nanoaodname]})
        zipped_object = dak.zip(zipped_dict)
        self.dakzipped = zipped_object
        
    def filter_dakzipped(self, mask):
        """Filter the object based on a mask.
        :param mask: mask to filter the object
        :type mask: ak.array
        """
        self.dakzipped = self.dakzipped[mask]

    def vetomask(self):
        self.filter_dakzipped(self.ptmask(opr.ge))
        veto_mask = dak.num(self.dakzipped)==0
        return veto_mask
    
    def numselmask(self, op):
        return op(dak.num(self.dakzipped), self.selcfg.count)
    
    def custommask(self, name, op, func=None):
        """Create custom mask based on input"""
        if self.selcfg.get(name, None) is None:
            raise ValueError(f"threshold value {name} is not given for object {self.name}")
        if func is not None: 
            return op(func(self.dakzipped[name]), self.selcfg[name])
        else:
            return op(self.dakzipped[name], self.selcfg[name])
    
    def ptmask(self, op):
        return op(self.dakzipped.pt, self.selcfg.pt)
    
    def absetamask(self, op):
        return self.custommask('eta', op, abs)
    
    def absdxymask(self, op):
        return self.custommask('dxy', op, abs)
    
    def absdzmask(self, op):
        return self.custommask('dz', op, abs)
    
    def bdtidmask(self, op):
        return self.custommask("bdtid", op)
    
    def fourvector(self, events, sort=True, sortname='pt'):
        object_ak = ak.zip({
        "pt": events[self.name+"_pt"],
        "eta": events[self.name+"_eta"],
        "phi": events[self.name+"_phi"],
        "M": events[self.name+"_mass"]
        })
        if sort:
            object_ak = object_ak[ak.argsort(object_ak[sortname], ascending=False)]
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
