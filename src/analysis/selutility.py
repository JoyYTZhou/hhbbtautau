#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
from analysis.dsmethods import *
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.schemas import BaseSchema
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
        self._lepselcfg = None
        self._jetselcfg = None
        self._filtersel = None
        self._leptonsel = None
        self._jetsel = None
        self._cutflow = None

    @property
    def channelname(self):
        return self._channelname
    @channelname.setter
    def channelname(self, value):
        self._channelname = value

    @property
    def lepselcfg(self):
        return self._lepselcfg
    @lepselcfg.setter
    def lepselcfg(self, value):
        self._lepselcfg = value

    @property
    def jetselcfg(self):
        return self._jetselcfg
    @jetselcfg.setter
    def jetselcfg(self, value):
        self._jetselcfg = value

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

    def jetsel(self):
        return self._jetsel
    @jetsel.setter
    def jetsel(self, value):
        self._jetsel = value

    def lepselsetter(self, events):
        """Custom function to set the lepton selections for a given channel.
        :param events: events loaded from a .root file
        :type events: dask_awkward.lib.core.Array
        """
        packedlepsel = PackedSelection()

        electron = Object("Electron", events, output_cfg.Electron, self.lepselcfg.electron)
        muon = Object("Muon", events, output_cfg.Muon, self.lepselcfg.muon)
        tau = Object("Tau", events, output_cfg.Tau, self.lepselcfg.tau)

        if not electron.veto:
            electron_mask = (electron.ptmask(opr.ge) & \
                        electron.absetamask(opr.le) & \
                        electron.absbdtmask(opr.ge))
            electron.filter_dakzipped(electron_mask)
            elec_nummask = electron.numselmask(opr.eq)
        else: elec_nummask = electron.vetomask()

        if not muon.veto:
            muon_mask = (muon.ptmask(opr.ge) & \
                        muon.absetamask(opr.le) & \
                        muon.custommask('iso', opr.le))
            muon.filter_dakzipped(muon_mask)
            muon_nummask = muon.numselmask(opr.eq)
        else: muon_nummask = muon.vetomask()

        if not tau.veto:
            tau_mask = (tau.ptmask(opr.ge) & \
                        tau.absetamask(opr.le))
            tau.filter_dakzipped(tau_mask)
            tau_nummask = tau.numselmask(opr.eq)
        else: tau_nummask = tau.vetomask()

        packedlepsel.add_multiple({"ElectronSelection": elec_nummask,
                               "MuonSelection": muon_nummask,
                               "TauSelection": tau_nummask})

        self.leptonsel = packedlepsel
        return None

    def lepcaller(self, events):
        """Call the lepton selection for a given channel."""
        pass

    def jetselsetter(self, events):
        """Custom function to select jet selections for a given channel."""
        packedjetsel = PackedSelection()

        jet = Object("Jet", events, output_cfg.Jet, self.jetselcfg.Jet)
        jet_mask = (jet.ptmask(opr.ge) & \
                    jet.absetamask(opr.le))
        jet_nummask = jet.numselmask(opr.ge)

        fatjet = Object("FatJet", events, output_cfg.FatJet, self.jetselcfg.FatJet)
        fatjet_mask = (fatjet.custommask("mass", opr.ge))
        fatjet_nummask = fatjet.numselmask(opr.ge)

        packedjetsel.add_multiple_events({"JetSelections": jet_nummask,
                                          "FatJetSelections": fatjet_mask})
        jetsel = packedjetsel
        return None

    def jetcaller(self, events):
        """Call the jet selection for a given channel."""
        pass

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
        self._veto = selcfg.get('veto', None)
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

    def overlap(self, altobject):
        pass


    def dRoverlap(self, altobject):
        pass
