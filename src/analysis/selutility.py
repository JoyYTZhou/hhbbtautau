#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
import dask as dask
import dask.dataframe as dd
import dask.array as da
from analysis.dsmethods import *
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.schemas import BaseSchema
import vector as vec
import json as json
import operator as opr
import numpy as np
import pandas as pd
from collections import ChainMap
from config.selectionconfig import settings as sel_cfg

output_cfg = sel_cfg.signal.outputs
class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset.
    Attributes:
        channelseq (list): list of channel names in order of selection preference
        data (dict): dictionary of files
        channelnum (int): number of channels
        channelsel (list): list of selection configurations specific to channels
        commonsel (dict): dictionary of common selection configurations
    """
    def __init__(self, rt_cfg):
        self._channelseq = sel_cfg.signal.channelnames
        self._data = None
        self._rtcfg = rt_cfg
        self._dsname = self.rtcfg.PROCESS_NAME
        self._channelnum = self.rtcfg.CHANNEL_NO
        self._channelsel = [sel_cfg.signal[f'channel{i+1}'] for i in range(self.channelnum)]
        self._commonsel = sel_cfg.signal.commonsel
        self.outdir = self.rtcfg.OUTPUTDIR_PATH

    @property
    def data(self):
        return self._data

    @property
    def rtcfg(self):
        return self._rtcfg

    def setdata(self):
        with open(self.rtcfg.INPUTFILE_PATH, 'r') as samplepath:
            fileset = json.load(samplepath)
            self._data = fileset[self.rtcfg.PROCESS_NAME]

    @property
    def dsname(self):
        return self._dsname

    @property
    def channelnum(self):
        return self._channelnum

    @property
    def channelsel(self):
        return self._channelsel

    @property
    def channelseq(self):
        return self._channelseq

    @channelseq.setter
    def channelseq(self, value):
        self._channelseq = value

    @property
    def commonsel(self):
        return self._commonsel

    def loadfile(self, filename):
        events = NanoEventsFactory.from_root(
            file=filename,
            delayed=True,
            metadata={"dataset": self.dsname},
            schemaclass=BaseSchema,
        ).events()
        return events

    def singlerun(self, filename, suffix):
        """Run test selections on a single file.
        :return: output list, cutflow object list
        """
        cf_list = [None] * self.channelnum
        passed_list = [None] * self.channelnum
        events = self.loadfile(filename)
        for i in range(self.channelnum):
            print(f"Running selection for channel {self.channelseq[i]}")
            lepcfg = self.channelsel[i].selections
            jetcfg = self.commonsel
            cfgname = self.channelseq[i]
            evtsel = EventSelections(lepcfg, jetcfg, cfgname)
            evtsel.lepselsetter(events)
            passed, vetoed = evtsel.objselcaller(events)
            self.writeobj(passed, i, suffix)
            passed_list[i] = passed
            cf_list[i] = evtsel.cutflow
            events = vetoed
        return passed_list, cf_list

    def writeobj(self, passed, index, suffix):
        """This can be customized further"""
        chcfg = self.channelsel[index]
        electron = Object("Electron", passed, output_cfg.Electron, chcfg.selections.electron)
        muon = Object("Muon", passed, output_cfg.Muon, chcfg.selections.muon)
        tau = Object("Tau", passed, output_cfg.Tau, chcfg.selections.tau)

        edf = electron.to_daskdf()
        mdf = muon.to_daskdf()
        tdf = tau.to_daskdf()

        obj_df = pd.concat([edf, mdf, tdf], axis=1)
        obj_df.to_csv(pjoin(self.outdir,f"{chcfg.name}{suffix}.csv"), index=False)

    def res_to_df(self, cutflow_list):
        """Return a df (N cuts x K channels) with cutflow numbers"""
        df_list = [None] * len(cutflow_list)
        for i, cfres in enumerate(cutflow_list):
            header = self.channelseq[i]
            row_names = cfres.labels
            number = dask.compute(cfres.nevcutflow)[0]
            df = pd.DataFrame(data = number, columns = [header])
            df_list[i] = df
        df_concat = pd.concat(df_list, axis=1)
        df_concat.index = row_names
        return df_concat

    def res_to_np(self, cutflow_list):
        """Return a np (N cuts x K channels) with cutflow numbers"""
        row_names = None
        np_list = [None] * len(cutflow_list)
        for i, cfres in enumerate(cutflow_list):
            number = dask.compute(cfres.nevcutflow)[0]
            np_list[i] = number
            row_names = cfres.labels
        return np.array(np_list).transpose(), row_names

    def runall(self):
        """Run all files"""
        self.setdata()
        for i, (filename, partitions) in enumerate(self.data.items()):
            print(f"Running {filename} ===================")
            passed, cf = self.singlerun({filename: partitions}, suffix=i)
            cf_df = self.res_to_df(cf)
            cf_df.to_csv(pjoin(self.outdir, f"cutflow_{i}.csv"))

class EventSelections:
    def __init__(self, lepcfg, jetcfg, cfgname) -> None:
        self._channelname = cfgname
        self._lepselcfg = lepcfg
        self._jetselcfg = jetcfg
        self._filtersel = None
        self.objsel = PackedSelection()
        self.cutflow = None

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

    def lepselsetter(self, events):
        """Custom function to set the lepton selections for a given channel.
        :param events: events loaded from a .root file
        :type events: dask_awkward.lib.core.Array
        """
        electron = Object("Electron", events, output_cfg.Electron, self.lepselcfg.electron)
        muon = Object("Muon", events, output_cfg.Muon, self.lepselcfg.muon)
        tau = Object("Tau", events, output_cfg.Tau, self.lepselcfg.tau)

        if not electron.veto:
            electron_mask = (electron.ptmask(opr.ge) & \
                        electron.absetamask(opr.le) & \
                        electron.bdtidmask(opr.ge))
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

        self.objsel.add_multiple({"ElectronSelection": elec_nummask,
                               "MuonSelection": muon_nummask,
                               "TauSelection": tau_nummask})

        return None

    def jetselsetter(self, events):
        """Custom function to select jet selections for a given channel."""
        jet = Object("Jet", events, output_cfg.Jet, self.jetselcfg.Jet)
        jet_mask = (jet.ptmask(opr.ge) & \
                    jet.absetamask(opr.le))
        jet_nummask = jet.numselmask(opr.ge)

        fatjet = Object("FatJet", events, output_cfg.FatJet, self.jetselcfg.FatJet)
        fatjet_mask = (fatjet.custommask("mass", opr.ge))
        fatjet_nummask = fatjet.numselmask(opr.ge)

        self.objsel.add_multiple_events({"JetSelections": jet_nummask,
                                          "FatJetSelections": fatjet_mask})
        return None

    def objselcaller(self, events):
        """Call the lepton selection for a given channel.
        :return: passed events, vetoed events
        :rtype: dask_awkward.lib.core.Array
        """
        passed = events[self.objsel.all()]
        vetoed = events[~(self.objsel.all())]
        self.cutflow = self.objsel.cutflow(*self.objsel.names).result()
        return passed, vetoed

    def setall(self, events):
        self.lepselsetter(events)
        self.jetselsetter(passed)
        passed, vetoed = self.objselcaller(passed)
        return passed, vetoed

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
        self._dakzipped = None
        self.objcfg = objcfg
        self.selcfg = selcfg
        self.fields = None
        self.selection = PackedSelection()
        self.set_dakzipped(events)

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
        """Given events, read the object as a dask array for selection purpose."""
        vars_dict = dict(ChainMap(*(self.objcfg.values())))
        zipped_dict = {}
        for name, nanoaodname in vars_dict.items():
            zipped_dict.update({name: events[nanoaodname]})
        zipped_object = dak.zip(zipped_dict)
        self.dakzipped = zipped_object
        self.fields = list(vars_dict.keys())

    def to_daskdf(self, sortname='pt', ascending=False, index=0):
        """Take the object, unzip it, compute it, flatten it into a dataframe
        """
        if self.veto is True:
            print(f"Veto set for {self.name}.")
            return None
        computed, = dask.compute(self.dakzipped[dak.argsort(self.dakzipped[sortname], ascending=ascending)])
        dakarr_dict = {}
        for i, field in enumerate(self.fields):
            colname = self.name + "_" + field
            dakarr_dict.update({colname: ak.to_list(computed[field][:, index])})
        objdf = pd.DataFrame(dakarr_dict)
        return objdf

    def output_df(self, objdf, outfn):
        objdf.to_csv(outfn, index=False)

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

class OutputHist():
    """Output"""
    def __init__(self, obj_name, typeval):
        self._channelname = None
        self._type = typeval
        self._objname = obj_name
        self._hist = None

    @property
    def channelname(self):
        return self._channelname
    @channelname.setter
    def channelname(self, value):
        self._channelname = value
    @property
    def type(self):
        return self._type
    @type.setter
    def type(self, value):
        self._type = value
    @property
    def objname(self):
        return self._objname
    @objname.setter
    def objname(self, value):
        self._objname = value
    @property
    def hist(self):
        return self._hist
    @hist.setter
    def hist(self, value):
        self._hist = value

    def sethist(self, events):
        output_dict = output_cfg[self.objname][self.type]
        output_names = list(output_dict.keys())
        nanoaod_names = list(output_dict.values())








