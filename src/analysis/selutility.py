#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
import dask
from dask.distributed import Client, as_completed
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.schemas import BaseSchema
import vector as vec
import json as json
import operator as opr
import logging
import itertools
import pandas as pd
import gc
from collections import ChainMap
import uproot
from config.selectionconfig import settings as sel_cfg
from analysis.helper import *

output_cfg = sel_cfg.signal.outputs

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset.
    Attributes:
        channelseq (list): list of channel names in order of selection preference
        data (dict): dictionary of files
        commonsel (dict): dictionary of common selection configurations
    """
    def __init__(self, rt_cfg):
        self._rtcfg = rt_cfg
        self._metadata = None
        self._channelsel = sel_cfg.signal[f'channel{rt_cfg.CHANNEL_INDX}']
        self._commonsel = sel_cfg.signal.commonsel
        self.treename = "Events"
        self.dsname = None
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        checkpath(self.outdir)
        if self.rtcfg.TRANSFER: logging.info("File transfer in real time!")
        
    @property
    def metadata(self):
        return self._metadata

    @property
    def rtcfg(self):
        return self._rtcfg

    @property
    def channelsel(self):
        return self._channelsel

    @property
    def commonsel(self):
        return self._commonsel

    def rundata(self, client):
        self.setdata()
        for dataset, info in self.metadata.items():
            logging.info(f"Processing {dataset}...")
            self.dsname = dataset
            if client==None:
                logging.debug("No client spawned! In test mode.")
                self.runfile(info['filelist'][0], 0)
            else:
                self.dasklineup(info['filelist'], client)
        logging.info("Execution finished!")
        
    def setdata(self):
        with open(self.rtcfg.INPUTFILE_PATH, 'r') as samplepath:
            self._metadata = json.load(samplepath)

    def loadfile(self, filename, **kwargs):
        """This is a wrapper function around a coffea load file from root function,
        which is in itself yet another wrapper function of uproot._dask. 
        I am writing this doc to humiliate myself in the future.
        
        :return: The loaded file dict
        :rtype: dask_awkward.lib.core.Array
        """
        events = NanoEventsFactory.from_root(
            file=filename,
            delayed=True,
            metadata={"dataset": self.dsname},
            schemaclass=BaseSchema,
            **kwargs
        ).events()
        return events
    
    def runfile(self, filename, suffix, write_method='dask', write_npz=False):
        """Run test selections on a single file dict.
        :param write_method: method to write the output
        :return: cutflow dataframe 
        """
        logging.debug(f"Starting task for file: {filename}")
        events = self.loadfile({filename: self.treename})
        output = []
        lepcfg = self.channelsel.selections
        jetcfg = self.commonsel
        channelname = self.channelsel.name
        evtsel = EventSelections(lepcfg, jetcfg, channelname)
        passed, vetoed = evtsel.select(events)
        if write_npz:
            npzname = pjoin(self.outdir, f'cutflow_{suffix}_{channelname}.npz')
            evtsel.cfobj.to_npz(npzname)
        row_names = evtsel.cutflow.labels
        if write_method == 'dask':
            logging.debug("Writing to dask")
            self.writedask(passed, channelname, suffix)
        elif write_method == 'dataframe':
            self.writeobj(passed, channelname, suffix)
        else:
            raise ValueError("Write method not supported")

        localpath = pjoin(self.outdir, f'cutflow_{suffix}.csv')
        logging.debug(f"Writing to localpath: {localpath}")

        cutflow_df = evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)

        logging.debug(f"transfer cutflows: {self.rtcfg.TRANSFER}")

        if self.rtcfg.TRANSFER:
            logging.debug(f"Transfer path is {self.rtcfg.TRANSFER_PATH}")
            condorpath = f'{self.rtcfg.TRANSFER_PATH}/cutflow_{suffix}.csv'
            result = cpcondor(localpath, condorpath, is_file=True)
            return result

    def writedask(self, passed, prefix, suffix, fields=None):
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        logging.debug("Writing results.....")
        if fields is None:
            dir_name = pjoin(self.outdir, prefix)
            checkpath(dir_name)
            uproot.dask_write(passed, destination=dir_name, compute=True, prefix=f'{self.dsname}_{prefix}_{suffix}')
        else:
            pass
        
        if self.rtcfg.TRANSFER:
            logging.debug(f"Transfer results to {self.rtcfg.TRANSFER_PATH}")
            transferfiles(dir_name, self.rtcfg.TRANSFER_PATH)
        
    def writeobj(self, passed, index, suffix):
        """This can be further simplified.I do not like this function...
        Write computed awk array selected in sel_cfg to csv files."""
        chcfg = self.channelsel[index]
        electron = Object("Electron", passed, output_cfg.Electron, chcfg.selections.electron)
        muon = Object("Muon", passed, output_cfg.Muon, chcfg.selections.muon)
        tau = Object("Tau", passed, output_cfg.Tau, chcfg.selections.tau)

        edf = electron.to_daskdf()
        mdf = muon.to_daskdf()
        tdf = tau.to_daskdf()

        obj_df = pd.concat([edf, mdf, tdf], axis=1)
        del edf, mdf, tdf

        obj_path = pjoin(self.outdir, f"{chcfg.name}{suffix}.csv")
        obj_df.to_csv(obj_path, index=False)

        if self.rtcfg.TRANSFER:
            dest_path = pjoin(self.rtcfg.TRANSFER_PATH, f"{chcfg.name}{suffix}.csv")
            result = cpcondor(obj_path, dest_path, is_file=True)
   
    def pickupfailed(self, indexi, indexf):
        success = 0 
        failed_files = {}
        last_file = 0
        for i, (filename, partitions) in enumerate(self.data):
            logging.info(f"Running {filename} ===================")
            try:
                cf_df = self.singlerun({filename: partitions}, suffix=i)
            except OSError as e:
                failed_files.update({filename: e.strerror})
                logging.info(f"Caught an OSError while processing file {i}")
                logging.info("==========================")
                print(filename)
                print("==========================")
                print(e.strerror)
                continue
        pass
 
    def runonebyone(self, filelist):
        for i, file in enumerate(filelist):
            self.runfile(file)
            
    def dasklineup(self, filelist, client):
        """Run all files for one dataset through creating task submissions, with errors handled and collected.
        logging statements from runfile() are centrally collected here into one file.""" 
        futures = [client.submit(self.runfile, fn, i) for i, fn in enumerate(filelist)]

        for future, result in as_completed(futures, with_results=True, raise_errors=False):
            if isinstance(result, Exception):
                logging.error(f"Task failed with exception: {result}", exc_info=True)
            else:
                logging.info(f"Task succeeded with result: {result}")
        
    def transferfile(self, fn):
        """Transfer output by a processor to a final destination."""
        dest_path = pjoin(self.rtcfg.TRANSFER_PATH, fn)
        init_path = pjoin(self.outdir, fn)
        com = f"xrdcp -f {init_path} {dest_path}" 
        result = runcom(com, shell=True, capture_output=True, text=True)
        
        return result
        
class EventSelections:
    def __init__(self, lepcfg, jetcfg, cfgname) -> None:
        self._channelname = cfgname
        self._lepselcfg = lepcfg
        self._jetselcfg = jetcfg
        self.objsel = PackedSelection()
        self.cutflow = None
        self.cfobj = None

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


    def selectlep(self, events):
        """Custom function to set the lepton selections based on config.
        :param events: events loaded from a .root file
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

    def selectjet(self, events):
        """Jet selections based on configuration object. 
        Create selections on jet objects alone."""
        jet = Object("Jet", events, output_cfg.Jet, self.jetselcfg.Jet)
        jet_mask = (jet.ptmask(opr.ge) & \
                    jet.absetamask(opr.le))
        jet_nummask = jet.numselmask(opr.ge)

        fatjet = Object("FatJet", events, output_cfg.FatJet, self.jetselcfg.FatJet)
        fatjet_mask = (fatjet.custommask("mass", opr.ge))
        fatjet_nummask = fatjet.numselmask(opr.ge)

        self.objsel.add_multiple({"JetSelections": jet_nummask,
                                          "FatJetSelections": fatjet_mask})
        return None

    def callobjsel(self, events):
        """Apply all the selections in line on the events
        :return: passed events, vetoed events
        """
        passed = events[self.objsel.all()]
        vetoed = events[~(self.objsel.all())]
        self.cfobj = self.objsel.cutflow(*self.objsel.names)
        self.cutflow = self.cfobj.result()
        return passed, vetoed

    def select(self, events):
        """Apply all selections in selection config object on the events."""
        self.selectlep(events)
        # self.selectjet(events)
        passed, vetoed = self.callobjsel(events)
        return passed, vetoed

    def cf_to_df(self):
        """Return a dataframe for a single EventSelections.cutflow object.
        DASK GETS COMPUTED!
        :return: cutflow df
        :rtype: pandas.DataFrame
        """
        row_names = self.cutflow.labels
        number = dask.compute(self.cutflow.nevcutflow)[0]
        df_cf = pd.DataFrame(data = number, columns = [self.channelname], index=row_names)
        return df_cf

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
        """Take a dask zipped object, unzip it, compute it, flatten it into a dataframe
        """
        if self.veto is True:
            logging.info(f"Veto set for {self.name}.")
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
    
    def osmask(self):
        return dak.prod(self.dakzipped['charge'], axis=1) < 0 

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








