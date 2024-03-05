#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
import dask
from coffea.analysis_tools import PackedSelection
import vector as vec
import operator as opr
import pandas as pd
from collections import ChainMap
import uproot
from config.selectionconfig import settings as sel_cfg
from .helper import *

output_cfg = sel_cfg.signal.outputs

class Processor:
    """Process individual file or filesets given strings/dicts belonging to one dataset.
    Attributes:
        channelseq (list): list of channel names in order of selection preference
        data (dict): dictionary of files
        commonsel (dict): dictionary of common selection configurations
    """
    def __init__(self, rt_cfg, dataset):
        self._rtcfg = rt_cfg
        self.treename = self.rtcfg.TREE_NAME
        self.outdir = self.rtcfg.OUTPUTDIR_PATH
        self.dataset = dataset
        if self.rtcfg.COPY_LOCAL: checkpath(self.rtcfg.COPY_DIR)
        if self.rtcfg.TRANSFER: 
            checkcondorpath(self.rtcfg.TRANSFER_PATH)
        checkpath(self.rtcfg.OUTPUTDIR_PATH)
        self.defselections()

    @property
    def rtcfg(self):
        return self._rtcfg

    def defselections(self):
        self.lepcfg = sel_cfg.signal[f'channel{self.rtcfg.CHANNEL_INDX}'].selections
        self.jetcfg = sel_cfg.signal.commonsel
        self.channelname = sel_cfg.signal[f'channel{self.rtcfg.CHANNEL_INDX}'].name
        
    def loadfile(self, filename, suffix):
        """This is a wrapper function around uproot._dask. 
        I am writing this doc to humiliate myself in the future.
        
        :return: The loaded file dict (and error messages if encountered)
        :rtype: dask_awkward.lib.core.Array
        """
        msg = []
        user_step_size = uproot._util.unset if not self.rtcfg.STEP_SIZE else self.rtcfg.STEP_SIZE
        if self.rtcfg.COPY_LOCAL:
            msg.append("Copying file to local ... ")
            destpath = pjoin(self.rtcfg.COPY_DIR, f"{self.dataset}_{suffix}.root")
            msg.append(f"Destination path {destpath}")
            cproot(filename, destpath)
            try:
                events = uproot.dask(
                    files={destpath: self.treename},
                    step_size=user_step_size
                )
            except OSError as e:
                msg.append(f"Failed again to load file after copying: {e}")
                events = None
        else:
            events = uproot.dask(
                files={filename: self.treename},
                step_size=user_step_size
            ) 
        return events, msg
    
    def runfile(self, filename, suffix, write_method='dask', write_npz=False):
        """Run test selections on a single file dict.
        :param write_method: method to write the output
        :return: cutflow dataframe 
        """
        msg = []
        msg.append(f'start processing {filename}!')
        events, loadmsg = self.loadfile(filename, suffix)
        msg.extend(loadmsg)

        evtsel = EventSelections(self.lepcfg, self.jetcfg, self.channelname)
        passed = evtsel.select(events, return_veto=False)
        if write_npz:
            npzname = pjoin(self.outdir, f'cutflow_{suffix}_{self.channelname}.npz')
            evtsel.cfobj.to_npz(npzname)
        if write_method == 'dask':
            self.writedask(passed, self.channelname, suffix, delayed=False)
            msg.append(f'computing {filename} finished!')
        elif write_method == 'dataframe':
            self.writeobj(passed, self.channelname, suffix)
        elif write_method == 'pickle':
            pass
        else:
            raise ValueError("Write method not supported")

        cutflow_name = f'{self.dataset}_cutflow_{suffix}.csv'
        localpath = pjoin(self.outdir, cutflow_name)
        cutflow_df = evtsel.cf_to_df() 
        cutflow_df.to_csv(localpath)

        del evtsel, cutflow_df, events, passed

        if self.rtcfg.TRANSFER:
            condorpath = f'{self.rtcfg.TRANSFER_PATH}/{cutflow_name}'
            cpcondor(localpath, condorpath, is_file=True)
        msg.append(f"file {filename} processed successfully!")
        if self.rtcfg.COPY_LOCAL: delfiles(self.rtcfg.COPY_DIR)

        return '\n'.join(msg)

    def writedask(self, passed, prefix, suffix, delayed=True, fields=None):
        """Wrapper around uproot.dask_write(),
        transfer all root files generated to a destination location."""
        msg = []
        if fields is None:
            dir_name = pjoin(self.outdir, prefix)
            checkpath(dir_name)
            if delayed: uproot.dask_write(passed, destination=dir_name, compute=False, prefix=f'{self.dataset}_{prefix}_{suffix}')
            else: 
                uproot.dask_write(passed, destination=dir_name, compute=True, prefix=f'{self.dataset}_{prefix}_{suffix}')
        else:
            pass

        if self.rtcfg.TRANSFER:
            transferfiles(dir_name, self.rtcfg.TRANSFER_PATH)
            shutil.rmtree(dir_name)
        
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
            cpcondor(obj_path, dest_path, is_file=True)
        
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

    def callobjsel(self, events, compute_veto=False):
        """Apply all the selections in line on the events
        :return: passed events, vetoed events
        """
        passed = events[self.objsel.all()]
        self.cfobj = self.objsel.cutflow(*self.objsel.names)
        self.cutflow = self.cfobj.result()
        if compute_veto: 
            vetoed = events[~(self.objsel.all())]
            result = (passed, vetoed)
        else:
            result = passed
        return result

    def select(self, events, return_veto=False):
        """Apply all selections in selection config object on the events."""
        self.selectlep(events)
        # self.selectjet(events)
        result = self.callobjsel(events, return_veto)
        return result

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








