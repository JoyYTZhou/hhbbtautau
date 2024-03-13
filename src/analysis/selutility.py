#!/usr/bin/env python

import awkward as ak
import dask_awkward as dak
import dask
from coffea.analysis_tools import PackedSelection
import vector as vec
import pandas as pd
import operator as opr
from config.selectionconfig import selectionsettings as selcfg

default_lepsel = selcfg.lepselections
default_jetsel = selcfg.jetselections
default_mapcfg = selcfg.outputs

class BaseEventSelections:
    def __init__(self, lepcfg=default_lepsel, jetcfg=default_jetsel, mapcfg=default_mapcfg) -> None:
        self._lepselcfg = lepcfg
        self._jetselcfg = jetcfg
        self._mapcfg = mapcfg
        self.objsel = PackedSelection()
        self.cutflow = None
        self.cfobj = None

    @property
    def lepselcfg(self):
        return self._lepselcfg

    @property
    def jetselcfg(self):
        return self._jetselcfg
    
    @property
    def mapcfg(self):
        return self._mapcfg

    def selectlep(self, events):
        """Custom function to set the lepton selections based on config.
        :param events: events loaded from a .root file
        """
        pass
                        
    def selectjet(self, events):
        """Custom function to select jets based on config.
        :param events: events loaded from a .root file"""
        pass
        
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
        self.selectjet(events)
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
    def __init__(self, name, mapcfg, selcfg):
        """Construct an object from provided events with given selection configuration.
        
        Parameters
        - `name`: name of the object
        :type name: str
        :param mapcfg: mapping cfg from abbreviations to actual nanoaod names
        :type mapcfg: dynaconf dict
        :param selcfg: object selection configuration
        :type selcfg: dynaconf dict
        """
        self._name = name
        self._veto = selcfg.get('veto', None)
        self._dakzipped = None
        self.mapcfg = mapcfg
        self.selcfg = selcfg
        self.fields = list(self.mapcfg.keys())
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
        """Given events, read only object-related observables and zip them into dict."""
        zipped_dict = {}
        for name, nanoaodname in self.mapcfg.items():
            zipped_dict.update({name: events[nanoaodname]})
        zipped_object = dak.zip(zipped_dict)
        self.dakzipped = zipped_object

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

class Mask():
    def __init__(self) -> None:
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








