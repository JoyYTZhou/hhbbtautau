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
    """Base class for event selections.
    
    Attributes
    - `lepselcfg`: lepton selection configuration. Default to selcfg.lepselections
    - `jetselcfg`: jet selection configuration. Default to selcfg.jetselections
    - `mapcfg`: mapping configuration {key=abbreviation, value=nanoaodname}
    - `objsel`: PackedSelection object to keep track of cutflow
    - `cutflow`: cutflow object
    """
    def __init__(self, lepcfg=default_lepsel, jetcfg=default_jetsel, mapcfg=default_mapcfg) -> None:
        """Initialize the event selection object with the given selection configurations."""
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
        df_cf = pd.DataFrame(data = number, index=row_names)
        return df_cf

class Object():
    """Object class for handling object selections.

    Attributes
    - `name`: name of the object
    - `selcfg`: selection configuration for the object
    - `mapcfg`: mapping configuration for the object
    - `dakzipped`: dask zipped object
    - `fields`: list of fields in the object
    - `selection`: PackedSelection object to keep track of more detailed cutflow
    """

    def __init__(self, events, name, selcfg, **kwargs):
        """Construct an object from provided events with given selection configuration.
        
        Parameters
        - `name`: name of the object
        - `selcfg`: Selection configuration for the object
        """
        self._name = name
        self._selcfg = selcfg
        self._mapcfg = kwargs.get('mapcfg', default_mapcfg[name])
        self.events = events
        self.cutflow = kwargs.get('cutflow', PackedSelection())
        self.dakzipped = None
        self.fields = list(self.mapcfg.keys())

    @property
    def name(self):
        return self._name

    @property
    def selcfg(self):
        return self._selcfg
    
    @property
    def mapcfg(self):
        return self._mapcfg

    def custommask(self, maskname, op, func=None):
        """Create custom mask based on input.
        
        Parameters
        - `maskname`: name of the mask
        - `op`: operator to use for the mask
        - `func`: function to apply to the data. Defaults to None.

        Returns
        - `mask`: mask based on input
        """
        if self.selcfg.get(maskname, None) is None:
            raise ValueError(f"threshold value {maskname} is not given for object {self.name}")
        if self.mapcfg.get(maskname, None) is None:
            raise ValueError(f"Nanoaodname is not given for {maskname} of object {self.name}")
        aodname = self.mapcfg[maskname]
        selval = self.selcfg[maskname]
        if func is not None:
            return op(func(self.events[aodname]), selval)
        else:
            return op(self.events[aodname], selval)

    def numselmask(self, op, mask):
        return op(dak.sum(mask, axis=1), self.selcfg.count)

    def ptmask(self, op):
        return self.custommask('pt', op)

    def absetamask(self, op):
        return self.custommask('eta', op, abs)

    def absdxymask(self, op):
        return self.custommask('dxy', op, abs)

    def absdzmask(self, op):
        return self.custommask('dz', op, abs)

    def bdtidmask(self, op):
        return self.custommask("bdtid", op)
    
    def osmask(self):
        """Entirely wrong of now."""
        return dak.prod(self.dakzipped['charge'], axis=1) < 0 

    def set_dakzipped(self):
        """Given self.events, read only object-related observables and zip them into dict."""
        zipped_dict = {}
        for name, nanoaodname in self.mapcfg.items():
            zipped_dict.update({name: self.events[nanoaodname]})
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

    def fourvector(self, sort=True, sortname='pt'):
        object_ak = ak.zip({
        "pt": self.events[self.name+"_pt"],
        "eta": self.events[self.name+"_eta"],
        "phi": self.events[self.name+"_phi"],
        "M": self.events[self.name+"_mass"]
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









