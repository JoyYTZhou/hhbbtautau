#!/usr/bin/env python
import awkward as ak
import dask_awkward as dak
import dask
from utils.cutflowutil import weightedSelection
from utils.datautil import arr_handler
import vector as vec
import pandas as pd
import operator as opr
from config.selectionconfig import selectionsettings as selcfg

default_trigsel = selcfg.triggerselections
default_objsel = selcfg.objselections
default_mapcfg = selcfg.outputs

class BaseEventSelections:
    """Base class for event selections.
    
    Attributes
    - `mapcfg`: mapping configuration {key=abbreviation, value=nanoaodname}
    - `objsel`: PackedSelection object to keep track of cutflow
    - `cutflow`: cutflow object
    """
    def __init__(self, trigcfg=default_trigsel, objcfg=default_objsel, mapcfg=default_mapcfg) -> None:
        """Initialize the event selection object with the given selection configurations."""
        self._trigcfg = trigcfg
        self._objselcfg = objcfg
        self._mapcfg = mapcfg
        self.objsel = weightedSelection()
        self.cutflow = None
        self.cfobj = None
    
    def __call__(self, events, wgtname='Generator_weight', **kwargs):
        """Apply all the selections in line on the events"""
        return self.callevtsel(events, wgtname=wgtname, **kwargs)

    @property
    def trigcfg(self):
        return self._trigcfg

    @property
    def objselcfg(self):
        return self._objselcfg
    
    @property
    def mapcfg(self):
        return self._mapcfg
    
    def triggersel(self, events):
        pass

    def setevtsel(self, events):
        """Custom function to set the object selections on event levels based on config.
        Mask should be N*bool 1-D array.

        :param events: events loaded from a .root file
        """
        pass
                        
    def callevtsel(self, events, wgtname, compute_veto=False):
        """Apply all the selections in line on the events
        Parameters
        
        :return: passed events, vetoed events
        """
        self.triggersel(events)
        self.setevtsel(events)
        if self.objsel.names:
            self.cfobj = self.objsel.cutflow(events[wgtname], *self.objsel.names)
            self.cutflow = self.cfobj.result()
            passed = events[self.cutflow.maskscutflow[-1]]
            if compute_veto: 
                vetoed = events[~(self.objsel.all())]
                result = (passed, vetoed)
            else:
                result = passed
            return result
        else:
            raise UserWarning("Events selections not set, this is base selection!")
            return events

    def cf_to_df(self):
        """Return a dataframe for a single EventSelections.cutflow object.
        DASK GETS COMPUTED!
        :return: cutflow df
        :rtype: pandas.DataFrame
        """
        row_names = self.cutflow.labels
        dfdata = {}
        if self.cutflow.wgtevcutflow is not None:
            wgt_number = dask.compute(self.cutflow.wgtevcutflow)[0]
            dfdata['wgt'] = wgt_number
        number = dask.compute(self.cutflow.nevcutflow)[0]
        dfdata['raw'] = number
        df_cf = pd.DataFrame(dfdata, index=row_names)
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

    def __init__(self, events, name, **kwargs):
        """Construct an object from provided events with given selection configuration.
        
        Parameters
        - `name`: name of the object
        - `selcfg`: Selection configuration for the object
        """
        self._name = name
        self._selcfg = kwargs.get('selcfg', default_objsel[name])
        self._mapcfg = kwargs.get('mapcfg', default_mapcfg[name])
        self.events = events
        self.cutflow = kwargs.get('cutflow', weightedSelection())
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
        aodarr = self.events[aodname]
        if func is not None:
            return op(func(aodarr), selval)
        else:
            return op(aodarr, selval)

    def numselmask(self, mask, op):
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
    
    def evtosmask(self, selmask):
        """Create mask on events with OS objects.
        !!! Note that this mask is applied per event, not per object.
        1 for events with 2 OS objects that pass selmask"""
        aodname = self.mapcfg['charge']
        aodarr = self.events[aodname][selmask]
        sum_charge = abs(dak.sum(aodarr, axis=1))
        mask = (sum_charge < dak.num(aodarr, axis=1))
        return mask
    
    def getzipped(self):
        return Object.set_zipped(self.events, self.mapcfg)
    
    @staticmethod
    def jetid_ol_check(lep):
        jetidx = lep

    @staticmethod
    def sortmask(dfarr, **kwargs):
        """Wrapper around awkward argsort function.
        
        Parameters
        - `dfarr`: the data arr to be sorted
        """
        dfarr = arr_handler(dfarr)
        sortmask = ak.argsort(dfarr, 
                   axis=kwargs.get('axis', -1), 
                   ascending=kwargs.get('ascending', False),
                   highlevel=kwargs.get('highlevel', True)
                   )
        return sortmask

    @staticmethod
    def fourvector(events, field, sort=True, sortname='pt'):
        """Returns a fourvector from the events.
    
        Parameters
        - `events`: the events to extract the fourvector from. 
        - `field`: the name of the field in the events that contains the fourvector information.
        - `sort`: whether to sort the fourvector
        - `sortname`: the name of the field to sort the fourvector by.

        Return
        - a fourvector object.
        """
        object_ak = ak.zip({
            "pt": events[field+"_pt"],
            "eta": events[field+"_eta"],
            "phi": events[field+"_phi"],
            "M": events[field+"_mass"]
            })
        if sort:
            object_ak = object_ak[ak.argsort(object_ak[sortname], ascending=False)]
            object_LV = vec.Array(object_ak)
        return object_LV

    @staticmethod
    def set_zipped(events, namemap, delayed=False):
        """Given events, read only object-related observables and zip them into dict."""
        zipped_dict = {}
        for name, nanoaodname in namemap.items():
            zipped_dict.update({name: events[nanoaodname]})
        if delayed: zipped_object = dak.zip(zipped_dict)
        else: zipped_object = ak.zip(zipped_dict)
        return zipped_object

    @staticmethod
    def object_to_df(dakzipped, sortname, prefix='', ascending=False, index=0):
        """Take a dask zipped object, unzip it, compute it, flatten it into a dataframe"""
        computed, = dask.compute(dakzipped[dak.argsort(dakzipped[sortname], ascending=ascending)])
        dakarr_dict = {}
        for i, field in enumerate(dakzipped.fields):
            colname = prefix + "_" + field if prefix != '' else field
            dakarr_dict.update({colname: ak.to_list(computed[field][:, index])})
        objdf = pd.DataFrame(dakarr_dict)
        return objdf

    
        

    def dRoverlap(self, altobject):
        pass










