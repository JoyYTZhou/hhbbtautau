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
        self.colobj = None
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
        """Custom function to set the object selections on event levels based on config.
        Mask should be N*bool 1-D array.
        """
        pass

    def setevtsel(self, events):
        """Custom function to set the object selections based on config.
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
        else:
            raise ValueError("Events selections not set, this is base selection!")
        if self.colobj is None:
            passed = events[self.cutflow.maskscutflow[-1]]
            if compute_veto: 
                vetoed = events[~(self.objsel.all())]
                result = (passed, vetoed)
            else:
                result = passed
            return result
        else:
            return self.colobj

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
    
class Object:
    """Object class for handling object selections.

    Attributes
    - `name`: name of the object
    - `selcfg`: selection configuration for the object
    - `mapcfg`: mapping configuration for the object
    - `dakzipped`: dask zipped object
    - `fields`: list of fields in the object
    - `selection`: PackedSelection object to keep track of more detailed cutflow
    """

    def __init__(self, name, **kwargs):
        """Construct an object from provided events with given selection configuration.
        
        Parameters
        - `name`: name of the object

        kwargs: 
        - `selcfg`: selection configuration for the object
        - `mapcfg`: mapping configuration for the object
        """
        self._name = name
        self._selcfg = kwargs.get('selcfg', default_objsel[name])
        self._mapcfg = kwargs.get('mapcfg', default_mapcfg[name])
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

    def get_zipped(self, events):
        return set_zipped(events, namemap=self._mapcfg)

    def custommask(self, events, maskname, op, func=None):
        """Create custom mask based on input.
        
        Parameters
        - `events`: events to apply the mask on
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
        aodarr = events[aodname]
        if func is not None:
            return op(func(aodarr), selval)
        else:
            return op(aodarr, selval)

    def numselmask(self, mask, op):
        return Object.maskredmask(mask, op, self.selcfg.count)

    def ptmask(self, events, op):
        return self.custommask(events, 'pt', op)

    def absetamask(self, events, op):
        return self.custommask(events, 'eta', op, abs)

    def absdxymask(self, events, op):
        return self.custommask(events, 'dxy', op, abs)

    def absdzmask(self, events, op):
        return self.custommask(events, 'dz', op, abs)
    
    def evtosmask(self, events, selmask):
        """Create mask on events with OS objects.
        !!! Note that this mask is applied per event, not per object.
        1 for events with 2 OS objects that pass selmask"""
        aodname = self.mapcfg['charge']
        aodarr = events[aodname][selmask]
        sum_charge = abs(ak.sum(aodarr, axis=1))
        mask = (sum_charge < ak.num(aodarr, axis=1))
        return mask

    def dRmask(self, events, threshold=0.4, **kwargs) -> ak.Array:
        vecs = Object.fourvector(events, self.name, **kwargs)
        return dRoverlap(vecs[0], vecs, threshold)
    
    def jetovcheck(self):
        """Urgent!"""
        aodname = self.mapcfg['jetidx']

    def getfourvec(self, events, **kwargs):
        return Object.fourvector(events, self.name, **kwargs)
    
    def getzipped(self, events, sort=True, sort_by='pt', **kwargs):
        """Get zipped object."""
        zipped = set_zipped(events, self.mapcfg)
        if sort:
            zipped = zipped[Object.sortmask(zipped[sort_by], **kwargs)]
        return zipped 

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
    def fourvector(events, fieldname, sort=True, sortname='pt', ascending=False):
        """Returns a fourvector from the events.
    
        Parameters
        - `events`: the events to extract the fourvector from. 
        - `fieldname`: the name of the field in the events that contains the fourvector information.
        - `sort`: whether to sort the fourvector
        - `sortname`: the name of the field to sort the fourvector by.
        - `ascending`: whether to sort the fourvector in ascending order.

        Return
        - a fourvector object.
        """
        vec_type = ['pt', 'eta', 'phi', 'mass']
        to_be_zipped = {cop: events[fieldname+"_"+cop] for cop in vec_type}
        object_ak = ak.zip(to_be_zipped)
        if sort:
            object_ak = object_ak[ak.argsort(object_ak[sortname], ascending=ascending)]
            object_LV = vec.Array(object_ak)
        return object_LV

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

    @staticmethod
    def maskredmask(mask, op, count) -> ak.Array:
        """Reduces the mask to event level selections. 
        
        Parameters
        - `mask`: the mask to be reduced
        - `op`: the operator to be used for the reduction
        - `count`: the count to be used for the reduction
        """
        return op(dak.sum(mask, axis=1), count)

def set_zipped(events, namemap, sort=True, sort_name='pt') -> ak.highlevel.Array:
    """Given events, read only object-related observables and zip them into ak. 
    Then zip the dict into an object.
    
    Parameters
    - `events`: events to extract the object from
    - `namemap`: mapping configuration for the object
    - `sort`: whether to sort the object
    - `sort_name`: the name of the field to sort the object by
    """
    zipped_dict = {}
    for name, nanoaodname in namemap.items():
        zipped_dict.update({name: events[nanoaodname]})
    if isinstance(events, dak.lib.core.Array):
        zipped_object = dak.zip(zipped_dict)
    else:
        zipped_object = ak.zip(zipped_dict)
    return zipped_object

def dRoverlap(vec, veclist, threshold=0.4, op=opr.ge) -> ak.Array:
    """Return deltaR mask. Default comparison threshold is 0.4. Default comparison is >=. 
    
    Parameters
    - `vec`: the vector to compare with
    - `veclist`: the list of vectors to compare against vec
    - `threshold`: the threshold for the comparison
    
    Return
    - a mask of the veclist that satisfies the comparison condition."""
    return op(vec.deltaR(veclist), threshold)











