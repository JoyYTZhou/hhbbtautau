# adapted from https://github.com/bu-cms/projectcoffea/blob/master/projectcoffea/helpers/helpers.py

import numpy as np
import os
import awkward as ak
import vector as vec
import pandas as pd

pjoin = os.path.join

def checkevents(events):
    """Returns True if the events are in the right format, False otherwise."""
    if hasattr(events, 'keys') and callable(getattr(events, 'keys')):
        return True
    elif isinstance(events, pd.core.frame.DataFrame):
        return True
    else:
        raise TypeError("Invalid type for events. Must be an awkward object or a DataFrame")

def arr_handler(dfarr):
    """Handle different types of data arrays to convert them to awkward arrays."""
    if isinstance(dfarr, pd.core.series.Series):
        try: 
            ak_arr = dfarr.ak.array
            return ak_arr
        except AttributeError as e:
            return dfarr
    elif isinstance(dfarr, pd.core.frame.DataFrame):
        raise ValueError("specify a column. This is a dataframe.")
    elif isinstance(dfarr, ak.highlevel.Array):
        return dfarr
    else:
        raise TypeError(f"This is of type {type(dfarr)}")

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

def dphi(phi1, phi2):
    """Calculates delta phi between objects"""
    x = np.abs(phi1 - phi2)
    sign = x<=np.pi
    dphi = sign* x + ~sign * (2*np.pi - x)
    return dphi

def min_dphi_jet_met(jets, met_phi, njet=4, ptmin=30, etamax=2.4):
    """Calculate minimal delta phi between jets and met

    :param jets: Jet candidates to use, must be sorted by pT
    :type jets: JaggedCandidateArray
    :param met_phi: MET phi values, one per event
    :type met_phi: array
    :param njet: Number of leading jets to consider, defaults to 4
    :type njet: int, optional
    """

    assert(met_phi.shape!=())
    

    jets=jets[(jets.pt>ptmin)&(jets.abseta < etamax)]
    jets = jets[:,:njet]

    return dphi(jets.phi, met_phi).min()

def mt(pt1, phi1, pt2, phi2):
    """Calculates MT of two objects"""
    return np.sqrt(2 * pt1 * pt2 * (1-np.cos(phi1-phi2)))

def pt_phi_to_px_py(pt, phi):
    """Convert pt and phi to px and py."""
    x = pt * np.cos(phi)
    y = pt * np.sin(phi)

    return x, y

def recoil(met_pt, met_phi, eles, mus, photons):
    """Calculates hadronic recoil by removing leptons from MET

    :param met_pt: MET pt values
    :type met_pt: array
    :param met_phi: MET phi values
    :type met_phi: array
    :param eles: Electron candidates
    :type eles: JaggedCandidateArray
    :param mus: Muon candidates
    :type mus: JaggedCandidateArray
    :return: Pt and phi of the recoil
    :rtype: tuple of arrays (pt, phi)
    """
    met_x, met_y = pt_phi_to_px_py(met_pt, met_phi)
    ele_x, ele_y = pt_phi_to_px_py(eles.pt, eles.phi)
    gam_x, gam_y = pt_phi_to_px_py(photons.pt, photons.phi)
    mu_x, mu_y = pt_phi_to_px_py(mus.pt, mus.phi)

    recoil_x = met_x + ele_x.sum() + mu_x.sum() + gam_x.sum()
    recoil_y = met_y + ele_y.sum() + mu_y.sum() + gam_y.sum()

    recoil_pt = np.hypot(recoil_x, recoil_y)
    recoil_phi = np.arctan2(recoil_y, recoil_x)
    return recoil_pt, recoil_phi


def weight_shape(values, weight):
    """Broadcasts weight array to right shape for given values"""
    return (~np.isnan(values) * weight).flatten()

def object_overlap(toclean, cleanagainst, dr=0.4):
    """Generate a mask to use for overlap removal

    :param toclean: Candidates that should be cleaned (lower priority candidats)
    :type toclean: JaggedCandidateArray
    :param cleanagainst: Candidates that should be cleaned against (higher priority)
    :type cleanagainst: JaggedCandidateArray
    :param dr: Delta R parameter, defaults to 0.4
    :type dr: float, optional
    :return: Mask to select non-overlapping candidates in the collection to be cleaned
    :rtype: JaggedArray
    """
    comb_phi = toclean.phi.cross(cleanagainst.phi, nested=True)
    comb_eta = toclean.eta.cross(cleanagainst.eta, nested=True)
    delta_r = np.hypot( dphi(comb_phi.i0, comb_phi.i1), comb_eta.i0-comb_eta.i1)

    return delta_r.min() > dr


def mask_or(df, masks):
    """Returns the OR of the masks in the list

    :param df: Data frame
    :type df: LazyDataFrame
    :param masks: Mask names as saved in the df
    :type masks: List
    :return: OR of all masks for each event
    :rtype: array
    """
    # Start with array of False
    decision = np.ones(df.size)==0

    # Flip to true if any is passed
    for t in masks:
        try:
            decision = decision | df[t]
        except KeyError:
            continue
    return decision

def mask_and(df, masks):
    """Returns the AND of the masks in the list

    :param df: Data frame
    :type df: LazyDataFrame
    :param masks: Mask names as saved in the df
    :type masks: List
    :return: OR of all masks for each event
    :rtype: array
    """
    # Start with array of False
    decision = np.ones(df.size)==1

    # Flip to true if any is passed
    for t in masks:
        try:
            decision = decision & df[t]
        except KeyError:
            continue
    return decision


from coffea.lookup_tools import extractor




def sigmoid(x,a,b,c,d):
    """
    Sigmoid function for trigger turn-on fits.

    f(x) = c + (d-c) / (1 + np.exp(-a * (x-b)))
    """
    return c + (d-c) / (1 + np.exp(-a * (x-b)))

def sigmoid3(x,a,b,c):
    '''
    Sigmoid function with three parameters.
    '''
    return c / (1 + np.exp(-a * (x-b)))

def exponential(x, a, b, c):
    """Exponential function for scale factor fits."""
    return a * np.exp(-b * x) + c


def candidates_in_hem(candidates):
    """Returns a mask telling you which candidates are in the HEM region"""
    return (-3.0 < candidates.eta) & (candidates.eta < -1.3) & (-1.8 < candidates.phi) & (candidates.phi < -0.6)

def electrons_in_hem(electrons):
    """Returns a mask telling you which electrons (different region def compared to jets) are in the HEM region"""
    return (-3.0 < electrons.eta) & (electrons.eta < -1.39) & (-1.6 < electrons.phi) & (electrons.phi < -0.9)

def calculate_vecB(ak4, met_pt, met_phi):
    '''Calculate vecB (balance) quantity, based on jets and MET.'''
    mht_p4 = ak4[ak4.pt>30].p4.sum()
    mht_x = - mht_p4.pt * np.cos(mht_p4.phi) 
    mht_y = - mht_p4.pt * np.sin(mht_p4.phi) 

    met_x = met_pt * np.cos(met_phi)
    met_y = met_pt * np.sin(met_phi)

    vec_b = np.hypot(met_x-mht_x, met_y-mht_y) / np.hypot(met_x+mht_x, met_y+mht_y) 

    return vec_b

def calculate_vecDPhi(ak4, met_pt, met_phi, tk_met_phi):
    '''Calculate vecDPhi quantitity.'''
    vec_b = calculate_vecB(ak4, met_pt, met_phi)
    dphitkpf = dphi(met_phi, tk_met_phi)
    vec_dphi = np.hypot(3.33 * vec_b, dphitkpf)

    return vec_dphi
