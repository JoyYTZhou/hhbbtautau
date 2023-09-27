#!/usr/bin/env python

import numpy as np
import awkward as ak
from awkward import JaggedArray, ArrayBuilder
from HHtobbtautau.src.analysis.dsmethods import *
from coffea.nanoevents.methods import candidate
import coffea.processor as processor
from coffea.nanoevents.methods import vector
import pandas as pd
import uproot

def setup_majorcandidates(events, cfg, filename):
    """ Preselect candidate events for a target process with major object selections.

    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array
    :param cfg: configuration object
    :type cfg: DynaConf object
    :param filename: filename of the root file to be written with the filtered events
    :type filename: string
    :return: events dataframe with major object selections
    :rtype: dataframe
    """

    muons, electrons, taus = lep_properties(events)

    # Set up selections for the major candidates
    if cfg.signal.channelno>=1:
        for i in range(cfg.signal.channelno):
            # Create lepselection object: coffea.process.PackedSelection
            lepselection = processor.PackedSelection()
            lepcfgname = "signal.channel"+str(i)
            channelname = cfg[lepcfgname+".name"]
            lepselname = cfg[lepcfgname+".selections"]
            # select electrons
            if lepselname.electron != None:
                eselect = lepselname.electron
                
                electronmask = (electrons.pt>eselect.ptLevel) & \
                               (abs(electrons.eta)<eselect.absetaLevel) & \
                               (electrons.bdtid>eselect.BDTLevel) & \
                               (abs(electrons.dxy)<eselect.absdxyLevel) & \
                               (abs(electrons.dz)<eselect.abdzLevel)
                               
                filter_electrons = electrons[electronmask]
                lepselection.add("ElectronSelection", (ak.num(filter_electrons)==eselect.count))
            
            # select muons
            if lepselname.muon != None:
                mselect = lepselname.muon
                ############    ALERT   ############
                # ! This wouldn't work if there are more than one muons in the dataset.
                # ! For this analysis it would work
                muonmask = (muons.pt>mselect.ptLevel) & \
                            (abs(muons.eta)<mselect.absetaLevel) & \
                            (muons.bdtid>mselect.BDTLevel) & \
                            (abs(muons.dxy)<mselect.absdxyLevel) & \
                            (abs(muons.dz)<mselect.absdzLevel) & \
                            (muons.iso<mselect.isoLevel) & \
                            (muons.tightid>mselect.IDLevel)
                            
                filter_muons = muons[muonmask]
                lepselection.add("MuonSelection", (ak.num(filter_muons)==mselect.count))
                
            # select taus
            if lepselname.tau != None:
                if lepselname.muon!="double":
                    tselect = lepselname.tau
                    
                    taumask = (taus.pt>tselect.ptLevel) & \
                              (abs(taus.eta)<tselect.absetaLevel) & \
                              (taus.idvsjet>tselect.IDvsjetLevel) & \
                              (taus.idvsmu>tselect.IDvsmuLevel) & \
                              (taus.idvse>tselect.IDvseLevel) & \
                              (abs(taus.dz)<tselect.absdzLevel)
                    
                    filter_taus = taus[taumask]
                    lepselection.add("TauSelection", (ak.num(filter_taus)>=tselect.count))
                else:
                    tselect1 = lepselname.tau1
            else:
                    lepselection.add("TauSelection", ak.num(taus)==0)
            
            # Evaluate the selection collections at this point to identify individual leptons selected
            filtered_events = events[lepselection.all(*(lepselection.names))]
            filter_muons = muons[lepselection.all(*(lepselection.names))]
            filter_taus = taus[lepselection.all(*(lepselection.names))]
            filter_electrons = electrons[lepselection.all(*(lepselection.names))]
            
            # select pair properties
            if lepselname.pair != None:
                pairselect = lepselname.pair
                if pairselect.find("M") != -1 and pairselect.find("T")!= -1:
                    dR = (filter_muons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                    if pairselect.OS == True: 
                        OS = (filter_muons[:,0]["charge"] * filter_taus["charge"] < 0)
                        pairmask = OS & dR
                    else: 
                        SS = (filter_muons[:,0]["charge"] * filter_taus["charge"] > 0)
                        pairmask = SS & dR
                elif pairselect.find("E") != -1 and pairselect.find("T")!= -1:
                    dR = (filter_electrons[:,0].delta_r(filter_taus) >= pairselect.dRLevel)
                    if pairselect.OS == True:
                        OS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] < 0)
                        pairmask = dR & OS
                    else:
                        SS = (filter_electrons[:,0]["charge"] * filter_taus["charge"] > 0)
                        pairmask = dR & SS
                filtered_events = filtered_events[ak.any(pairmask, axis=1)]
            
            
            # select jet properties
            if lepselname.jet != None:
                jetselect = lepselname.jet
                ak4s, ak8s = jet_properties(filtered_events, cfg)
                
                ak4mask = (ak4s.pt > jetselect.pt) & \
                          (abs(ak4s.eta) < jetselect.absetaLevel) 
                filter_ak4s = ak4s[ak4mask]
                ak4mask = (ak.num(filter_ak4s) >= jetselect.count)
                filtered_events = filtered_events[ak4mask]
                filter_muons = filter_muons[ak4mask]
                filter_electrons = filter_electrons[ak4mask]
                filter_taus = filter_taus[ak4mask]
                filter_ak4s = filter_ak4s[ak4mask]
            else:
                pass
                
                
                
                
                
                
            with uproot.update(filename) as rootfile:
                  rootfile[channelname].extend({
                      "Muon": muons,
                      "Electron": electrons,
                      "Tau": taus
                  })
            
            
            

def lep_properties(events, extra=None):
    """
    Return a collection of dictionaries containing the properties of leptons.
    
    :param events: events in a NANOAD dataset
    :type events: coffea.nanoevents.NanoEvents.array 
    :return: dictionaries of properties
    :rtype: coffea.nanoevents.methods.vector.PtEtaPhiMLorentzVectorArray

    """
    # A collection of dictionaries, each dictionary describing a single muon candidate property
    muons = ak.zip({
        "pt": events.Muon_pt, # type events.Muon_pt: high-level awkward array
        "eta": events.Muon_eta,
        "phi": events.Muon_phi,
        "mass": events.Muon_mass,
        "charge": events.Muon_charge,
        "dxy": events.Muon_dxy,
        "dz": events.Muon_dz,
        "iso": events.Muon_pfRelIso04_all,
        "tightid": events.Muon_tightId
    }, with_name="PtEtaPhiMLorentzVector", behavior=vector.behavior)

    # A collection of dictionaries, each dictionary describing a single electron candidate property 
    electrons = ak.zip({
        "pt": events.Electron_pt, # type events.Electron_pt: high-level awkward array
        "eta": events.Electron_eta,
        "phi": events.Electron_phi,
        "mass": events.Electron_mass,
        "charge": events.Electron_charge,
        "dxy": events.Electron_dxy,
        "dz": events.Electron_dz,
        "bdtid": events.Electron_mvaIso,
    }, with_name="PtEtaPhiECandidate", behavior=vector.behavior)

    # A collection of dictionaries, each dictionary describing a single tau candidate property
    taus = ak.zip({
        "pt": events.Tau_pt, # type events.Tau_pt: high-level awkward array
        "eta": events.Tau_eta,
        "phi": events.Tau_phi,
        "mass": events.Tau_mass,
        "charge": events.Tau_charge,
        "dxy": events.Tau_dxy,
        "dz": events.Tau_dz,
        "idvsjet": events.Tau_idDeepTau2018v2p5VSjet,
        "idvsmu": events.Tau_idDeepTau2018v2p5VSmu,
        "idvse": events.Tau_idDeepTau2018v2p5VSe,
    }, with_name="PtEtaPhiTCandidate", behavior=candidate.behavior)

    return muons, electrons, taus

def jet_properties(events, cfg, extra=None):
    """ Returns the selected jet properties
    
    :param events: events
    :type events: coffea.nanoevents.methods.base.NanoEventsArray
    :param cfg: configuration file with the next level directly point to jet properties
    :type cfg: DynaConf object
    :return: AK4 and AK8 jets
    :rtype: ak.array
    """
    
    # A collection of dictionaries, each dictionary describing a single muon candidate property
    ak4s = ak.zip({
        "pt": events.Jet_pt, # type events.Muon_pt: high-level awkward array
        "eta": events.Jet_eta,
        "phi": events.Jet_phi,
        "mass": events.Jet_mass,
        "deepJetbtagger": events.Jet_btagDeepFlavB,
        "deepCSVbtagger": events.Jet_btagDeepB,
        "jetID": events.Jet_jetId,
        "deepJetCvsB": events.Jet_btagDeepFlavCvB
    }, with_name = "PtEtaPhiMCandidate", behavior = vector.behavior)

    ak8s = ak.zip({
        "pt": events.FatJet_pt,
        "eta": events.FatJet_eta,
        "phi": events.FatJet_phi,
        "mass": events.FatJet_mass,
        "jetID": events.FatJet_jetId,
        "QCD": events.FatJet_particleNetMD_QCD,
        "Xbb": events.FatJet_particleNetMD_Xbb,
        "Xcc": events.FatJet_particleNetMD_Xcc,
        "Xqq": events.FatJet_particleNetMD_Xqq,
        "Hbb": events.FatJet_btagHbb
    }, with_name = "PtEtaPhiMCandidate", behavior = vector.behavior)
    
    return ak4s, ak8s

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


    
    

    



def find_gen_dilepton(gen, pdgsum=0):
    """
    Finds and builds dilepton candidate from gen particles.

    Dilepton candidates are constructed from all charged and
    neutral leptons. A dilepton candidate is considered valid if
    the absolute value of the sum of the PDG IDs of the constituents
    is equal to the pdgsum parameter, and both constituents can be
    traced back to the same parent. It is *not* checked what exactly
    the parents are, as there will be no record for off-shell bosons.

    Choose pdgsum=0 for Z candidates, pdgsum=1 for W candidates.

    :param gen: Gen candidates
    :type gen: JaggedArray
    :param pdgsum: Absolute sum of PDG IDs to form a valid candidate
    :type pdgsum: int
    :return: Dilepton candidates
    :rtype: JaggedArray
    """
    leps = gen[(((gen.status==1) & islep(gen.pdg))) | ((gen.status==2) & (np.abs(gen.pdg)==15))]
    dileps = leps.distincts()

    dilepton_flavour = np.abs(dileps.i0.pdg + dileps.i1.pdg) == pdgsum
    good_dileps = dileps[dilepton_flavour]
    return good_dileps


def stat1_dilepton(df, gen):
    """Build a dilepton candidate from status 1 leptons

    :param df: Data frame
    :type df: dataframe
    :param gen: Gen. candidates
    :type gen: JaggedArray
    :return: pt and phi of dilepton
    :rtype: tuple of two 1D arrays
    """
    if is_lo_z(df['dataset']) or is_lo_z_ewk(df['dataset']) or is_nlo_z(df['dataset']):
        pdgsum = 0
        target = 91
    elif is_lo_w(df['dataset']) or  is_lo_w_ewk(df['dataset']) or is_nlo_w(df['dataset']):
        pdgsum = 1
        target = 81
    gen_dilep = find_gen_dilepton(gen[(gen.flag&1)==1], pdgsum)
    gen_dilep = gen_dilep[(np.abs(gen_dilep.mass-target)).argmin()]
    return gen_dilep.pt.max(), gen_dilep.phi.max()


def merge_dileptons(dilepton1, dilepton2, target, dilepton3=None):
    """
    Choose highest mass dilepton from up to three option lists.

    :return: pt and phi of the chosen dilepton
    :rtype: tuple of two 1D arrays
    """

    mmax1 = dilepton1.mass.max()
    mmax2 = dilepton2.mass.max()
    if dilepton3 is not None:
        mmax3 = dilepton3.mass.max()
    else:
        mmax3 = -1e3 * np.ones(dilepton1.size)

    dist1 = np.abs(mmax1 - target)
    dist2 = np.abs(mmax2 - target)
    dist3 = np.abs(mmax3 - target)

    take2 = (dist2 < dist1) & (dist2 < dist3)
    take3 = (dist3 < dist1) & (dist3 < dist2)
    take1 = ~(take2 | take3)

    vpt1 = dilepton1.pt.max()
    vphi1 = dilepton1.phi.max()
    vpt1[~take1] = 0
    vphi1[~take1] = 0

    vpt2 = dilepton2.pt.max()
    vphi2 = dilepton2.phi.max()
    vpt2[~take2] = 0
    vphi2[~take2] = 0

    if dilepton3 is not None:
        vpt3 = dilepton3.pt.max()
        vphi3 = dilepton3.phi.max()
        vpt3[~take3] = 0
        vphi3[~take3] = 0
    else:
        vpt3 = np.zeros(dilepton1.size)
        vphi3 = np.zeros(dilepton1.size)
    vphi = vphi1 + vphi2 + vphi3
    vpt = vpt1 + vpt2 + vpt3

    return vpt, vphi


def genv(gen):
    genbosons = gen[(gen.status==62)&((gen.abspdg==23)|(gen.abspdg==24))]
    return genbosons

def fill_gen_v_info(df, gen, dressed):
    '''
    One-stop function to generate gen v pt info.

    For stat1, dressed and lhe V, the pt and phi
    information is written into the data frame.
    '''

    # Gen bosons derived with different methods
    genbosons = genv(gen)
    df['gen_v_pt_part'], df['gen_v_phi_part'] = genbosons.pt[genbosons.pt.argmax()].max(), genbosons.phi[genbosons.pt.argmax()].max()
    df['gen_v_pt_stat1'], df['gen_v_phi_stat1'] = stat1_dilepton(df, gen)
    df['gen_v_pt_dress'], df['gen_v_phi_dress'] = dressed_dilep(df, gen, dressed)


    # Combine in order of preference:
    # 1. Gen boson from generator history
    df['gen_v_pt_combined'], df['gen_v_phi_combined'] = df['gen_v_pt_part'], df['gen_v_phi_part']
    # 2. Dilepton made from dressed leptons
    filler_pt, filler_phi = df['gen_v_pt_dress'], df['gen_v_phi_dress']
    # 3. Dilepton made from naked leptons
    invalid_filler = filler_pt <= 0
    filler_pt[invalid_filler] = df['gen_v_pt_stat1'][invalid_filler]
    filler_phi[invalid_filler] = df['gen_v_phi_stat1'][invalid_filler]

    invalid = genbosons.counts == 0
    df['gen_v_pt_combined'][invalid] = filler_pt[invalid]
    df['gen_v_phi_combined'][invalid] = filler_phi[invalid]

    # LHE is just pass through
    df['gen_v_pt_lhe'] = df['LHE_Vpt']
    df['gen_v_phi_lhe'] = np.zeros(df.size)

def setup_gen_candidates(df):
    '''
    Set up gen candidates.

    :param df: Data frame
    :type df: dataframe
    :return: candidate array including all gen 
    :rtype: tuple of two 1D arrays

    '''

    gen = JaggedCandidateAray.candidatesfromcounts(
        df['nGenPart'],
        pt=df['GenPart_pt'],
        eta=df['GenPart_eta'],
        phi=df['GenPart_phi'],
        mass=df['GenPart_mass'],
        charge=df['GenPart_pdgId'],
        pdg=df['GenPart_pdgId'],
        abspdg=np.abs(df['GenPart_pdgId']),
        status=df['GenPart_status'],
        flag = df['GenPart_statusFlags'])
    return gen

def setup_gen_jets(df):
    genjets = JaggedCandidateArray.candidatesfromcounts(
        df['nGenJet'],
        pt=df['GenJet_pt'],
        eta=df['GenJet_eta'],
        phi=df['GenJet_phi'],
        mass=0*df['GenJet_pt']
        )
    return genjets

def setup_gen_jets_ak8(df):
    genjets = JaggedCandidateArray.candidatesfromcounts(
        df['nGenJetAK8'],
        pt=df['GenJetAK8_pt'],
        eta=df['GenJetAK8_eta'],
        phi=df['GenJetAK8_phi'],
        mass=0*df['GenJetAK8_pt']
        )
    return genjets

def setup_gen_jets_ak8(df):
    genjets = JaggedCandidateArray.candidatesfromcounts(
        df['nGenJetAK8'],
        pt=df['GenJetAK8_pt'],
        eta=df['GenJetAK8_eta'],
        phi=df['GenJetAK8_phi'],
        mass=df['GenJetAK8_mass']
        )
    return genjets

def setup_dressed_gen_candidates(df):
    dressed = JaggedCandidateArray.candidatesfromcounts(
        df['nGenDressedLepton'],
        pt=df['GenDressedLepton_pt'],
        eta=df['GenDressedLepton_eta'],
        phi=df['GenDressedLepton_phi'],
        mass=0*df['GenDressedLepton_pt'],
        status=np.ones(df['GenDressedLepton_pt'].size),
        pdg=df['GenDressedLepton_pdgId'])
    return dressed

def islep(pdg):
    """Returns True if the PDG ID represents a lepton."""
    abspdg = np.abs(pdg)
    return (11<=abspdg) & (abspdg<=16)

def setup_lhe_candidates(df):
    lhe = JaggedCandidateArray.candidatesfromcounts(
            df['nLHEPart'],
            pt=df['LHEPart_pt'],
            eta=df['LHEPart_eta'],
            phi=df['LHEPart_phi'],
            mass=df['LHEPart_mass'],
            pdg=df['LHEPart_pdgId'],
            status=df['LHEPart_status'],
        )
    return lhe

def setup_lhe_cleaned_genjets(df):
    genjets = JaggedCandidateArray.candidatesfromcounts(
            df['nGenJet'],
            pt=df['GenJet_pt'],
            eta=df['GenJet_eta'],
            abseta=np.abs(df['GenJet_eta']),
            phi=df['GenJet_phi'],
            mass=df['GenJet_mass']
        )
    lhe = JaggedCandidateArray.candidatesfromcounts(
                df['nLHEPart'],
                pt=df['LHEPart_pt'],
                eta=df['LHEPart_eta'],
                phi=df['LHEPart_phi'],
                mass=df['LHEPart_mass'],
                pdg=df['LHEPart_pdgId'],
            )

    lhe_leps_gams = lhe[(islep(lhe.pdg) & ~isnu(lhe.pdg)) | (lhe.pdg==22)]

    return genjets[(~genjets.match(lhe_leps_gams,deltaRCut=0.4))]


def isnu(pdg):
    """Returns True if the PDG ID represents a neutrino."""
    abspdg = np.abs(pdg)
    return (12==abspdg) | (14==abspdg) | (16==abspdg)

def get_gen_photon_pt(gen):
    """Pt of gen photon for theory corrections."""
    all_gen_photons = gen[(gen.pdg==22)]
    prompt_mask = (all_gen_photons.status==1)&(all_gen_photons.flag&1==1)
    stat1_mask = (all_gen_photons.status==1)
    gen_photons = all_gen_photons[prompt_mask | (~prompt_mask.any()) & stat1_mask ]
    return gen_photons.pt.max()