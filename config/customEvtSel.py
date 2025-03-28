# This file contains custom event selection classes for the src.analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from src.analysis.evtselutil import SkimSelections, BaseEventSelections, PreselSelections
from src.analysis.objutil import ObjectMasker, ObjectProcessor

from config.projectconfg import mc_nm, data_nm, selection_sync, selection_loose, selection_vbf
import operator as opr
import awkward as ak
import dask_awkward as dak

def switch_selections(sel_name):
    selections = {
        'tightskim': tightSkim,
        'vbfskim': VBFSkim,
        'onelooseb': OneLooseB,
        'twolooseb': TwoLooseB,
        'zerolooseb': ZeroLooseB
    }
    return selections.get(sel_name, BaseEventSelections)

ditau_trigsel = selection_sync.triggerselections
sync_objsel = selection_sync.objselections
vbf_trigsel = selection_vbf.triggerselections
loose_objsel = selection_loose.objselections

class vetoSkim(SkimSelections):
    def _setevtsel(self, events):
        electron = self.getObjMasker(events, "Electron")
        muon = self.getObjMasker(events, "Muon")

        e_base_conditions = {
            'pt': (opr.ge,),
            'dxy': (opr.le, abs),
            'eta': (opr.le, abs),
            'dz': (opr.le, abs),
            'mvaIso_WP90': (opr.eq,)
        }
        e_mask = electron.create_combined_mask(e_base_conditions)
        elec_nummask = electron.vetomask(e_mask)

        m_base_conditions = {
            'pt': (opr.ge,),
            'dxy': (opr.le, abs),
            'eta': (opr.le, abs),
            'dz': (opr.le, abs),
            'mediumid': (opr.eq,),
            'tightid': (opr.eq,),
            'isoid04': (opr.le,)
        }
        m_mask = muon.create_combined_mask(m_base_conditions)
        muon_nummask = muon.vetomask(m_mask)

        self.objsel.add_multiple({"Electron Veto": elec_nummask,
                                "Muon Veto": muon_nummask})

class tightSkim(vetoSkim):
    """Di-Tau Trigger + Sync Vetos"""
    def __init__(self, is_mc) -> None:
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=ditau_trigsel, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=False, is_mc=is_mc)

class VBFSkim(vetoSkim):
    """VBF Trigger + Sync Vetos"""
    def __init__(self, is_mc):
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=vbf_trigsel, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=False, is_mc=is_mc)
    
class LoosetwoTau(PreselSelections):
    """Implement Loose Tau Selections + b jet selections."""
    def __init__(self, is_mc) -> None:
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=ditau_trigsel, objselcfg=loose_objsel, mapcfg=mapcfg, sequential=True, is_mc=is_mc)

    def seltwotaus(self, events) -> ak.Array:
        tau_masker = self.getObjMasker(events, "Tau")

        base_conditions = {
            'pt': (opr.ge,),
            'eta': (opr.le, abs),
            'dz': (opr.lt, abs),
            'idvsjet': (opr.ge,),
            'idvsmu': (opr.ge,),
            'idvse': (opr.ge,)
            }
        tau_mask = tau_masker.create_combined_mask(base_conditions)
        tau_nummask = tau_masker.numselmask(tau_mask, opr.ge)
        tau_masker, events = self.selobjhelper(events, '>= 2 Medium hadronic Taus', tau_masker, tau_nummask)

        tau_mask = tau_masker.create_combined_mask(base_conditions)

        tau_proc = self.getObjProc('Tau')

        dR_mask, events = tau_proc.apply_event_level_dr(events, tau_mask, 0.5)
        self.handle_selection_masks("Tau dR >= 0.5", dR_mask)

        tau_masker.events = events
        tau_mask = tau_masker.create_combined_mask(base_conditions)
        ld_tau, sd_tau = tau_proc.apply_obj_level_dr(events, tau_mask, 0.5)
        
        self.objcollect['LDTau'] = ld_tau
        self.objcollect['SDTau'] = sd_tau
        self.objcollect['nTau'] = ak.sum(tau_mask, axis=1)

        return events
    
    def _jobjmask(self, events):
        base_conditions = {
            'pt': (opr.ge,),
            'eta': (opr.le, abs),
        }
        jet_masker = self.getObjMasker(events, "Jet")
        j_mask = jet_masker.create_combined_mask(base_conditions)
        ld_tau, _ = ObjectProcessor.fourvector(self.objcollect['LDTau'], None, sort=False)
        sd_tau, _ = ObjectProcessor.fourvector(self.objcollect['SDTau'], None, sort=False)
        jet_proc = self.getObjProc('Jet') 
        jetdR_mask = jet_proc.dRwOther(events, ld_tau, 0.4)[0] & jet_proc.dRwOther(events, sd_tau, 0.4)[0]
            
        return j_mask & jetdR_mask, jet_masker

    def selbjets(self, events, bjet_count, operator) -> ak.Array:
        # Step 1: Get basic jet mask and masker
        # - Creates a mask for jets based on pt, eta, and dR requirements (via _jobjmask)
        # - Returns both the mask and the jet masker object
        jet_mask, jet_masker = self._jobjmask(events)

        # Step 2: Create mask for minimum jet requirement
        # - Creates a mask requiring at least 2 jets that pass basic requirements
        jet_nummask = jet_masker.numselmask(jet_mask, opr.ge)

        # Step 3: Apply minimum jet selection
        # - Applies the >=2 jets requirement and updates events accordingly
        jet_masker, events = self.selobjhelper(events, '>=2 ak4 jets', jet_masker, jet_nummask)

        # Step 4: Create b-jet count mask
        # - Creates a new mask for b-tagged jets with specified count requirement
        # - Combines basic jet requirements with b-tagging requirement
        # - operator can be 'equal to' or 'greater than or equal to'
        jet_nummask = jet_masker.maskredmask(
            (self._jobjmask(events)[0] & jet_masker.custommask('btag', opr.ge)),
            operator,
            count=bjet_count
        )

        # Step 5: Set selection name based on operator
        if operator == opr.eq:
            sel_name = f'=={bjet_count} Loose B-tagged'
        elif operator == opr.ge:
            sel_name = f'>={bjet_count} Loose B-tagged'

        # Step 6: Apply b-jet selection
        # - Applies the b-jet count requirement and updates events
        jet_masker, events = self.selobjhelper(events, sel_name, jet_masker, jet_nummask)

        # Step 7: Get final jet mask
        # - Gets updated mask with all requirements
        jet_mask = self._jobjmask(events)[0]

        # Step 8: Process jets
        # - Gets jet processor object
        # - Creates zipped array of jets sorted by b-tagging score
        # - Separates into leading and sub-leading jets
        jet_proc = self.getObjProc('Jet')
        jet_zipped = jet_proc.getzipped(events, jet_mask, sort_by='btag')
        ld_jet, sd_jet = jet_zipped[:,0], jet_zipped[:,1]

        self.objcollect['LDBjet'] = ld_jet
        self.objcollect['SDBjet'] = sd_jet
        self.objcollect['nJets'] = ak.sum(jet_mask, axis=1)
        self.saveWeights(events)
        
class OneLooseB(LoosetwoTau):
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        self.selbjets(events, 1, opr.eq)

class TwoLooseB(LoosetwoTau):
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        self.selbjets(events, 2, opr.ge)

class ZeroLooseB(LoosetwoTau):
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        self.selbjets(events, 0, opr.eq)
