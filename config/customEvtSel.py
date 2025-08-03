# This file contains custom event selection classes for the src.analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from src.analysis.evtselutil import SkimSelections, BaseEventSelections, PreselSelections
from src.analysis.objutil import ObjectProcessor
import logging

from config.projectconfg import mc_nm, data_nm, selection_sync, selection_loose, selection_vbf, dijet_trigger
import operator as opr
import awkward as ak

def switch_selections(sel_name):
    selections = {
        'tightskim': tightSkim,
        'vbfskim': VBFSkim,
        'jetskim': jetSkim,
        'onelooseb': LooseTauOneB,
        'twolooseb': LooseTauTwoB,
        'zerolooseb': LooseTauZeroB,
        'zeromediumb': MediumTauZeroB,
        'resoneb': ResOneB,
        'restwob': ResTwoB,
        'vbfpresel': VBFPresel
    }
    return selections.get(sel_name, BaseEventSelections)

ditau_trigsel = selection_sync.triggerselections
dijet_trigger = dijet_trigger.triggerselections
sync_objsel = selection_sync.objselections
vbf_trigsel = selection_vbf.triggerselections
loose_objsel = selection_loose.objselections # loosetau.yaml

class vetoSkim(SkimSelections):
    def _setevtsel(self, events):
        electron = self.getObjProc(events, "Electron")
        muon = self.getObjProc(events, "Muon")

        e_base_conditions = {
            'pt': (opr.ge,),
            'dxy': (opr.le, abs),
            'eta': (opr.le, abs),
            'dz': (opr.le, abs),
            'mvaIso_WP90': (opr.eq,)
        }
        e_mask = electron.create_combined_mask(e_base_conditions)
        elec_nummask = electron.vetomask(e_mask)

        m_base_conditions = {'pt': (opr.ge,), 'dxy': (opr.le, abs), 'eta': (opr.le, abs), 'dz': (opr.le, abs), 'mediumid': (opr.eq,), 'tightid': (opr.eq,), 'isoid04': (opr.le,)}
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

class jetSkim(vetoSkim):
    """New Ditau+Jet trigger"""
    def __init__(self, is_mc):
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=dijet_trigger, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=False, is_mc=is_mc)
    
class VBFSkim(vetoSkim):
    """VBF Trigger + Sync Vetos"""
    def __init__(self, is_mc):
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=vbf_trigsel, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=False, is_mc=is_mc)

class TwoTauMixin: 
    def seltwotriggertaus(self, events, tau_level='Medium') -> ak.Array:
        base_conditions = {'pt': (opr.ge,), 'eta': (opr.le, abs), 'dz': (opr.lt, abs), 'idvsjet': (opr.ge,), 'idvsmu': (opr.ge,), 'idvse': (opr.ge,)}
        num_selname= f'>= 2 {tau_level} hadronic Taus'
        _, events, tau_mask = self.apply_objsel_trigger_match(events, 'Tau', 15, base_conditions, num_selname)
        _, events, ld_tau, sd_tau, n_tau = self.apply_dr_selections(events, "Tau", 0.5, tau_mask, selection_name=f'{tau_level} Tau dR < 0.5', sortname='pt')

        self.objcollect['LDTau'] = ld_tau
        self.objcollect['SDTau'] = sd_tau
        self.objcollect['nTau'] = n_tau

        return events

    def seltwotaus(self, events, tau_level='Medium') -> ak.Array:
        tau_proc = self.getObjProc(events, "Tau")

        base_conditions = {'pt': (opr.ge,), 'eta': (opr.le, abs), 'dz': (opr.lt, abs), 'idvsjet': (opr.ge,), 'idvsmu': (opr.ge,), 'idvse': (opr.ge,)}
        tau_mask = tau_proc.create_combined_mask(base_conditions)
        tau_nummask = tau_proc.numselmask(tau_mask, opr.ge)
        tau_proc, events = self.apply_selection_mask(events, f'>= 2 {tau_level} hadronic Taus', tau_proc, tau_nummask)

        tau_proc, events, ld_tau, sd_tau = self.apply_dr_selections(events, "Tau", 0.5, base_conditions, selection_name=f'{tau_level} Tau dR < 0.5', sortname='pt')

        self.objcollect['LDTau'] = ld_tau
        self.objcollect['SDTau'] = sd_tau
        self.objcollect['nTau'] = ak.sum(tau_mask, axis=1)

        ld_tau_trigger_check = tau_proc.match_trigger(ld_tau, 15)
        sd_tau_trigger_check = tau_proc.match_trigger(sd_tau, 15)
        trigger_check = (ld_tau_trigger_check & sd_tau_trigger_check)
        tau_proc, events = self.apply_selection_mask(events, "Tau Trigger Match", tau_proc, trigger_check)

        return events
    
    def _jobjmask(self, events):
        base_conditions = {'pt': (opr.ge,), 'eta': (opr.le, abs), 'jetid': (opr.ge,)}
        jet_proc = self.getObjProc(events, "Jet")
        j_mask = jet_proc.create_combined_mask(base_conditions)
        ld_tau, _ = ObjectProcessor.fourvector(self.objcollect['LDTau'], None, sort=False)
        sd_tau, _ = ObjectProcessor.fourvector(self.objcollect['SDTau'], None, sort=False)
        jetdR_mask = jet_proc.dRwOther(events, ld_tau, 0.5)[0] & jet_proc.dRwOther(events, sd_tau, 0.5)[0]
            
        return j_mask & jetdR_mask, jet_proc

    def selbjets(self, events, bjet_count, operator) -> ak.Array:
        # Step 1: Get basic jet mask and masker
        # - Creates a mask for jets based on pt, eta, and dR requirements (via _jobjmask)
        # - Returns both the mask and the jet masker object
        jet_mask, jet_proc = self._jobjmask(events)

        # Step 2: Create mask for minimum jet requirement
        # - Creates a mask requiring at least 2 jets that pass basic requirements
        jet_nummask = jet_proc.numselmask(jet_mask, opr.ge)

        # Step 3: Apply minimum jet selection
        # - Applies the >=2 jets requirement and updates events accordingly
        jet_proc, events = self.apply_selection_mask(events, '>=2 ak4 jets', jet_proc, jet_nummask)

        # Step 4: Create b-jet count mask
        # - Creates a new mask for b-tagged jets with specified count requirement
        # - Combines basic jet requirements with b-tagging requirement
        # - operator can be 'equal to' or 'greater than or equal to'
        jet_nummask = jet_proc.maskredmask(
            (self._jobjmask(events)[0] & jet_proc.custommask('btag', opr.ge)),
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
        jet_proc, events = self.apply_selection_mask(events, sel_name, jet_proc, jet_nummask)

        # Step 7: Get final jet mask
        # - Gets updated mask with all requirements
        jet_mask = self._jobjmask(events)[0]

        # Step 8: Process jets
        # - Gets jet processor object
        # - Creates zipped array of jets sorted by b-tagging score
        # - Separates into leading and sub-leading jets
        jet_proc = self.getObjProc(events, 'Jet', sortname='btag')
        jet_zipped = jet_proc.getzipped(events, jet_mask)
        ld_jet, sd_jet = jet_zipped[:,0], jet_zipped[:,1]

        self.objcollect['LDBjet'] = ld_jet
        self.objcollect['SDBjet'] = sd_jet
        self.objcollect['nJets'] = ak.sum(jet_mask, axis=1)

        self.objcollect['MET_phi'] = events['MET_phi']
        self.objcollect['MET_pt'] = events['MET_pt']
        self.objcollect['MET_sumEt'] = events['MET_sumEt']
        
        if self._with_wgt: self.saveWeights(events)
        
        return events, jet_zipped

    def selvbfjets(self, events, jet_zipped) -> ak.Array:
        vbf_base = ak.num(jet_zipped['pt'], axis=1) >= 4
        self.handle_selection_masks(">=2 VBF Jets", vbf_base)
        events = events[vbf_base]
        logging.debug(f"Selection VBF mask passed {len(events)} events!")
        vbfjet_zipped = jet_zipped[vbf_base][:,2:]
        vbf_mask = (vbfjet_zipped['pt'] >= 30) & (abs(vbfjet_zipped['eta']) <= 4.7)
        print(vbf_mask)
        vbf_sum = ak.sum(vbf_mask, axis=1) >= 2
        logging.debug(f"Selection VBF mask passed {len(events)} events!")
        self.handle_selection_masks("VBF pt >= 30", vbf_sum)
        if self._with_wgt: self.saveWeights(events)
        
class LoosetwoTau(PreselSelections):
    """Implement Loose Tau Selections + b jet selections."""
    def __init__(self, is_mc) -> None:
        mapcfg = mc_nm if is_mc else data_nm
        super().__init__(trigcfg=None, objselcfg=loose_objsel, mapcfg=mapcfg, sequential=True, is_mc=is_mc)

class MediumtwoTau(PreselSelections):
    """Implement Medium Tau Selections + b jet selections."""
    def __init__(self, is_mc) -> None:
        mapcfg = mc_nm if is_mc else data_nm
        super().__init__(trigcfg=None, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=True, is_mc=is_mc)

class MediumTauZeroB(TwoTauMixin, MediumtwoTau):
    def _setevtsel(self, events):
        events = self.seltwotriggertaus(events, "Medium")
        self.selbjets(events, 0, opr.eq) 

class LooseTauOneB(TwoTauMixin, LoosetwoTau):
    """Implement Loose Tau Selections + b jet selections."""
    def _setevtsel(self, events):
        events = self.seltwotriggertaus(events, "Loose")
        self.selbjets(events, 1, opr.eq)

class LooseTauTwoB(TwoTauMixin, LoosetwoTau):
    """Implement Loose Tau Selections + b jet selections."""
    def _setevtsel(self, events):
        events= self.seltwotriggertaus(events, "Loose")
        self.selbjets(events, 2, opr.ge)

class LooseTauZeroB(TwoTauMixin, LoosetwoTau):
    def _setevtsel(self, events):
        events = self.seltwotriggertaus(events, "Loose")
        self.selbjets(events, 0, opr.eq)

class ResOneB(TwoTauMixin, PreselSelections):
    def __init__(self, is_mc) -> None:
        if is_mc:
            mapcfg = mc_nm
        else:
            mapcfg = data_nm
        super().__init__(trigcfg=ditau_trigsel, objselcfg=sync_objsel, mapcfg=mapcfg, sequential=True, is_mc=is_mc)
    
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        self.selbjets(events, 1, opr.eq)

class ResTwoB(ResOneB):
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        self.selbjets(events, 2, opr.ge)

class VBFPresel(ResOneB):
    def _setevtsel(self, events):
        events = self.seltwotaus(events)
        events, jets = self.selbjets(events, 1, opr.ge)
        self.selvbfjets(events, jets)