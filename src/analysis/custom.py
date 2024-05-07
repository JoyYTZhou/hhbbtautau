# This file contains custom event selection classes for the analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from .selutility import BaseEventSelections, Object
import operator as opr
import dask_awkward as dak

def switch_selections(sel_name):
    selections = {
        'vetoskim': skimEvtSel,
        'trialskim': trialEvtSel,
        'prelim': prelimEvtSel,
        'regionA': AEvtSel,
        'tauskim': tauEvtSel
    }
    return selections.get(sel_name, BaseEventSelections)

class skimEvtSel(BaseEventSelections):
    """Reduce event sizes"""
    def triggersel(self, events):
        for trigname, value in self.trigcfg.items():
            if value:
                self.objsel.add(trigname, events[trigname])
            else:
                self.objsel.add(trigname, events[~trigname])

    def setevtsel(self, events):
        muon = Object(events, "Muon")
        electron = Object(events, "Electron")
        e_mask = (electron.ptmask(opr.ge) & \
                electron.custommask('cbtightid', opr.ge) & \
                electron.absdxymask(opr.le) & \
                electron.absetamask(opr.le) & \
                electron.absdzmask(opr.le) & \
                electron.custommask('mvaisoid', opr.eq)
                )
        elec_nummask = electron.numselmask(e_mask, opr.eq)

        m_mask = (muon.ptmask(opr.ge) & \
                muon.absdxymask(opr.le) & \
                muon.absetamask(opr.le) & \
                muon.absdzmask(opr.le) & \
                muon.custommask('looseid', opr.eq) & \
                muon.custommask('isoid', opr.ge))
        muon_nummask = muon.numselmask(m_mask, opr.eq)

        self.objsel.add_multiple({"Electron Veto": elec_nummask,
                                "Muon Veto": muon_nummask})
        return None 

class trialEvtSel(skimEvtSel):
    def setevtsel(self, events) -> None:
        muon = Object(events, "Muon")
        electron = Object(events, "Electron")
        e_mask = (electron.ptmask(opr.ge) & \
                electron.custommask('cbtightid', opr.ge) & \
                electron.absdxymask(opr.le) & \
                electron.absetamask(opr.le) & \
                electron.absdzmask(opr.le)
                )

        elec_nummask = electron.numselmask(e_mask, opr.eq)

        m_mask = (muon.ptmask(opr.ge) & \
                muon.absdxymask(opr.le) & \
                muon.absetamask(opr.le) & \
                muon.absdzmask(opr.le) & \
                muon.custommask('looseid', opr.eq))
        muon_nummask = muon.numselmask(m_mask, opr.eq)

        self.objsel.add_multiple({"Electron Veto": elec_nummask,
                                "Muon Veto": muon_nummask})
        return None

class prelimEvtSel(BaseEventSelections):
    def tausel(self, events) -> None:
        tau = Object(events, "Tau")
        def tauobjmask(tau):
            tau_mask = (tau.ptmask(opr.ge) & \
                        tau.absetamask(opr.le) & \
                        tau.absdzmask(opr.lt) & \
                        tau.custommask('idvsjet', opr.ge))
            return tau_mask

        tau_mask = tauobjmask(tau)
        tau_nummask = tau.numselmask(tau_mask, opr.ge)
        self.objsel.add('>= 2 Medium Taus', tau_nummask)
    
    def jetsel(self, events) -> None:
        pass

    def selevtsel(self, events) -> None:
        self.tausel(events)
        self.jetsel(events)
        
               
class AEvtSel(BaseEventSelections):
    def triggersel(self, events):
        return super().triggersel(events)

    def tausel(self, events) -> None:
        tau = Object(events, "Tau")
        
        def tauobjmask(tau):
            tau_mask = (tau.ptmask(opr.ge) & \
                        tau.absetamask(opr.le) & \
                        tau.absdzmask(opr.lt) & \
                        tau.custommask('idvsjet', opr.ge))
            return tau_mask

        tau_mask = tauobjmask(tau)
        tau_nummask = tau.evtosmask(tau_mask)
        self.objsel.add('>= 2 OS VVloose Taus', tau_nummask)

        events = events[tau_nummask]
        tau.events = events
        tau_mask = tauobjmask(tau)
        
        # select two OS taus candidates
        leading_tau, subleading_cand = tau.getldsd(mask=tau_mask)
        os_mask = (subleading_cand['charge'] != leading_tau['charge'])
        subleading_cand = subleading_cand[os_mask]

        leading_lv = Object.fourvector(leading_tau, sort=False)
        subleading_lvs = Object.fourvector(subleading_cand, sort=False)
        dR_mask = Object.dRoverlap(leading_lv, subleading_lvs, threshold=0.5)

        # make sure two candidates are separated enough
        tau_nummask = Object.maskredmask(dR_mask, opr.ge, 1)
        leading_tau = leading_tau[tau_nummask]
        subleading_cand = subleading_cand[tau_nummask][:,0]
        self.objsel.add('Tau dR >= 0.5', tau_nummask)
        self.objcollect['LeadingTau'] = leading_tau
        self.objcollect['SubleadingTau'] = subleading_cand

        return None
    
    def jetsel(self, events) -> None:
        jet = Object(events, 'Jet')
        
        def jobjmask(jet):
            j_mask = (jet.ptmask(opr.ge) &
                  jet.absetamask(opr.le) &
                  jet.custommask('btag', opr.ge))
            return j_mask
        
        jet_mask = jobjmask(jet)
        jet_nummask = jet.numselmask(jet_mask, opr.ge)
        events = events[jet_nummask]

        # start selecting candidate jets

        return None
        
    def selevtsel(self, events):
        self.tausel()

class tauEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def setevtsel(self, events):
        
        pass


class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 