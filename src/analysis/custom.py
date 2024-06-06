# This file contains custom event selection classes for the analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from .evtselutil import BaseEventSelections, Object
import operator as opr
import dask_awkward as dak

def switch_selections(sel_name):
    selections = {
        'vetoskim': skimEvtSel,
        'prelim': prelimEvtSel
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
                electron.absdxymask(opr.le) & \
                electron.absetamask(opr.le) & \
                electron.absdzmask(opr.le) & \
                electron.custommask('mvaisoid', opr.eq)
                )
        elec_nummask = electron.vetomask(e_mask)

        m_mask = (muon.ptmask(opr.ge) & \
                muon.absdxymask(opr.le) & \
                muon.absetamask(opr.le) & \
                muon.absdzmask(opr.le) & \
                muon.custommask('looseid', opr.eq) & \
                muon.custommask('isoid', opr.ge))
        muon_nummask = muon.vetomask(m_mask)

        self.objsel.add_multiple({"Electron Veto": elec_nummask,
                                "Muon Veto": muon_nummask})
        return None 

class prelimEvtSel(BaseEventSelections):
    def tausel(self, events):
        tau = Object(events, "Tau", weakrefEvt=True)
        def tauobjmask(tau: 'Object'):
            tau_mask = (tau.ptmask(opr.ge) & \
                        tau.absetamask(opr.le) & \
                        tau.absdzmask(opr.lt) & \
                        tau.custommask('idvsjet', opr.ge) & \
                        tau.custommask('idvsmu', opr.ge) & \
                        tau.custommask('idvse', opr.ge))
            return tau_mask
        tau_mask = tauobjmask(tau)
        tau_nummask = tau.numselmask(tau_mask, opr.ge)

        tau, events = self.selobjhelper(events, '>= 2 Medium hadronic Taus', tau, tau_nummask)
        tau_mask = tauobjmask(tau)

        leading_tau, sd_cand = tau.getldsd(mask=tau_mask)
        self.objcollect['LeadingTau'] = leading_tau

        dR_mask = tau.dRwSelf(threshold=0.5, mask=tau_mask)
        sd_cand = sd_cand[dR_mask]

        tau_nummask = Object.maskredmask(dR_mask, opr.ge, 1)
        tau, events = self.selobjhelper(events,'Tau dR >= 0.5', tau, tau_nummask)

        sd_cand = sd_cand[tau_nummask][:,0]
        self.objcollect['SubleadingTau'] = sd_cand
    
    # def jetsel(self, events) -> None:
    #     jet = Object(events, 'Jet')
        
    #     def jobjmask(jet: 'Object'):
    #         j_mask = (jet.ptmask(opr.ge) &
    #               jet.absetamask(opr.le) &
    #               jet.custommask('btag', opr.ge))
    #         tau_ldvec = Object.fourvector(self.objcollect['LeadingTau'])
    #         tau_sdvec = Object.fourvector(self.objcollect['SubleadingTau'])
    #         dR_mask = jet.dRwOther(tau_ldvec, 0.5) & jet.dRwOther(tau_sdvec, 0.5)
    #         return j_mask & dR_mask
        
    #     jet_mask = jobjmask(jet)
    #     jet_nummask = jet.numselmask(jet_mask, opr.ge)
    #     jet, events = self.selobjhelper(events, '>=2 Medium B-tagged jets', jet, jet_nummask)

    #     # start selecting candidate jets
    #     ld_j, sd_j = jet.getldsd(mask=jobjmask(jet))
    #     self.objcollect['LeadingBjet'] = ld_j
    #     self.objcollect['SubleadingBjet'] = sd_j[:,0]

    def setevtsel(self, events) -> None:
        self.tausel(events)