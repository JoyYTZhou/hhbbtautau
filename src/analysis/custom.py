# This file contains custom event selection classes for the analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from .selutility import BaseEventSelections, Object
import operator as opr

def switch_selections(sel_name):
    selections = {
        'skim': mockskimEvtSel,
        'prelim': prelimEvtSel
    }
    return selections.get(sel_name, BaseEventSelections)

class mockskimEvtSel(BaseEventSelections):
    """Reduce event sizes"""
    def selectlep(self, events):
        electron = Object(events, "Electron", self.lepselcfg.electron)
        muon = Object(events, "Muon", self.lepselcfg.muon)
        tau = Object(events, "Tau", self.lepselcfg.tau)

        e_mask = (electron.ptmask(opr.ge) & \
                electron.custommask('mvaisoid', opr.gt) & \
                electron.custommask('cbtightid', opr.ge) & \
                electron.absdxymask(opr.le) & \
                electron.absetamask(opr.le) & \
                electron.absdzmask(opr.le)
                )
        elec_nummask = electron.numselmask(opr.eq, e_mask)

        m_mask = (muon.ptmask(opr.ge) & \
                muon.absdxymask(opr.le) & \
                muon.absetamask(opr.le) & \
                muon.absdzmask(opr.le) & \
                muon.custommask('cbtightid', opr.ge) & \
                muon.custommask('isoid', opr.ge))
        muon_nummask = muon.numselmask(opr.eq, m_mask)

        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le))

        tau_nummask = tau.numselmask(opr.ge, tau_mask)

        self.objsel.add_multiple({"ElectronVeto": elec_nummask,
                                "MuonVeto": muon_nummask,
                                "TauSelections": tau_nummask})
        return None
        
class prelimEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def selectlep(self, events):
        tau = Object(events, 'Tau', self.lepselcfg.tau)
        self.objsel.add(name="OSTau", selection=tau.osmask())
        return None

    def selectjet(self, events):
        jet = Object(events, 'Jet', self.jetselcfg.jet)
        j_mask = (jet.ptmask(opr.ge) &
                  jet.absetamask(opr.le))
        j_nummask = jet.numselmask(opr.ge, j_mask)
        
        self.objsel.add_multiple({"JetSelection": j_nummask})
        return None

class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 