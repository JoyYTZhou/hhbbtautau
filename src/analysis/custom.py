# This file contains custom event selection classes for the analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from .selutility import BaseEventSelections, Object
import operator as opr
import dask_awkward as dak

def switch_selections(sel_name):
    selections = {
        'lepvetoskim': skimEvtSel,
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
                continue

    def setevtsel(self, events):
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
                muon.custommask('looseid', opr.eq) & \
                muon.custommask('isoid', opr.ge))
        muon_nummask = muon.numselmask(m_mask, opr.eq)

        self.objsel.add_multiple({"Electron Veto": elec_nummask,
                                "Muon Veto": muon_nummask})
        return None 
        
class prelimEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def setevtsel(self, events):
        tau = Object(events, "Tau")
        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le) & \
                    tau.osmask())

        tau_nummask = tau.numselmask(opr.ge, tau_mask)
  
        jet = Object(events, 'Jet')
        j_mask = (jet.ptmask(opr.ge) &
                  jet.absetamask(opr.le) &
                  jet.custommask('btag', opr.ge))

        j_nummask = jet.numselmask(opr.ge, j_mask)

        self.objsel.add_multiple({"Tau Selection", tau_nummask, 
                                  "Jet Selection", j_nummask})

        return None

class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 