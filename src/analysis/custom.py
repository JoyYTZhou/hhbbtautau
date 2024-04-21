# This file contains custom event selection classes for the analysis.
# The classes are inherited from the BaseEventSelections class
# TECHNICALLY THIS SHOULD BE THE ONLY FILE THAT NEEDS TO BE MODIFIED FOR CUSTOM EVENT SELECTIONS
from .selutility import BaseEventSelections, Object
import operator as opr
import dask_awkward as dak

def switch_selections(sel_name):
    selections = {
        'lepskim': skimEvtSel,
        'tauskim': tauEvtSel,
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
        
class tauEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def setevtsel(self, events):
        tau = Object(events, "Tau")
        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le) & \
                    tau.absdzmask(opr.lt))
        
        tau_nummask = tau.numselmask(tau_mask, opr.ge)
        self.objsel.add('>= 1 Taus', tau_nummask)
        pass
 
class prelimEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def setevtsel(self, events):
        tau = Object(events, "Tau")
        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le) & \
                    tau.absdzmask(opr.lt))
        
        tau_nummask = tau.numselmask(tau_mask, opr.ge)
        self.objsel.add('>= 2 Taus', tau_nummask)

        tagger_mask = tau.custommask('idvsjet', opr.ge)
        tagger_nummask = tau.numselmask(opr.ge, (tagger_mask & tau_mask))
        self.objsel.add("2-tagged Tau Selection", tagger_nummask)
        
        jet = Object(events, 'Jet')
        j_mask = (jet.ptmask(opr.ge) &
                  jet.absetamask(opr.le) &
                  jet.custommask('btag', opr.ge) )

        j_nummask = jet.numselmask(opr.ge, j_mask)
        self.objsel.add("2 B-tagged Jets", j_nummask)

        j_lepveto_mask = (jet.custommask('jetid', opr.eq))
        j_vetonummask = jet.numselmask(opr.ge, j_lepveto_mask) 

        self.objsel.add("Jet Idx TightLepVeto", j_vetonummask)

        # Beware: this should not be considered as candidate pair selections!
        # tau_nummask = tau.evtosmask(tau_mask)
        return None

class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 