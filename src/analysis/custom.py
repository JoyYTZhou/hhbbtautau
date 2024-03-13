from .selutility import BaseEventSelections, Object
import operator as opr

class mockskimEvtSel(BaseEventSelections):
    """Reduce event sizes."""
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
        electron = Object(events, "Electron", self.lepselcfg.electron)
        muon = Object(events, "Muon", self.lepselcfg.muon)
        tau = Object(events, "Tau", self.lepselcfg.tau)

        electron_mask = (electron.ptmask(opr.ge) & \
                    electron.absetamask(opr.le) & \
                    electron.bdtidmask(opr.ge))

        elec_nummask = electron.numselmask(opr.ge, electron_mask)

        muon_mask = (muon.ptmask(opr.ge) & \
                    muon.absetamask(opr.le) & \
                    muon.custommask('iso', opr.le))
        muon_nummask = muon.numselmask(opr.ge, muon_mask)

        tau_mask = (tau.ptmask(opr.ge) & \
                    tau.absetamask(opr.le))

        tau_nummask = tau.numselmask(opr.ge, tau_mask)

        self.objsel.add_multiple({"ElectronSelection": elec_nummask,
                                "MuonSelection": muon_nummask,
                                "TauSelection": tau_nummask})

        return None

class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 