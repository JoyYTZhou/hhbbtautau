from .selutility import BaseEventSelections, Object
import operator as opr

class prelimEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def selectlep(self, events):
        electron = Object("Electron", self.lepselcfg.electron)
        muon = Object("Muon", self.lepselcfg.muon)
        tau = Object("Tau", self.lepselcfg.tau)

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