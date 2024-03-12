from .selutility import BaseEventSelections, Object
import operator as opr

class prelimEvtSel(BaseEventSelections):
    """Custom event selection class for the preliminary event selection."""
    def selectlep(self, events):
        electron = Object("Electron", events, self.output_cfg.Electron, self.lepselcfg.electron)
        muon = Object("Muon", events, self.output_cfg.Muon, self.lepselcfg.muon)
        tau = Object("Tau", events, self.output_cfg.Tau, self.lepselcfg.tau)

        if not electron.veto:
            electron_mask = (electron.ptmask(opr.ge) & \
                        electron.absetamask(opr.le) & \
                        electron.bdtidmask(opr.ge))
            electron.filter_dakzipped(electron_mask)
            elec_nummask = electron.numselmask(opr.eq)
        else: elec_nummask = electron.vetomask()

        if not muon.veto:
            muon_mask = (muon.ptmask(opr.ge) & \
                        muon.absetamask(opr.le) & \
                        muon.custommask('iso', opr.le))
            muon.filter_dakzipped(muon_mask)
            muon_nummask = muon.numselmask(opr.eq)
        else: muon_nummask = muon.vetomask()

        if not tau.veto:
            tau_mask = (tau.ptmask(opr.ge) & \
                        tau.absetamask(opr.le))
            tau.filter_dakzipped(tau_mask)
            tau_nummask = tau.numselmask(opr.ge)
        else: tau_nummask = tau.vetomask()

        self.objsel.add_multiple({"ElectronSelection": elec_nummask,
                                "MuonSelection": muon_nummask,
                                "TauSelection": tau_nummask})

        return None

class fineEvtSel(BaseEventSelections):
    """Custom event selection class for the fine event selection."""
    def selectlep(self, events):
        pass 