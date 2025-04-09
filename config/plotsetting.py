if_stack = True
hist_type = 'fill'

HT = {
    "HT": {
        "hist": {'bins': 30, 'range': [0,1500]},
        "plot": {'xlabel': r'$H_T$ (GeV)'}
        }
}

tau_pt = {
    "LDTau_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading $\tau$ $P_t$ (GeV)'}},
    "SDTau_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading $\tau$ $P_t$ (GeV)'}},
}

tau_eta = {
    "LDTau_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Leading $\tau$ $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}},
    "SDTau_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'xlabel': r'Subleading $\tau$ $\eta$',
                 'histtype': hist_type,
                 'alpha': 0.95,
                 'stack': if_stack}}
}

tau_gen = {
    "LDTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Leading $\tau$ Gen Flavor'}},
    "SDTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Subleading $\tau$ Gen Flavor'}}
}

dR = {
    "Tau_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR $(\tau)$|'}}, 
    "Bjet_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR(b Jets)|'}},
    "RecoH_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR(Reco H)|'}}
}

H_mass = {
    "DiTau_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Visible Mass (2$\tau$) (GeV)'}},
    "DiJet_mass": {
        "hist": {'bins': 40, 'range': [0,400]},
        "plot": {'xlabel': r'Invariant Mass (2b) (GeV)'}},
}

H_pt = {
    "DiTau_pt": {
        "hist": {'bins': 25, 'range': [0,500]},
        "plot": {'xlabel': r'Visible $P_t$ (2$\tau$) (GeV)'}},
    "DiJet_pt": {
        "hist": {'bins': 20, 'range': [0,400]},
        "plot": {'xlabel': r'Invariant $P_t$ (2b) (GeV)'}}
    }


bjetbytag_pt = {
    "LDBjetBYtag_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading Jet $P_t$ (GeV)',
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SDBjetBYtag_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading Jet $P_t$ (GeV)',
                 'histtype': hist_type,
                 'stack': if_stack}}
}

bjetbytag_btag = {
    "LDBjetBYtag_btag": {
        "hist": {'bins': 40, 'range': [0,1]},
        "plot": {'xlabel': r'Leading Jet B Tag Score',
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SDBjetBYtag_btag": {
        "hist": {'bins': 40, 'range': [0,1]},
        "plot": {'xlabel': r'Subleading Jet B Tag Score',
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjetbypt_btag = {
    "LDBjetBYpt_btag": {
        "hist": {'bins': 40, 'range': [0,1]},
        "plot": {'xlabel': r'Leading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SDBjetBYpt_btag": {
        "hist": {'bins': 40, 'range': [0,1]},
        "plot": {'xlabel': r'Subleading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjetbytag_mass = {
    "LDBjetBYtag_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SDBjetBYtag_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjetbypt_mass = {
    "LDBjetBYpt_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SDBjetBYpt_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjetbytag_eta = {
    "LDBjetBYtag_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Leading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}},
    "SDBjetBYtag_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Subleading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}}
}

object_dict = tau_pt | tau_eta | bjetbypt_btag | bjetbypt_mass | bjetbytag_eta | tau_gen | dR
object_dict = object_dict | H_mass