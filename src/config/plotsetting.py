tau_pt = {
    "LeadingTau_pt": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'histtype': 'step',
                'xlabel': r'Leading $\tau$ $P_t$ (GeV)',
                'alpha': 0.95,
                'stack': False}},
    "SubleadingTau_pt": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading $\tau$ $P_t$ (GeV)',
                'histtype': 'step',
                'alpha': 0.95,
                'stack': False}} 
}

tau_eta = {
    "LeadingTau_eta": {
        "hist": {'bins': 135, 'range': [-2.1,2.1]},
        "plot": {'histtype': 'step',
                 'xlabel': r'Leading $\tau$ $\eta$',
                 'alpha': 0.95,
                 'stack': False}},
    "SubleadingTau_eta": {
        "hist": {'bins': 135, 'range': [-2.1,2.1]},
        "plot": {'xlabel': r'Subleading $\tau$ $\eta$',
                 'histtype': 'step',
                 'alpha': 0.95,
                 'stack': False}}
}

tau_gen = {
    "LeadingTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Leading $\tau$ Gen Flavor',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
    "SubleadingTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Subleading $\tau$ Gen Flavor',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
}


bjet_pt = {
    "LeadingBjet_pt": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'xlabel': r'Leading Jet $P_t$ (GeV)',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
    "SubleadingBjet_pt": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading Jet $P_t$ (GeV)',
                 'histtype': 'step',
                 'alpha': 0.95,
                 'stack': False}}
}

b_tag = {
    "LeadingBjet_btag": {
        "hist": {'bins': 10, 'range': [0,1]},
        "plot": {'xlabel': r'Leading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
    "SubleadingBjet_btag": {
        "hist": {'bins': 10, 'range': [0,1]},
        "plot": {'xlabel': r'Subleading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
}

bjet_mass = {
    "LeadingBjet_mass": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'xlabel': r'Leading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
    "SubleadingBjet_mass": {
        "hist": {'bins': 135, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': False,
                 'histtype': 'step'}},
}

bjet_eta = {
    "LeadingBjet_eta": {
        "hist": {'bins': 135, 'range': [-2.1,2.1]},
        "plot": {'histtype': 'step',
                 'xlabel': r'Leading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': False}},
    "SubleadingBjet_eta": {
        "hist": {'bins': 135, 'range': [-2.1,2.1]},
        "plot": {'histtype': 'step',
                 'xlabel': r'Subleading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': False}}
}

object_dict = tau_pt | tau_eta | bjet_eta | bjet_pt | b_tag