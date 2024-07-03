if_stack = True
hist_type = 'fill'

tau_pt = {
    "LeadingTau_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'histtype': hist_type,
                'xlabel': r'Leading $\tau$ $P_t$ (GeV)',
                'alpha': 0.95,
                'stack': if_stack}},
    "SubleadingTau_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading $\tau$ $P_t$ (GeV)',
                'histtype': hist_type,
                'alpha': 0.95,
                'stack': if_stack}} 
}

tau_eta = {
    "LeadingTau_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Leading $\tau$ $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}},
    "SubleadingTau_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'xlabel': r'Subleading $\tau$ $\eta$',
                 'histtype': hist_type,
                 'alpha': 0.95,
                 'stack': if_stack}}
}

tau_gen = {
    "LeadingTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Leading $\tau$ Gen Flavor',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SubleadingTau_genflav": {
        "hist": {'bins': 6, 'range': [-0.5, 5.5]},
        "plot": {'xlabel': r'Subleading $\tau$ Gen Flavor',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

dR = {
    "Tau_dR": {
        "hist": {'bins': 20, 'range': [-5, 5]},
        "plot": {'xlabel': r'dR $(\tau)$',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}}, 
    "Bjet_dR": {
        "hist": {'bins': 20, 'range': [-5, 5]},
        "plot": {'xlabel': r'dR(B Jets)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}}, 
}


bjet_pt = {
    "LeadingBjet_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading Jet $P_t$ (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SubleadingBjet_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading Jet $P_t$ (GeV)',
                 'histtype': hist_type,
                 'alpha': 0.95,
                 'stack': if_stack}}
}

b_tag = {
    "LeadingBjet_btag": {
        "hist": {'bins': 10, 'range': [0,1]},
        "plot": {'xlabel': r'Leading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SubleadingBjet_btag": {
        "hist": {'bins': 10, 'range': [0,1]},
        "plot": {'xlabel': r'Subleading Jet B Tag Score',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjet_mass = {
    "LeadingBjet_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
    "SubleadingBjet_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading B-Jet Mass (GeV)',
                 'alpha': 0.95,
                 'stack': if_stack,
                 'histtype': hist_type}},
}

bjet_eta = {
    "LeadingBjet_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Leading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}},
    "SubleadingBjet_eta": {
        "hist": {'bins': 42, 'range': [-2.1,2.1]},
        "plot": {'histtype': hist_type,
                 'xlabel': r'Subleading B-jet $\eta$',
                 'alpha': 0.95,
                 'stack': if_stack}}
}

object_dict = tau_pt | tau_eta | bjet_eta | bjet_pt | b_tag | bjet_mass | tau_gen | dR