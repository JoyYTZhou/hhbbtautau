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
    "LeadingEta": 
        {'varname': 'eta',
        'objindx': 0,
        'bins': 135, 
        'range': [-2.1,2.1],
        'xlabel': r'Leading $\tau$ $\eta$',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'LeadingTauEta'},
    "SubleadingEta": 
        {'varname': 'eta',
        'objindx': 1,
        'bins': 135, 
        'range': [-2.1,2.1],
        'xlabel': r'Subleading $\tau$ $\eta$',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'SubleadingTauEta'}}

tau_dict = tau_eta | tau_pt

jet_pt = {
    "LeadingPt": 
        {'varname': 'pt',
        'objindx': 0,
        'bins': 135, 
        'range': [0,300],
        'xlabel': r'Leading Jet $P_t$ (GeV)',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'LeadingJetPt'},
    "SubleadingPt": 
        {'varname': 'pt',
        'objindx': 1,
        'bins': 135, 
        'range': [0,300],
        'xlabel': r'Subleading Jet $P_t$ (GeV)',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'SubleadingJetPt'}}

jet_btag = {
    "LeadingBtag": 
        {'varname': 'btag',
        'objindx': 0,
        'bins': 10, 
        'range': [0,1],
        'xlabel': r'Leading Jet B Tag Score',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'LeadingJetBtag'},
    "SubleadingBtag": 
        {'varname': 'btag',
        'objindx': 1,
        'bins': 10, 
        'range': [0,1],
        'xlabel': r'Subleading Jet B Tag Score',
        'save': True,
        'alpha': 0.95,
        'stack': False,
        'save_name': 'SubleadingJetBtag'}}

jet_dict = jet_btag | jet_pt