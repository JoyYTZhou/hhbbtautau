import matplotlib as mpl

colors = list(mpl.colormaps['Dark2'].colors).extend(list(mpl.colormaps['tab10'].colors))

tau_pt = {
    "LeadingPt": 
        {'varname': 'pt',
        'objindx': 0,
        'bins': 135, 
        'range': [0,300],
        'xlabel': r'Leading $\tau$ $P_t$ (GeV)',
        'save': True,
        'alpha': 0.95,
        'color': colors,
        'stack': False,
        'save_name': 'LeadingTauPt'},
    "SubleadingPt": 
        {'varname': 'pt',
        'objindx': 1,
        'bins': 135, 
        'range': [0,300],
        'xlabel': r'Subleading $\tau$ $P_t$ (GeV)',
        'save': True,
        'alpha': 0.95,
        'color': colors,
        'stack': False,
        'save_name': 'SubleadingTauPt'}}

tau_eta = {
    "LeadingEta": 
        {'varname': 'eta',
        'objindx': 0,
        'bins': 135, 
        'range': [-2.1,2.1],
        'xlabel': r'Leading $\tau$ $\eta$',
        'save': True,
        'alpha': 0.95,
        'color': colors,
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
        'color': colors,
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
        'color': colors,
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
        'color': colors,
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
        'color': colors,
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
        'color': colors,
        'stack': False,
        'save_name': 'SubleadingJetBtag'}}

jet_dict = jet_btag | jet_pt