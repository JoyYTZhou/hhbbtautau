if_stack = True
hist_type = 'fill'

HT = {
    "HT": {
        "hist": {'bins': 30, 'range': [0,1500]},
        "plot": {'xlabel': r'$H_T$ (GeV)'}
        }
}

# Show mbb in infer+validation region
infer_H_mass = {
    "DiTau_mass": {
        "hist": {'bins': 30, 'range': [0,600]},
        "plot": {'xlabel': r'Visible Mass (2$\tau$) (GeV)'}},
    "DiJet_mass": {
        "hist": {'bins': 4, 'range': [0,160]},
        "plot": {'xlabel': r'Invariant Mass (2b) (GeV)'}}
}

train_H_mass = {
    "DiJet_mass": {
        "hist": {'bins': 15, 'range': [160,760]},
        "plot": {'xlabel': r'Invariant Mass (2b) (GeV)'}},
     "DiTau_mass": {
        "hist": {'bins': 30, 'range': [0,600]},
        "plot": {'xlabel': r'Visible Mass (2$\tau$) (GeV)'}}
} 

H_mass = {
    "DiJet_mass": {
        "hist": {'bins': 40, 'range': [0,250]},
        "plot": {'xlabel': r'Invariant Mass (2b) (GeV)'}},
     "DiTau_mass": {
        "hist": {'bins': 30, 'range': [0,200]},
        "plot": {'xlabel': r'Visible Mass (2$\tau$) (GeV)'}}
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
    "DiTau_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR $(\tau)$|'}}, 
    "DiJet_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR(b Jets)|'}},
    "DiHiggs_dR": {
        "hist": {'bins': 20, 'range': [0, 5]},
        "plot": {'xlabel': r'|dR(Reco H)|'}}
}


H_pt = {
    "DiTau_pt": {
        "hist": {'bins': 25, 'range': [0,500]},
        "plot": {'xlabel': r'Visible $P_t$ (2$\tau$) (GeV)'}},
    "DiJet_pt": {
        "hist": {'bins': 20, 'range': [0,400]},
        "plot": {'xlabel': r'Invariant $P_t$ (2b) (GeV)'}}
    }

bjet_pt = {
    "Bjet1_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading Jet $P_t$ (GeV)',
                 'stack': if_stack,
                 'histtype': hist_type}},
    "Bjet2_pt": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading Jet $P_t$ (GeV)',
                 'histtype': hist_type,
                 'stack': if_stack}}
}


bjet_mass = {
    "Bjet1_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Leading Jet Mass (GeV)',
                 'stack': if_stack,
                 'histtype': hist_type}},
    "Bjet2_mass": {
        "hist": {'bins': 30, 'range': [0,300]},
        "plot": {'xlabel': r'Subleading Jet Mass (GeV)',
                 'histtype': hist_type,
                 'stack': if_stack}}
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
