import pytest
import numpy as np
import awkward as ak
from src.analysis.objutil import ObjectProcessor

@pytest.fixture
def sample_events():
    return ak.Array({
        'Muon_pt': ak.Array([[50.0, 40.0, 30.0], [45.0, 35.0], [60.0, 55.0, 20.0]]),
        'Muon_eta': ak.Array([[0.5, 0.6, 0.7], [0.4, 0.8], [0.3, 0.9, 1.0]]),
        'Muon_phi': ak.Array([[0.1, 0.2, 0.3], [0.4, 0.5], [0.6, 0.7, 0.8]]),
        'Muon_mass': ak.Array([[0.105, 0.105, 0.105], [0.105, 0.105], [0.105, 0.105, 0.105]]),
    })
    
@pytest.fixture
def large_sample_events():
    # Create a sample with 10 events, each containing 3-5 muons
    return ak.Array({
        'Muon_pt': ak.Array([
            [100.0, 80.0, 60.0, 40.0, 20.0],           # Event 0: 5 muons
            [95.0, 75.0, 55.0, 35.0],                  # Event 1: 4 muons
            [120.0, 90.0, 70.0, 50.0, 30.0],           # Event 2: 5 muons
            [85.0, 65.0, 45.0],                        # Event 3: 3 muons
            [110.0, 88.0, 66.0, 44.0],                 # Event 4: 4 muons
            [105.0, 85.0, 65.0, 45.0, 25.0],           # Event 5: 5 muons
            [98.0, 78.0, 58.0, 38.0],                  # Event 6: 4 muons
            [115.0, 95.0, 75.0],                       # Event 7: 3 muons
            [92.0, 72.0, 52.0, 32.0],                  # Event 8: 4 muons
            [108.0, 88.0, 68.0, 48.0, 28.0],          # Event 9: 5 muons
        ]),
        'Muon_eta': ak.Array([
            [0.1, 0.5, 1.0, 1.5, 2.0],
            [-0.2, 0.3, 0.8, 1.3],
            [0.0, 0.4, 0.9, 1.4, 1.9],
            [-0.3, 0.2, 0.7],
            [0.15, 0.55, 1.05, 1.55],
            [-0.1, 0.35, 0.85, 1.35, 1.85],
            [0.25, 0.65, 1.15, 1.65],
            [-0.15, 0.45, 0.95],
            [0.05, 0.45, 0.95, 1.45],
            [-0.25, 0.25, 0.75, 1.25, 1.75],
        ]),
        'Muon_phi': ak.Array([
            [0.0, 0.5, 1.0, 1.5, 2.0],
            [0.2, 0.7, 1.2, 1.7],
            [0.1, 0.6, 1.1, 1.6, 2.1],
            [0.3, 0.8, 1.3],
            [0.15, 0.65, 1.15, 1.65],
            [0.25, 0.75, 1.25, 1.75, 2.25],
            [0.35, 0.85, 1.35, 1.85],
            [0.45, 0.95, 1.45],
            [0.55, 1.05, 1.55, 2.05],
            [0.65, 1.15, 1.65, 2.15, 2.65],
        ]),
        'Muon_mass': ak.Array([
            [0.105, 0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105],
            [0.105, 0.105, 0.105, 0.105, 0.105],
        ]),
    })

@pytest.fixture
def large_sample_mask():
    # Create a mask that selects most muons but excludes some
    return ak.Array([
        [True, True, True, False, False],    # Event 0: select first 3
        [True, True, True, False],           # Event 1: select first 3
        [True, True, True, True, False],     # Event 2: select first 4
        [True, True, False],                 # Event 3: select first 2
        [True, True, True, False],           # Event 4: select first 3
        [True, True, True, True, False],     # Event 5: select first 4
        [True, True, False, False],          # Event 6: select first 2
        [True, True, True],                  # Event 7: select all 3
        [True, True, True, False],           # Event 8: select first 3
        [True, True, True, False, False],    # Event 9: select first 3
    ])

def test_get_dr_selection_results_large(large_sample_events, object_processor, large_sample_mask):
    # Get selection results
    event_mask, filtered_events, leading, subleading = object_processor.get_dr_selection_results(
        large_sample_events,
        large_sample_mask,
        dr_threshold=0.4
    )
    
    # Test 1: Check basic dimensions and types
    assert len(event_mask) == 10  # Should have 10 events
    assert len(filtered_events) == ak.sum(event_mask)
    assert len(leading) == len(filtered_events)
    assert len(subleading) == len(filtered_events)
    
    # Test 2: Verify leading objects have highest pT
    assert ak.all(leading.pt >= ak.max(subleading.pt, axis=1, keepdims=True))

    # Test 3: Check deltaR separation between leading and subleading
    for evt_idx in range(len(leading)):
        if len(subleading[evt_idx]) > 0:
            deta = leading[evt_idx].eta - subleading[evt_idx].eta
            dphi = leading[evt_idx].phi - subleading[evt_idx].phi
            dr = np.sqrt(deta**2 + dphi**2)
            assert ak.all(dr >= 0.4)
    
    # Test 4: Verify pT ordering
    assert ak.all(leading.pt >= 60.0)  # All leading muons should have pT >= 60 GeV
    
    # Test 5: Check event selection efficiency
    n_passing = ak.sum(event_mask)
    assert n_passing > 0  # Should have some passing events
    assert n_passing <= 10  # Cannot have more passing events than input events
    
    # Test 6: Verify muon multiplicity
    assert ak.all(ak.num(leading) <= 1)  # Should have at most 1 leading muon per event
    assert ak.all(ak.num(subleading) <= 1)  # Should have at most 1 subleading muon per event

    # Test 7: Check specific kinematic ranges
    assert ak.all(abs(leading.eta) < 2.5)  # Standard eta acceptance
    assert ak.all(abs(subleading.eta) < 2.5)

@pytest.fixture
def object_processor():
    return ObjectProcessor("Muon", {})

@pytest.fixture
def sample_mask():
    return ak.Array([[True, True, False], [True, True], [True, True, False]])

def test_dRwSelf():
    events = {
        'obj_pt': ak.Array([[100.0, 50.0, 25.0], [80.0, 40.0, 20.0]]),
        'obj_eta': ak.Array([[0.0, 0.5, 0.8], [0.0, 0.4, 0.6]]),
        'obj_phi': ak.Array([[0.0, 0.5, 1.0], [0.0, 0.4, 0.8]]),
        'obj_mass': ak.Array([[0.1, 0.1, 0.1], [0.1, 0.1, 0.1]])
    }

    obj_processor = ObjectProcessor('obj', {})
    mask = ak.Array([[True, True, True], [True, True, True]])
    threshold = 0.4
    result = obj_processor.dRwSelf(events, threshold, mask)

    # Check basic length
    assert len(result) == 2
    assert all(len(event) == 2 for event in result)

    # Check dimensionality
    assert len(ak.flatten(result, axis=None).tolist()) == 4  # 2 events × 2 objects
    assert ak.num(result, axis=0) == 2  # number of events
    assert all(ak.num(result, axis=1) == 2)  # number of objects per event

    deta1 = 0.5
    dphi1 = 0.5
    dr1 = np.sqrt(deta1**2 + dphi1**2)

    deta2 = 0.8
    dphi2 = 1.0
    dr2 = np.sqrt(deta2**2 + dphi2**2)

    assert result[0][0] == (dr1 > threshold)
    assert result[0][1] == (dr2 > threshold)

    result_unsorted = obj_processor.dRwSelf(events, threshold, mask, sort=False)
    assert len(result_unsorted) == 2

def test_get_dr_selection_results(sample_events, object_processor, sample_mask):
    # Get selection results
    event_mask, filtered_events, leading, subleading = object_processor.get_dr_selection_results(
        sample_events, 
        sample_mask,
        dr_threshold=0.5
    )
    
    # Test 1: Check return types and lengths
    assert len(event_mask) == len(sample_events)
    assert len(filtered_events) == ak.sum(event_mask)
    assert len(leading) == len(filtered_events)
    assert len(subleading) == len(filtered_events)
    
    # Test 2: Verify leading objects have highest pT
    assert ak.all(leading.pt >= ak.max(subleading.pt, axis=1, keepdims=True))

    # Test 3: Check deltaR separation
    for evt_idx in range(len(leading)):
        if len(subleading[evt_idx]) > 0:
            deta = leading[evt_idx].eta - subleading[evt_idx].eta
            dphi = leading[evt_idx].phi - subleading[evt_idx].phi
            dr = np.sqrt(deta**2 + dphi**2)
            assert ak.all(dr >= 0.5)
            
    # Test 4: Verify event filtering
    assert len(filtered_events) <= len(sample_events)

# def test_apply_dr_selections_empty(object_processor):
#     empty_events = ak.Array({
#         'Muon_pt': ak.Array([[], []]),
#         'Muon_eta': ak.Array([[], []]),
#         'Muon_phi': ak.Array([[], []]),
#         'Muon_mass': ak.Array([[], []]),
#     })
#     empty_mask = ak.Array([[], []])
    
#     leading, subleading = object_processor.apply_dr_selections(
#         empty_events, 
#         empty_mask, 
#         dr_threshold=0.5
#     )
    
#     assert len(leading) == len(empty_events)
#     assert len(subleading) == len(empty_events)
#     assert ak.all(ak.num(leading) == 0)
#     assert ak.all(ak.num(subleading) == 0)

# def test_apply_dr_selections_threshold(sample_events, object_processor, sample_mask):
#     leading_small, subleading_small = object_processor.apply_dr_selections(
#         sample_events, 
#         sample_mask, 
#         dr_threshold=0.1
#     )
    
#     leading_large, subleading_large = object_processor.apply_dr_selections(
#         sample_events, 
#         sample_mask, 
#         dr_threshold=2.0
#     )
    
#     assert ak.sum(ak.num(leading_small)) <= ak.sum(ak.num(leading_large))

# def test_apply_dr_selections_deltaR_calculation(sample_events, object_processor, sample_mask):
#     dr_threshold = 0.5
#     leading, subleading = object_processor.apply_dr_selections(
#         sample_events, 
#         sample_mask, 
#         dr_threshold=dr_threshold
#     )
    
#     for i in range(len(leading)):
#         if len(leading[i]) > 0 and len(subleading[i]) > 0:
#             dR = np.sqrt(
#                 (leading[i].eta - subleading[i].eta)**2 + 
#                 (leading[i].phi - subleading[i].phi)**2
#             )
#             assert dR >= dr_threshold

# def test_apply_dr_selections_sorting(sample_events, object_processor, sample_mask):
#     leading, subleading = object_processor.apply_dr_selections(
#         sample_events, 
#         sample_mask, 
#         dr_threshold=0.5
#     )
    
#     for i in range(len(leading)):
#         if len(leading[i]) > 0:
#             assert leading[i].pt >= ak.max(sample_events.Muon_pt[i][sample_mask[i]][1:])

if __name__ == '__main__':
    pytest.main([__file__])