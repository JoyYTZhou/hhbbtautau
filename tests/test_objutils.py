import pytest
import awkward as ak
import numpy as np
from src.analysis.objutil import ObjectProcessor
import vector as vec

@pytest.fixture
def sample_events():
    """Create sample events with simple particle data."""
    return ak.Array({
        'Muon_pt': ak.Array([[50.0, 40.0, 30.0], [45.0, 35.0], [60.0, 55.0, 20.0]]),
        'Muon_eta': ak.Array([[0.5, 0.6, 0.7], [0.4, 0.8], [0.3, 0.9, 1.0]]),
        'Muon_phi': ak.Array([[0.1, 0.2, 0.3], [0.4, 0.5], [0.6, 0.7, 0.8]]),
        'Muon_mass': ak.Array([[0.105, 0.105, 0.105], [0.105, 0.105], [0.105, 0.105, 0.105]]),
    })

@pytest.fixture
def object_processor():
    """Create an ObjectProcessor instance for testing."""
    return ObjectProcessor("Muon", {})

@pytest.fixture
def sample_mask():
    """Create a sample mask for testing."""
    return ak.Array([[True, True, False], [True, True], [True, True, False]])

def test_apply_dr_selections_basic(sample_events, object_processor, sample_mask):
    """Test basic functionality of apply_dr_selections."""
    # Apply dR selections
    leading, subleading = object_processor.apply_dr_selections(
        sample_events, 
        sample_mask, 
        dr_threshold=0.5
    )
    
    # Check that we get the expected shapes
    assert len(leading) == len(sample_events)
    assert len(subleading) == len(sample_events)
    
    # Check that leading objects have higher pt than subleading
    assert all(ak.to_numpy(leading.pt) >= ak.to_numpy(subleading.pt))

def test_apply_dr_selections_empty(object_processor):
    """Test apply_dr_selections with empty events."""
    empty_events = ak.Array({
        'Muon_pt': ak.Array([[], []]),
        'Muon_eta': ak.Array([[], []]),
        'Muon_phi': ak.Array([[], []]),
        'Muon_mass': ak.Array([[], []]),
    })
    empty_mask = ak.Array([[], []])
    
    leading, subleading = object_processor.apply_dr_selections(
        empty_events, 
        empty_mask, 
        dr_threshold=0.5
    )
    
    assert len(leading) == len(empty_events)
    assert len(subleading) == len(empty_events)
    assert ak.all(ak.num(leading) == 0)
    assert ak.all(ak.num(subleading) == 0)

def test_apply_dr_selections_threshold(sample_events, object_processor, sample_mask):
    """Test different dR threshold values."""
    # Test with very small dR threshold
    leading_small, subleading_small = object_processor.apply_dr_selections(
        sample_events, 
        sample_mask, 
        dr_threshold=0.1
    )
    
    # Test with very large dR threshold
    leading_large, subleading_large = object_processor.apply_dr_selections(
        sample_events, 
        sample_mask, 
        dr_threshold=2.0
    )
    
    # The smaller threshold should be more restrictive
    assert ak.sum(ak.num(leading_small)) <= ak.sum(ak.num(leading_large))

def test_apply_dr_selections_deltaR_calculation(sample_events, object_processor, sample_mask):
    """Test that dR calculation is correct."""
    dr_threshold = 0.5
    leading, subleading = object_processor.apply_dr_selections(
        sample_events, 
        sample_mask, 
        dr_threshold=dr_threshold
    )
    
    # Manually calculate dR between leading and subleading objects
    for i in range(len(leading)):
        if len(leading[i]) > 0 and len(subleading[i]) > 0:
            dR = np.sqrt(
                (leading[i].eta - subleading[i].eta)**2 + 
                (leading[i].phi - subleading[i].phi)**2
            )
            assert dR >= dr_threshold

def test_apply_dr_selections_sorting(sample_events, object_processor, sample_mask):
    """Test that objects are properly sorted by pt."""
    leading, subleading = object_processor.apply_dr_selections(
        sample_events, 
        sample_mask, 
        dr_threshold=0.5
    )
    
    # Check that leading objects have the highest pt
    for i in range(len(leading)):
        if len(leading[i]) > 0:
            assert leading[i].pt >= ak.max(sample_events.Muon_pt[i][sample_mask[i]][1:])

if __name__ == '__main__':
    pytest.main([__file__])