from src.utils.coffeautil import weightedSelection
import numpy as np
import dask_awkward
import awkward as ak

def test_add_sequential():
    """Test the add_sequential method of weightedSelection class"""
    # Create a dummy weight array for testing
    weights = np.array([1.0, 1.0, 1.0, 1.0])
    selection = weightedSelection(weights)

    # Test with numpy arrays
    print("Testing with numpy arrays:")
    lastsel_np = np.array([True, False, True, False])  # length 4
    thissel_np = np.array([True, False])  # length 2 (matches number of True in lastsel)
    
    print("Input lastsel:", lastsel_np)
    print("Input thissel:", thissel_np)
    
    selection.add_sequential("test_np", thissel_np, lastsel_np)
    result_np = selection.all("test_np")
    print("Result:", result_np)
    # Expected: [True, False, False, False]
    # Because:
    # - First True in lastsel gets first value from thissel (True)
    # - Second True in lastsel gets second value from thissel (False)
    # - False positions in lastsel stay False

    # Test with dask_awkward arrays
    print("\nTesting with dask_awkward arrays:")
    lastsel_da = dask_awkward.from_awkward(ak.Array([True, False, True, False]), npartitions=1)
    thissel_da = dask_awkward.from_awkward(ak.Array([True, False]), npartitions=1) 
    
    print("Input lastsel:", lastsel_da.compute())
    print("Input thissel:", thissel_da.compute())
    
    selection.add_sequential("test_da", thissel_da, lastsel_da)
    result_da = selection.all("test_da")
    print("Result:", result_da.compute())
    # Expected: [True, False, False, False]

    # Test with fill_value=True
    print("\nTesting with fill_value=True:")
    selection.add_sequential("test_fill", thissel_np, lastsel_np, fill_value=True)
    result_fill = selection.all("test_fill")
    print("Result:", result_fill)
    # Expected: [True, True, False, True]
    # Because non-selected positions get fill_value=True

if __name__ == "__main__":
    test_add_sequential()