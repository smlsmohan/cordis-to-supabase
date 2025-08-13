#!/usr/bin/env python3

import pandas as pd
import numpy as np
from cordis_json_to_supabase import convert_id_to_integer_string

def test_id_conversion():
    """Test the ID conversion function with various input types."""
    print("🧪 Testing ID conversion function...")
    
    # Test cases
    test_cases = [
        ("105617.0", "105617"),  # String with decimal
        ("105617", "105617"),    # String without decimal
        (105617.0, "105617"),    # Float
        (105617, "105617"),      # Integer
        (np.float64(105617.0), "105617"),  # NumPy float
        (np.int64(105617), "105617"),      # NumPy int
        ("abc123", "abc123"),    # Non-numeric string
        (None, None),            # None
        (np.nan, None),          # NaN
        ("", ""),                # Empty string
        (0.0, "0"),              # Zero as float
        (0, "0"),                # Zero as int
    ]
    
    print("\nTest results:")
    all_passed = True
    
    for i, (input_val, expected) in enumerate(test_cases, 1):
        try:
            result = convert_id_to_integer_string(input_val)
            passed = result == expected
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {i:2d}. {input_val!r:15} → {result!r:10} (expected {expected!r}) {status}")
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"  {i:2d}. {input_val!r:15} → ERROR: {e}")
            all_passed = False
    
    print(f"\n{'✅ All tests passed!' if all_passed else '❌ Some tests failed!'}")
    return all_passed

def test_dataframe_conversion():
    """Test ID conversion with a sample DataFrame."""
    print("\n🧪 Testing DataFrame ID conversion...")
    
    # Create test DataFrame with various ID formats
    test_data = {
        'id': [105617.0, 105618, "105619.0", "105620", np.nan, 0.0],
        'name': ['Project A', 'Project B', 'Project C', 'Project D', 'Project E', 'Project F']
    }
    
    df = pd.DataFrame(test_data)
    print("Original DataFrame:")
    print(df)
    print(f"Original ID types: {df['id'].dtype}")
    
    # Apply conversion
    df['id'] = df['id'].apply(convert_id_to_integer_string)
    
    print("\nAfter ID conversion:")
    print(df)
    
    # Check results
    expected_ids = ["105617", "105618", "105619", "105620", None, "0"]
    actual_ids = df['id'].tolist()
    
    if actual_ids == expected_ids:
        print("✅ DataFrame conversion test passed!")
        return True
    else:
        print(f"❌ DataFrame conversion test failed!")
        print(f"Expected: {expected_ids}")
        print(f"Actual:   {actual_ids}")
        return False

if __name__ == "__main__":
    print("🔧 Testing CORDIS ID Conversion Functions")
    print("=" * 50)
    
    test1_passed = test_id_conversion()
    test2_passed = test_dataframe_conversion()
    
    if test1_passed and test2_passed:
        print("\n🎉 All ID conversion tests passed! The fix should work.")
    else:
        print("\n❌ Some tests failed. The fix needs adjustment.")
