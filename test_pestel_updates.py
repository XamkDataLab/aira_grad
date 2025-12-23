#!/usr/bin/env python3
"""
Test script to verify PESTEL updates
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the updated functions
from pestel.pestel_gradio import (
    get_aggregated_details_case_insensitive,
    get_sectors_for_detail_items,
    generate_ai_report
)

import pandas as pd

def test_functions():
    print("Testing PESTEL updates...")
    
    # Test 1: Check if get_sectors_for_detail_items exists
    print("\n1. Testing get_sectors_for_detail_items function:")
    try:
        # This should return an empty dict if no data, but shouldn't error
        result = get_sectors_for_detail_items('Political', 'trends', ['test'])
        print(f"   Function exists and returns: {type(result)}")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test 2: Check aggregated data function
    print("\n2. Testing get_aggregated_details_case_insensitive function:")
    try:
        # Test with no limit to ensure ALL items are returned
        df = get_aggregated_details_case_insensitive('Political', 'trends', limit=None)
        print(f"   Function returns dataframe with {len(df)} rows")
        if not df.empty:
            print(f"   Columns: {list(df.columns)}")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test 3: Verify generate_ai_report includes sector data
    print("\n3. Testing generate_ai_report function signature:")
    try:
        import inspect
        sig = inspect.signature(generate_ai_report)
        print(f"   Parameters: {list(sig.parameters.keys())}")
        
        # Check if the function body includes sector handling
        source = inspect.getsource(generate_ai_report)
        if 'get_sectors_for_detail_items' in source:
            print("   ✓ Function calls get_sectors_for_detail_items")
        if 'Top sectors:' in source:
            print("   ✓ Function includes sector information in output")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    print("\n✅ Test complete!")

if __name__ == "__main__":
    test_functions()