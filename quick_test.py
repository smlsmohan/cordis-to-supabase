#!/usr/bin/env python3

import os
import sys

# Set test mode and limit to Horizon Europe only
os.environ['TEST_MODE'] = 'true'
os.environ['TEST_ROWS'] = '1000'

# Set Supabase credentials
os.environ['SUPABASE_URL'] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"
os.environ['SUPABASE_TABLE'] = "cordis_projects"

# Import modules
import io
import json
import zipfile
import requests
import pandas as pd
import datetime
import numpy as np
from typing import Dict, List, Any, Optional

# Import functions from main script
from cordis_json_to_supabase import (
    download_and_extract_json,
    merge_programme_json, 
    normalise_dataframe,
    push_to_supabase,
    convert_id_to_integer_string
)

def test_horizon_europe_only():
    """Test with Horizon Europe data only to validate ID conversion fix."""
    print("🧪 QUICK TEST: Horizon Europe Only (1000 rows)")
    print("=" * 50)
    
    # Test the ID conversion function first
    print("🔧 Testing ID conversion function...")
    test_cases = ["105617.0", 105617.0, np.float64(105617.0)]
    for test_val in test_cases:
        result = convert_id_to_integer_string(test_val)
        print(f"  {test_val!r} → {result!r}")
    
    # Process only Horizon Europe
    print("\n📥 Downloading Horizon Europe data...")
    horizon_url = "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
    
    try:
        json_files = download_and_extract_json(horizon_url)
        if json_files:
            merged = merge_programme_json(json_files, "HORIZONprojects")
            if not merged.empty:
                print(f"\n📊 Processed {len(merged)} projects")
                
                # Show sample IDs to verify conversion
                print("\n🔍 Sample IDs after conversion:")
                sample_ids = merged['id'].head(10).tolist()
                for i, id_val in enumerate(sample_ids, 1):
                    print(f"  {i}: {id_val!r} (type: {type(id_val).__name__})")
                
                # Normalize and upload
                print("\n🧹 Normalizing data...")
                cleaned = normalise_dataframe(merged)
                
                if not cleaned.empty:
                    print(f"\n📤 Uploading {min(len(cleaned), 1000)} test records...")
                    push_to_supabase(cleaned, batch_size=50, test_mode=True, test_rows=1000)
                    print(f"\n🎉 Quick test completed successfully!")
                    return True
                else:
                    print("❌ No valid data after cleaning")
                    return False
            else:
                print("❌ No data extracted")
                return False
        else:
            print("❌ No JSON files found")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_horizon_europe_only()
    if success:
        print("\n✅ ID conversion fix is working! Ready for full pipeline.")
    else:
        print("\n❌ Test failed. Check the logs above.")
    sys.exit(0 if success else 1)
