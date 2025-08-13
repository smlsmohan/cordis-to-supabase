#!/usr/bin/env python3
"""
Test JSON Processing
====================

Quick test to verify the JSON processing works correctly.
"""

import os
import sys

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

from cordis_json_to_supabase import download_and_extract_json, merge_programme_json, normalise_dataframe

def test_json_processing():
    """Test JSON processing with one dataset."""
    print("🧪 Testing JSON processing with FP7 dataset...")
    
    try:
        # Test with FP7 first (smaller dataset)
        url = "https://cordis.europa.eu/data/cordis-fp7projects-json.zip"
        
        print("1️⃣ Downloading and extracting JSON...")
        json_files = download_and_extract_json(url)
        
        if not json_files:
            print("❌ No JSON files extracted")
            return False
        
        print(f"  ✅ Extracted {len(json_files)} JSON files")
        for filename in json_files.keys():
            print(f"    📄 {filename}")
        
        print("2️⃣ Processing and merging data...")
        merged = merge_programme_json(json_files, "fp7projects_test")
        
        if merged.empty:
            print("❌ No data merged")
            return False
        
        print(f"  ✅ Merged data: {len(merged)} rows")
        print(f"  📊 Columns: {list(merged.columns)}")
        print(f"  📋 Sample record keys: {list(merged.iloc[0].to_dict().keys()) if len(merged) > 0 else 'None'}")
        
        print("3️⃣ Testing normalization...")
        cleaned = normalise_dataframe(merged.head(10))  # Test with first 10 rows
        
        if cleaned.empty:
            print("❌ No data after normalization")
            return False
        
        print(f"  ✅ Normalized data: {len(cleaned)} rows")
        print(f"  📊 Final columns: {list(cleaned.columns)}")
        
        # Show sample data
        if len(cleaned) > 0:
            sample = cleaned.iloc[0].to_dict()
            print("  📝 Sample record:")
            for key, value in sample.items():
                if value is not None:
                    value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                    print(f"    {key}: {value_str}")
        
        print("🎉 JSON processing test successful!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_json_processing()
    if success:
        print("\n🚀 Ready to run the full JSON pipeline with ./run_json.sh")
    else:
        print("\n🔧 Please check the errors above")
    
    sys.exit(0 if success else 1)
