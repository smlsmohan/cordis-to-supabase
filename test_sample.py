#!/usr/bin/env python3
"""
Quick Test Script for CORDIS ETL
=================================

This script tests the main functionality with just one dataset to verify
everything is working before running the full pipeline.
"""

import os
import sys

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

from cordis_to_supabase import download_and_extract, merge_programme, normalise_dataframe, push_to_supabase

def test_single_dataset():
    """Test with just one dataset (FP7) to verify functionality."""
    
    print("🧪 Testing CORDIS ETL with FP7 dataset...")
    
    # Test with just FP7 projects (smaller dataset)
    url = "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip"
    
    try:
        print("1️⃣ Downloading and extracting data...")
        files = download_and_extract(url)
        print(f"   ✅ Downloaded {len(files)} CSV files")
        
        print("2️⃣ Merging programme data...")
        merged = merge_programme(files, "fp7projects")
        print(f"   ✅ Merged data: {len(merged)} rows, {len(merged.columns)} columns")
        
        print("3️⃣ Normalizing data...")
        cleaned = normalise_dataframe(merged)
        print(f"   ✅ Cleaned data: {len(cleaned)} rows, {len(cleaned.columns)} columns")
        
        # Test with just first 10 rows
        test_sample = cleaned.head(10)
        
        print("4️⃣ Uploading test sample (10 rows)...")
        push_to_supabase(test_sample, batch_size=5)
        print("   ✅ Sample upload successful!")
        
        print("🎉 Test completed successfully!")
        print(f"📊 Sample data columns: {list(test_sample.columns)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_single_dataset()
    if success:
        print("\n🚀 Ready to run the full pipeline with ./run.sh")
    else:
        print("\n🔧 Please check the errors above before running the full pipeline")
    
    sys.exit(0 if success else 1)
