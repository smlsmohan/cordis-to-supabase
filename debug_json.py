#!/usr/bin/env python3
"""
Debug JSON Issues
=================

This script helps identify JSON serialization issues in the CORDIS data.
"""

import os
import json
import pandas as pd

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

from cordis_to_supabase import download_and_extract, merge_programme, normalise_dataframe

def debug_json_issues():
    """Debug JSON serialization issues."""
    
    print("🔍 Debugging JSON serialization issues...")
    
    # Test with just FP7 projects (smaller dataset)
    url = "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip"
    
    try:
        print("1️⃣ Downloading and processing FP7 data...")
        files = download_and_extract(url)
        merged = merge_programme(files, "fp7projects")
        cleaned = normalise_dataframe(merged)
        
        print(f"   ✅ Processed {len(cleaned)} rows")
        
        # Test first row
        first_row = cleaned.iloc[0].to_dict()
        print("2️⃣ Testing first row JSON serialization...")
        
        try:
            json_str = json.dumps(first_row, default=str, ensure_ascii=False)
            print("   ✅ First row JSON serialization successful")
        except Exception as e:
            print(f"   ❌ First row JSON error: {e}")
            print("   🔍 Problematic values:")
            for key, value in first_row.items():
                try:
                    json.dumps(value)
                except:
                    print(f"      - {key}: {repr(value)}")
        
        # Test first 10 rows as batch
        print("3️⃣ Testing batch of 10 rows...")
        test_batch = cleaned.head(10).to_dict(orient="records")
        
        try:
            json_str = json.dumps(test_batch, default=str, ensure_ascii=False)
            print("   ✅ Batch JSON serialization successful")
            print(f"   📊 JSON size: {len(json_str)} characters")
            
            # Check for potential issues
            if '\x00' in json_str:
                print("   ⚠️  Found null bytes in JSON")
            if len(json_str) > 1000000:  # 1MB
                print("   ⚠️  JSON size is very large")
                
        except Exception as e:
            print(f"   ❌ Batch JSON error: {e}")
            
            # Try to find problematic rows
            print("   🔍 Testing individual rows...")
            for i, row in enumerate(test_batch):
                try:
                    json.dumps(row, default=str, ensure_ascii=False)
                except Exception as row_error:
                    print(f"      ❌ Row {i} error: {row_error}")
                    print(f"         Row data: {row}")
                    break
        
        # Test column data types
        print("4️⃣ Checking column data types...")
        for col in cleaned.columns:
            dtype = cleaned[col].dtype
            has_nulls = cleaned[col].isnull().sum()
            unique_types = set(type(x).__name__ for x in cleaned[col].dropna().head(10))
            print(f"   {col}: {dtype}, {has_nulls} nulls, types: {unique_types}")
            
            # Check for problematic values
            if dtype == 'object':
                sample_values = cleaned[col].dropna().head(3).tolist()
                for val in sample_values:
                    if '\x00' in str(val):
                        print(f"      ⚠️  Found null byte in {col}: {repr(val)}")
                        break
        
        return True
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_json_issues()
