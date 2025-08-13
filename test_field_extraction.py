#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/test_field_extraction.py
"""
Test script to see what fields are actually being extracted
"""

import os
import sys
import pandas as pd
sys.path.append('.')

# Import our functions
from cordis_json_to_supabase import download_and_extract_json, extract_project_data

def test_field_extraction():
    """Test what fields we're actually extracting"""
    print("🧪 Testing field extraction from Horizon Europe data...")
    
    # Download just one dataset
    url = "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
    json_files = download_and_extract_json(url)
    
    if 'project.json' in json_files:
        print("\n📋 Extracting from project.json...")
        df = extract_project_data(json_files['project.json'], 'project.json')
        
        if not df.empty:
            print(f"\n✅ Extracted {len(df)} projects with {len(df.columns)} columns")
            print("\n🗂️  All extracted columns:")
            for i, col in enumerate(sorted(df.columns), 1):
                non_null = df[col].notna().sum()
                sample = df[col].dropna().iloc[0] if non_null > 0 else "No data"
                sample_str = str(sample)[:50] + "..." if len(str(sample)) > 50 else str(sample)
                print(f"  {i:2d}. {col:25} | Data: {non_null:5d}/{len(df)} | Sample: {sample_str}")
            
            # Check specific missing fields
            print(f"\n🔍 Checking for missing key fields:")
            missing_fields = [
                'startdate', 'enddate', 'frameworkprogramme', 'legalbasis',
                'ecmaxcontribution', 'totalcost'
            ]
            
            for field in missing_fields:
                if field in df.columns:
                    non_null = df[field].notna().sum()
                    print(f"✅ {field}: {non_null} records have data")
                else:
                    print(f"❌ {field}: Not found in extracted data")
                    # Look for similar column names
                    similar = [col for col in df.columns if field.replace('_', '').lower() in col.replace('_', '').lower()]
                    if similar:
                        print(f"   📍 Similar columns: {similar}")
        else:
            print("❌ No data extracted from project.json")
    else:
        print("❌ project.json not found in downloaded data")

if __name__ == "__main__":
    test_field_extraction()
