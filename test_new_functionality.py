#!/usr/bin/env python3
"""
Test script to verify the new EuroSciVoc and country names functionality
without actually uploading to Supabase.
"""
import os
import pandas as pd
from cordis_json_to_supabase import (
    download_and_extract_json, 
    merge_programme_json,
    get_country_name,
    normalise_dataframe
)

def test_new_functionality():
    """Test the new EuroSciVoc and country names functionality."""
    print("🧪 Testing New Functionality")
    print("=" * 50)
    
    # Test country name mapping
    print("\n📍 Testing Country Name Mapping:")
    test_codes = ['DE', 'FR', 'IT', 'ES', 'US', 'UK', 'CH', 'JP']
    for code in test_codes:
        name = get_country_name(code)
        print(f"  {code} → {name}")
    
    # Test with a small dataset
    print("\n📥 Testing with HORIZON dataset (first 5 projects)...")
    
    try:
        # Download and process just HORIZON data
        url = "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
        json_files = download_and_extract_json(url)
        
        if json_files:
            merged = merge_programme_json(json_files, "HORIZONprojects")
            
            if not merged.empty:
                # Take just first 5 projects for testing
                sample = merged.head(5)
                
                print(f"\n📊 Sample Data ({len(sample)} projects):")
                print("-" * 60)
                
                # Check key columns
                key_columns = [
                    'id', 'title', 'org_countries', 'org_country_names', 
                    'euroscivoc_codes', 'euroscivoc_labels'
                ]
                
                available_cols = [col for col in key_columns if col in sample.columns]
                
                for idx, row in sample.iterrows():
                    print(f"\nProject {idx + 1}:")
                    print(f"  ID: {row.get('id', 'N/A')}")
                    print(f"  Title: {str(row.get('title', 'N/A'))[:60]}...")
                    
                    if 'org_countries' in row:
                        countries = str(row['org_countries']) if pd.notna(row['org_countries']) else 'N/A'
                        print(f"  Countries: {countries}")
                    
                    if 'org_country_names' in row:
                        country_names = str(row['org_country_names']) if pd.notna(row['org_country_names']) else 'N/A'
                        print(f"  Country Names: {country_names}")
                    
                    if 'euroscivoc_codes' in row:
                        eurovoc_codes = str(row['euroscivoc_codes']) if pd.notna(row['euroscivoc_codes']) else 'N/A'
                        print(f"  EuroSciVoc Codes: {eurovoc_codes[:80]}...")
                    
                    if 'euroscivoc_labels' in row:
                        eurovoc_labels = str(row['euroscivoc_labels']) if pd.notna(row['euroscivoc_labels']) else 'N/A'
                        print(f"  EuroSciVoc Labels: {eurovoc_labels[:80]}...")
                
                # Test normalization
                print(f"\n🧹 Testing Data Normalization...")
                normalized = normalise_dataframe(sample)
                
                print(f"✅ Normalization successful!")
                print(f"  Original columns: {len(sample.columns)}")
                print(f"  Normalized columns: {len(normalized.columns)}")
                print(f"  Available columns: {list(normalized.columns)}")
                
                # Check for our new columns
                new_columns = ['org_country_names', 'euroscivoc_labels', 'euroscivoc_codes']
                for col in new_columns:
                    if col in normalized.columns:
                        non_empty = normalized[col].notna().sum()
                        print(f"  {col}: {non_empty}/{len(normalized)} populated")
                    else:
                        print(f"  ❌ {col}: Column missing!")
                
            else:
                print("❌ No data extracted from HORIZON dataset")
        else:
            print("❌ No JSON files found in HORIZON dataset")
            
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_new_functionality()
