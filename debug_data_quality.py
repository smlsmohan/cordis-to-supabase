#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/debug_data_quality.py
"""
Debug script to analyze the actual field names and data quality in CORDIS JSON files
"""

import os
import io
import json
import zipfile
import requests
import pandas as pd
from collections import Counter

def analyze_json_structure(url: str, dataset_name: str):
    """Download and analyze the structure of JSON files"""
    print(f"\n🔍 Analyzing {dataset_name}")
    print("=" * 50)
    
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        
        for filename in z.namelist():
            if filename.endswith(".json"):
                print(f"\n📄 {filename}:")
                
                try:
                    json_content = z.read(filename).decode('utf-8')
                    json_data = json.loads(json_content)
                    
                    if isinstance(json_data, list) and json_data:
                        # Analyze first few records
                        sample_size = min(3, len(json_data))
                        print(f"   📊 Array with {len(json_data)} items")
                        
                        # Get all field names from first few records
                        all_fields = set()
                        for i in range(sample_size):
                            if isinstance(json_data[i], dict):
                                all_fields.update(json_data[i].keys())
                        
                        print(f"   🗂️  Fields found: {sorted(all_fields)}")
                        
                        # Show sample values for key fields
                        if filename == 'project.json':
                            sample = json_data[0]
                            print(f"   📋 Sample project fields:")
                            for field in ['projectID', 'startDate', 'endDate', 'frameworkProgramme', 
                                        'ecMaxContribution', 'totalCost', 'legalBasis']:
                                if field in sample:
                                    value = str(sample[field])[:100]
                                    print(f"      {field}: {value}")
                                else:
                                    print(f"      {field}: NOT FOUND")
                        
                        elif filename == 'organization.json':
                            sample = json_data[0] 
                            print(f"   📋 Sample organization fields:")
                            for field in ['projectID', 'name', 'role', 'country', 'city', 'organizationURL']:
                                if field in sample:
                                    value = str(sample[field])[:50]
                                    print(f"      {field}: {value}")
                                    
                    elif isinstance(json_data, dict):
                        print(f"   📊 Object with keys: {list(json_data.keys())}")
                    
                except Exception as e:
                    print(f"   ❌ Error parsing: {e}")
                    
    except Exception as e:
        print(f"❌ Failed to analyze {dataset_name}: {e}")

def main():
    """Analyze all CORDIS datasets"""
    print("🔬 CORDIS Data Quality Analysis")
    print("===============================")
    
    datasets = {
        "HORIZONprojects": "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip",
        "h2020projects": "https://cordis.europa.eu/data/cordis-h2020projects-json.zip",
        "fp7projects": "https://cordis.europa.eu/data/cordis-fp7projects-json.zip",
    }
    
    # Just analyze Horizon Europe for now to save time
    analyze_json_structure(datasets["HORIZONprojects"], "HORIZONprojects")

if __name__ == "__main__":
    main()
