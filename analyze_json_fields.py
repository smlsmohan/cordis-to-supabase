#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/analyze_json_fields.py
"""
Quick analysis of actual field names in CORDIS JSON data
"""

import requests
import zipfile
import json
import io

def analyze_project_fields():
    """Download and analyze just the project.json file from Horizon Europe"""
    print("🔍 Analyzing actual field names in project.json...")
    
    url = "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    
    # Extract just project.json
    json_content = z.read('project.json').decode('utf-8')
    json_data = json.loads(json_content)
    
    if isinstance(json_data, list) and json_data:
        sample_project = json_data[0]
        print(f"\n📋 Fields in project.json (sample of {len(json_data)} total projects):")
        print("=" * 50)
        
        for field, value in sorted(sample_project.items()):
            value_str = str(value)[:60] + "..." if len(str(value)) > 60 else str(value)
            print(f"{field:25} | {value_str}")
        
        print(f"\n🎯 Total fields found: {len(sample_project)}")
        
        # Check specific fields we're looking for
        target_fields = [
            'startDate', 'endDate', 'frameworkProgramme', 'legalBasis',
            'ecMaxContribution', 'totalCost', 'contentUpdateDate'
        ]
        
        print(f"\n🔍 Checking for expected fields:")
        for field in target_fields:
            if field in sample_project:
                value = sample_project[field]
                print(f"✅ {field}: {value}")
            else:
                print(f"❌ {field}: NOT FOUND")
                # Look for similar fields
                similar = [f for f in sample_project.keys() if field.lower() in f.lower()]
                if similar:
                    print(f"   Similar fields: {similar}")

if __name__ == "__main__":
    analyze_project_fields()
