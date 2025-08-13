#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/improve_data_quality.py
"""
Data quality improvement script to diagnose and fix field mapping issues.
This script analyzes the actual JSON structure and creates better field mappings.
"""

import os
import io
import json
import zipfile
import requests
import pandas as pd
from typing import Dict, Any, List
from collections import Counter

def analyze_json_structure_detailed(url: str, dataset_name: str) -> Dict[str, Any]:
    """Analyze JSON structure in detail to understand field mappings"""
    print(f"\n🔍 Detailed analysis of {dataset_name}")
    print("=" * 60)
    
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        analysis = {}
        
        for filename in z.namelist():
            if filename.endswith(".json"):
                print(f"\n📄 Analyzing {filename}...")
                
                try:
                    json_content = z.read(filename).decode('utf-8')
                    json_data = json.loads(json_content)
                    
                    if isinstance(json_data, list) and json_data:
                        # Analyze structure
                        sample_size = min(5, len(json_data))
                        all_fields = Counter()
                        
                        for i in range(sample_size):
                            if isinstance(json_data[i], dict):
                                all_fields.update(json_data[i].keys())
                        
                        analysis[filename] = {
                            'total_records': len(json_data),
                            'fields': dict(all_fields),
                            'sample_record': json_data[0]
                        }
                        
                        print(f"   📊 {len(json_data)} records")
                        print(f"   🗂️  Fields: {sorted(all_fields.keys())}")
                        
                        # For project.json, show detailed field analysis
                        if filename == 'project.json':
                            print(f"\n   📋 Sample project structure:")
                            sample = json_data[0]
                            
                            # Key fields we need
                            key_fields = [
                                'projectID', 'startDate', 'endDate', 'frameworkProgramme',
                                'ecMaxContribution', 'totalCost', 'legalBasis', 'contentUpdateDate'
                            ]
                            
                            for field in key_fields:
                                if field in sample:
                                    value = sample[field]
                                    print(f"      ✅ {field}: {type(value).__name__} = {str(value)[:50]}")
                                else:
                                    print(f"      ❌ {field}: NOT FOUND")
                            
                            # Show all top-level fields with types
                            print(f"\n   📝 All top-level fields with types:")
                            for field, value in sorted(sample.items()):
                                value_type = type(value).__name__
                                if isinstance(value, (dict, list)):
                                    count = len(value) if hasattr(value, '__len__') else 0
                                    print(f"      {field:25} | {value_type:10} | size: {count}")
                                else:
                                    value_str = str(value)[:30] + "..." if len(str(value)) > 30 else str(value)
                                    print(f"      {field:25} | {value_type:10} | {value_str}")
                        
                except Exception as e:
                    print(f"   ❌ Error parsing {filename}: {e}")
                    analysis[filename] = {'error': str(e)}
        
        return analysis
        
    except Exception as e:
        print(f"❌ Failed to analyze {dataset_name}: {e}")
        return {}

def create_optimal_field_mapping(analysis: Dict[str, Any]) -> Dict[str, str]:
    """Create optimal field mapping based on analysis"""
    print(f"\n🎯 Creating optimal field mapping...")
    
    mapping = {}
    
    if 'project.json' in analysis and 'sample_record' in analysis['project.json']:
        sample = analysis['project.json']['sample_record']
        
        # Direct mappings for fields that exist
        direct_mappings = {
            'projectID': 'id',
            'acronym': 'acronym',
            'status': 'status',
            'title': 'title',
            'objective': 'objective',
            'startDate': 'startdate',
            'endDate': 'enddate',
            'frameworkProgramme': 'frameworkprogramme',
            'legalBasis': 'legalbasis',
            'masterCall': 'mastercall',
            'subCall': 'subcall',
            'fundingScheme': 'fundingscheme',
            'ecMaxContribution': 'ecmaxcontribution',
            'totalCost': 'totalcost',
            'contentUpdateDate': 'contentupdatedate',
            'rcn': 'rcn',
            'grantDoi': 'grantdoi'
        }
        
        print(f"   📋 Field availability in project.json:")
        for source_field, target_field in direct_mappings.items():
            if source_field in sample:
                mapping[source_field] = target_field
                value = sample[source_field]
                print(f"      ✅ {source_field:20} → {target_field:20} | Sample: {str(value)[:40]}")
            else:
                print(f"      ❌ {source_field:20} → {target_field:20} | NOT FOUND")
    
    return mapping

def generate_improved_extraction_code(mapping: Dict[str, str]) -> str:
    """Generate improved extraction code based on field mapping"""
    code = '''
def extract_project_fields_improved(project: Dict[str, Any]) -> Dict[str, Any]:
    """Extract project fields with improved mapping based on actual JSON structure"""
    extracted = {}
    
    # Direct field mappings'''
    
    for source_field, target_field in mapping.items():
        code += f'''
    if '{source_field}' in project:
        extracted['{target_field}'] = project['{source_field}']'''
    
    code += '''
    
    # Handle nested fields that might need flattening
    # Add any special processing here
    
    return extracted
'''
    
    return code

def main():
    """Main function to analyze and improve data quality"""
    print("🔬 CORDIS Data Quality Analysis & Improvement")
    print("=" * 50)
    
    # Analyze just Horizon Europe for now (fastest)
    url = "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
    analysis = analyze_json_structure_detailed(url, "HORIZONprojects")
    
    if analysis:
        # Create optimal field mapping
        mapping = create_optimal_field_mapping(analysis)
        
        # Generate improved code
        improved_code = generate_improved_extraction_code(mapping)
        
        print(f"\n💡 Recommendations for improving data quality:")
        print("=" * 50)
        
        if mapping:
            print(f"✅ Found {len(mapping)} direct field mappings")
            print(f"📝 Generated improved extraction code")
            
            # Save the improved mapping
            with open('improved_field_mapping.json', 'w') as f:
                json.dump(mapping, f, indent=2)
            print(f"💾 Saved mapping to improved_field_mapping.json")
            
            # Save the improved code
            with open('improved_extraction_code.py', 'w') as f:
                f.write(improved_code)
            print(f"💾 Saved code to improved_extraction_code.py")
            
        else:
            print(f"❌ No field mappings found - check JSON structure")
    
    else:
        print(f"❌ Analysis failed")

if __name__ == "__main__":
    main()
