#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/final_data_quality_check.py
"""
Comprehensive data quality verification script for the completed CORDIS database.
This script provides detailed analysis of data completeness and quality metrics.
"""

import os
import pandas as pd
from supabase import create_client
from typing import Dict, List

def connect_to_supabase():
    """Connect to Supabase"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    
    return create_client(url, key)

def analyze_data_completeness(supabase) -> Dict:
    """Analyze data completeness across all columns"""
    print("📊 Analyzing Data Completeness")
    print("=" * 50)
    
    # Get all data with pagination to ensure we get all records
    all_data = []
    page_size = 1000
    offset = 0
    
    while True:
        result = supabase.table('cordis_projects').select("*").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        all_data.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    
    if not all_data:
        print("❌ No data found in database")
        return {}
    
    # Use all_data instead of result.data
    
    if not all_data:
        print("❌ No data found in database")
        return {}
    
    df = pd.DataFrame(all_data)
    total_records = len(df)
    
    print(f"📈 Total records: {total_records:,}")
    print()
    
    # Analyze each column
    completeness = {}
    
    # Group columns by category
    column_categories = {
        "Core Project Info": ["id", "acronym", "title", "objective", "status"],
        "Dates": ["startdate", "enddate", "contentupdatedate"],
        "Programme Info": ["frameworkprogramme", "legalbasis", "mastercall", "subcall", "fundingscheme"],
        "Financial": ["ecmaxcontribution", "totalcost"],
        "Organizations": ["org_names", "roles", "org_countries", "cities"],
        "Contact & URLs": ["organization_urls", "contact_forms", "project_urls"],
        "Topics & Classification": ["topics_codes", "topics_desc", "euroscivoc_labels", "euroscivoc_codes"],
        "Technical": ["rcn", "grantdoi", "programmesource"]
    }
    
    for category, columns in column_categories.items():
        print(f"\n🏷️  {category}")
        print("-" * 40)
        
        category_completeness = {}
        
        for col in columns:
            if col in df.columns:
                # Count non-null and non-empty values
                non_null = df[col].notna().sum()
                non_empty = df[col].apply(lambda x: bool(str(x).strip()) if x is not None else False).sum()
                
                completeness_pct = (non_empty / total_records) * 100
                category_completeness[col] = {
                    'count': non_empty,
                    'percentage': completeness_pct
                }
                
                # Quality rating
                if completeness_pct >= 90:
                    quality = "🟢 Excellent"
                elif completeness_pct >= 70:
                    quality = "🟡 Good"
                elif completeness_pct >= 40:
                    quality = "🟠 Fair"
                else:
                    quality = "🔴 Poor"
                
                print(f"  {col:20} | {non_empty:6,}/{total_records:,} ({completeness_pct:5.1f}%) | {quality}")
                
                # Show sample for key fields
                if col in ["frameworkprogramme", "ecmaxcontribution", "roles", "topics_codes"] and non_empty > 0:
                    sample = df[col].dropna().iloc[0]
                    sample_str = str(sample)[:50] + "..." if len(str(sample)) > 50 else str(sample)
                    print(f"                     | Sample: {sample_str}")
            else:
                print(f"  {col:20} | Column not found")
                category_completeness[col] = {'count': 0, 'percentage': 0}
        
        completeness[category] = category_completeness
    
    return completeness

def analyze_framework_distribution(supabase):
    """Analyze distribution across framework programmes"""
    print(f"\n\n📈 Framework Programme Distribution")
    print("=" * 50)
    
    result = supabase.table('cordis_projects').select("programmesource, frameworkprogramme").execute()
    
    if result.data:
        df = pd.DataFrame(result.data)
        
        # By programme source
        if 'programmesource' in df.columns:
            source_counts = df['programmesource'].value_counts()
            print("📋 By Programme Source:")
            for source, count in source_counts.items():
                print(f"  {source:15} | {count:7,} projects")
        
        # By framework programme
        if 'frameworkprogramme' in df.columns:
            framework_counts = df['frameworkprogramme'].value_counts()
            print(f"\n📋 By Framework Programme:")
            for framework, count in framework_counts.items():
                if pd.notna(framework):
                    print(f"  {framework:15} | {count:7,} projects")
        
        return source_counts.to_dict() if 'programmesource' in df.columns else {}
    
    return {}

def analyze_nested_fields_quality(supabase):
    """Analyze quality of nested fields (pipe-separated values)"""
    print(f"\n\n🔗 Nested Fields Quality Analysis")
    print("=" * 50)
    
    # Get sample data for nested fields analysis
    result = supabase.table('cordis_projects').select(
        "org_names, roles, organization_urls, contact_forms, cities, "
        "topics_codes, topics_desc, euroscivoc_labels, project_urls"
    ).limit(100).execute()
    
    if result.data:
        df = pd.DataFrame(result.data)
        
        nested_fields = ["org_names", "roles", "organization_urls", "contact_forms", 
                        "cities", "topics_codes", "topics_desc", "euroscivoc_labels"]
        
        for field in nested_fields:
            if field in df.columns:
                # Analyze pipe-separated values
                non_empty = df[field].apply(lambda x: bool(str(x).strip()) if x is not None else False).sum()
                multi_value = df[field].str.contains("|", na=False).sum()
                
                print(f"  {field:20} | {non_empty:3d}/100 populated | {multi_value:3d}/100 multi-value")
                
                # Show sample multi-value entry
                multi_samples = df[df[field].str.contains("|", na=False)][field]
                if not multi_samples.empty:
                    sample = multi_samples.iloc[0]
                    values = sample.split("|")
                    if len(values) >= 2:
                        print(f"                     | Sample ({len(values)} values): {values[0][:30]}... | {values[1][:30]}...")
                    else:
                        print(f"                     | Sample ({len(values)} values): {values[0][:30]}...")

def generate_quality_report(completeness: Dict, distribution: Dict):
    """Generate overall quality report"""
    print(f"\n\n📋 Overall Quality Report")
    print("=" * 50)
    
    # Calculate average completeness by category
    category_scores = {}
    for category, fields in completeness.items():
        if fields:
            avg_completeness = sum(field['percentage'] for field in fields.values()) / len(fields)
            category_scores[category] = avg_completeness
    
    print("📊 Category Quality Scores:")
    for category, score in sorted(category_scores.items(), key=lambda x: x[1], reverse=True):
        if score >= 80:
            quality = "🟢 Excellent"
        elif score >= 60:
            quality = "🟡 Good" 
        elif score >= 40:
            quality = "🟠 Fair"
        else:
            quality = "🔴 Needs Improvement"
        
        print(f"  {category:25} | {score:5.1f}% | {quality}")
    
    # Overall assessment
    overall_score = sum(category_scores.values()) / len(category_scores) if category_scores else 0
    
    print(f"\n🎯 Overall Data Quality Score: {overall_score:.1f}%")
    
    if overall_score >= 80:
        assessment = "🎉 EXCELLENT - Production ready!"
    elif overall_score >= 70:
        assessment = "✅ GOOD - Minor improvements needed"
    elif overall_score >= 60:
        assessment = "⚠️ FAIR - Some improvements needed"
    else:
        assessment = "🔧 POOR - Significant improvements needed"
    
    print(f"🏆 Assessment: {assessment}")
    
    return overall_score

def main():
    """Main verification function"""
    print("🔬 CORDIS Database - Final Data Quality Check")
    print("=" * 60)
    
    try:
        supabase = connect_to_supabase()
        print("✅ Connected to Supabase")
        
        # Run all analyses
        completeness = analyze_data_completeness(supabase)
        distribution = analyze_framework_distribution(supabase)
        analyze_nested_fields_quality(supabase)
        overall_score = generate_quality_report(completeness, distribution)
        
        # Save results
        results = {
            'completeness': completeness,
            'distribution': distribution,
            'overall_score': overall_score
        }
        
        import json
        with open('data_quality_report.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Detailed report saved to data_quality_report.json")
        
        return overall_score >= 70
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
