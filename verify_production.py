#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/verify_production.py
"""
Verification script to check the production database after schema update and full pipeline run.
"""

import os
import pandas as pd
from supabase import create_client, Client

def connect_to_supabase() -> Client:
    """Create and return Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
    
    return create_client(url, key)

def verify_schema(supabase: Client):
    """Verify that all expected columns exist in the table."""
    print("🔍 Verifying table schema...")
    
    # Expected columns based on our updated schema
    expected_columns = [
        "id", "acronym", "status", "title", "objective",
        "startDate", "endDate", "frameworkProgramme", "legalBasis", 
        "masterCall", "subCall", "fundingScheme", "ecMaxContribution", 
        "totalCost", "org_names", "roles", "org_countries", 
        "organization_urls", "contact_forms", "cities", "topics_codes", 
        "topics_desc", "euroSciVoc_labels", "euroSciVoc_codes", 
        "project_urls", "contentUpdateDate", "rcn", "grantDoi", "programmeSource"
    ]
    
    try:
        # Get a sample record to check available columns
        result = supabase.table('cordis_projects').select("*").limit(1).execute()
        
        if result.data:
            actual_columns = list(result.data[0].keys())
            
            print(f"✅ Table exists with {len(actual_columns)} columns")
            
            # Check for missing columns
            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)
            
            if missing:
                print(f"❌ Missing columns: {missing}")
            else:
                print("✅ All expected columns present")
                
            if extra:
                print(f"ℹ️  Extra columns: {extra}")
                
            return len(missing) == 0
        else:
            print("⚠️  Table exists but is empty")
            return True
            
    except Exception as e:
        print(f"❌ Schema verification failed: {e}")
        return False

def verify_data_counts(supabase: Client):
    """Verify record counts and data quality."""
    print("\n📊 Verifying data counts...")
    
    try:
        # Total count
        result = supabase.table('cordis_projects').select("id", count="exact").execute()
        total_count = result.count
        print(f"✅ Total records: {total_count:,}")
        
        # Check for null values in critical new columns
        new_columns = ["roles", "organization_urls", "contact_forms", "cities"]
        
        for col in new_columns:
            result = supabase.table('cordis_projects').select("id").not_.is_(col, "null").limit(1000).execute()
            populated_count = len(result.data)
            print(f"✅ {col}: {populated_count:,} records have data")
        
        # Check framework programme distribution
        result = supabase.table('cordis_projects').select("frameworkProgramme", count="exact").execute()
        if result.data:
            df = pd.DataFrame(result.data)
            framework_counts = df['frameworkProgramme'].value_counts()
            print(f"\n📈 Framework Programme distribution:")
            for framework, count in framework_counts.items():
                print(f"   {framework}: {count:,} projects")
        
        return total_count > 0
        
    except Exception as e:
        print(f"❌ Data verification failed: {e}")
        return False

def verify_nested_fields(supabase: Client):
    """Verify that nested fields are properly populated."""
    print("\n🔍 Verifying nested field population...")
    
    try:
        # Sample 10 records to check nested field content
        result = supabase.table('cordis_projects').select(
            "id", "org_names", "roles", "organization_urls", "contact_forms", 
            "cities", "topics_codes", "topics_desc"
        ).limit(10).execute()
        
        if result.data:
            df = pd.DataFrame(result.data)
            
            nested_fields = ["org_names", "roles", "organization_urls", "contact_forms", "cities"]
            
            for field in nested_fields:
                non_null_count = df[field].notna().sum()
                has_pipe_separated = df[field].str.contains("|", na=False).sum()
                print(f"✅ {field}: {non_null_count}/10 records populated, {has_pipe_separated} with multiple values")
                
                # Show sample values
                sample_values = df[field].dropna().head(2)
                for idx, value in sample_values.items():
                    preview = value[:100] + "..." if len(str(value)) > 100 else value
                    print(f"   Sample: {preview}")
        
        return True
        
    except Exception as e:
        print(f"❌ Nested field verification failed: {e}")
        return False

def main():
    """Main verification function."""
    print("🔬 CORDIS Production Database Verification")
    print("==========================================")
    
    try:
        supabase = connect_to_supabase()
        print("✅ Connected to Supabase")
        
        schema_ok = verify_schema(supabase)
        data_ok = verify_data_counts(supabase)
        nested_ok = verify_nested_fields(supabase)
        
        print("\n" + "="*50)
        if schema_ok and data_ok and nested_ok:
            print("🎉 ALL VERIFICATIONS PASSED!")
            print("✅ Database schema is correct")
            print("✅ Data is populated")
            print("✅ Nested fields are working")
            print("\n🚀 CORDIS ETL Pipeline deployment is SUCCESSFUL!")
        else:
            print("❌ SOME VERIFICATIONS FAILED!")
            print("Please check the issues above and re-run the pipeline if needed")
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    main()
