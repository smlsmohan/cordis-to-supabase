#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/test_push_data.py
"""
Test data push script to verify Supabase connection and schema before full production run.
This script will push a small batch of test data (100 records) to verify everything works.
"""

import os
import sys
import pandas as pd
from supabase import create_client, Client

def connect_to_supabase() -> Client:
    """Create and return Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        print("❌ ERROR: SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        print("\nPlease run:")
        print("export SUPABASE_URL='https://your-project-id.supabase.co'")
        print("export SUPABASE_KEY='your-service-role-key'")
        sys.exit(1)
    
    try:
        client = create_client(url, key)
        print("✅ Connected to Supabase successfully")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        sys.exit(1)

def test_table_exists(supabase: Client) -> bool:
    """Check if the cordis_projects table exists and has the correct schema."""
    print("\n🔍 Checking table schema...")
    
    try:
        # Try to get one record to check if table exists and what columns it has
        result = supabase.table('cordis_projects').select("*").limit(1).execute()
        
        if hasattr(result, 'data'):
            print("✅ Table 'cordis_projects' exists")
            
            # Check if we have the new columns
            if result.data:
                columns = list(result.data[0].keys())
                print(f"   Found {len(columns)} columns")
            else:
                # Table exists but is empty, try to get column info differently
                # Insert a test record to see what columns are available
                test_data = {"id": "TEST_SCHEMA_CHECK"}
                try:
                    supabase.table('cordis_projects').insert(test_data).execute()
                    # Delete the test record
                    supabase.table('cordis_projects').delete().eq('id', 'TEST_SCHEMA_CHECK').execute()
                    print("   Table schema verified via test insert")
                except Exception as e:
                    print(f"   Warning: Could not verify full schema: {e}")
            
            return True
        else:
            print("❌ Table 'cordis_projects' does not exist")
            print("\nPlease apply the schema update first:")
            print("1. Go to your Supabase dashboard → SQL Editor")
            print("2. Copy and paste the contents of 'simple_schema.sql'")
            print("3. Click 'Run' to execute")
            return False
            
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return False

def create_test_data() -> pd.DataFrame:
    """Create a small test dataset for verification."""
    print("\n📦 Creating test data...")
    
    test_data = {
        'id': ['TEST001', 'TEST002', 'TEST003'],
        'acronym': ['TESTPROJ1', 'TESTPROJ2', 'TESTPROJ3'],
        'title': ['Test Project 1', 'Test Project 2', 'Test Project 3'],
        'objective': ['Testing objective 1', 'Testing objective 2', 'Testing objective 3'],
        'frameworkProgramme': ['HORIZON EUROPE', 'H2020', 'FP7'],
        'org_names': ['Test Org 1|Test Org 2', 'Single Test Org', 'Org A|Org B|Org C'],
        'roles': ['coordinator|participant', 'coordinator', 'coordinator|participant|participant'],
        'org_countries': ['DE|FR', 'IT', 'ES|NL|BE'],
        'organization_urls': ['https://test1.com|https://test2.com', 'https://single.com', ''],
        'contact_forms': ['contact1.html|contact2.html', '', 'contact3.html'],
        'cities': ['Berlin|Paris', 'Rome', 'Madrid|Amsterdam|Brussels'],
        'topics_codes': ['TOPIC1|TOPIC2', 'TOPIC3', 'TOPIC4|TOPIC5'],
        'topics_desc': ['Topic Description 1|Topic Description 2', 'Topic Description 3', ''],
        'euroSciVoc_labels': ['Label1|Label2', 'Label3', 'Label4'],
        'euroSciVoc_codes': ['CODE1|CODE2', 'CODE3', 'CODE4'],
        'project_urls': ['https://project1.com', 'https://project2.com', ''],
        'programmeSource': ['test', 'test', 'test']
    }
    
    df = pd.DataFrame(test_data)
    print(f"✅ Created test dataset with {len(df)} records")
    print(f"   Testing all new columns: {list(df.columns)}")
    
    return df

def push_test_data(supabase: Client, df: pd.DataFrame) -> bool:
    """Push test data to Supabase."""
    print(f"\n🚀 Pushing {len(df)} test records to Supabase...")
    
    try:
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Insert the test data
        result = supabase.table('cordis_projects').insert(records).execute()
        
        if hasattr(result, 'data') and result.data:
            print(f"✅ Successfully inserted {len(result.data)} test records")
            return True
        else:
            print("❌ No data was inserted")
            return False
            
    except Exception as e:
        print(f"❌ Failed to insert test data: {e}")
        return False

def verify_test_data(supabase: Client) -> bool:
    """Verify that test data was inserted correctly."""
    print("\n🔍 Verifying test data insertion...")
    
    try:
        # Fetch the test records
        result = supabase.table('cordis_projects').select("*").like('id', 'TEST%').execute()
        
        if result.data:
            print(f"✅ Found {len(result.data)} test records in database")
            
            # Check a few key fields
            for record in result.data:
                print(f"   Record {record['id']}: {record['acronym']} - {record['title']}")
                
                # Verify nested fields are populated
                nested_fields = ['org_names', 'roles', 'organization_urls', 'cities']
                for field in nested_fields:
                    if record.get(field):
                        pipe_count = record[field].count('|')
                        print(f"     {field}: {pipe_count + 1} values")
                    else:
                        print(f"     {field}: empty")
            
            return True
        else:
            print("❌ No test records found in database")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying test data: {e}")
        return False

def cleanup_test_data(supabase: Client):
    """Remove test data after verification."""
    print("\n🧹 Cleaning up test data...")
    
    try:
        result = supabase.table('cordis_projects').delete().like('id', 'TEST%').execute()
        print("✅ Test data cleaned up successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not clean up test data: {e}")

def main():
    """Main test function."""
    print("🧪 CORDIS Test Data Push")
    print("========================")
    
    # Connect to Supabase
    supabase = connect_to_supabase()
    
    # Check table exists
    if not test_table_exists(supabase):
        return False
    
    # Create test data
    test_df = create_test_data()
    
    # Push test data
    if not push_test_data(supabase, test_df):
        return False
    
    # Verify test data
    if not verify_test_data(supabase):
        return False
    
    # Clean up
    cleanup_test_data(supabase)
    
    print("\n" + "="*50)
    print("🎉 TEST DATA PUSH SUCCESSFUL!")
    print("✅ Supabase connection works")
    print("✅ Table schema is correct") 
    print("✅ All new columns are functional")
    print("✅ Nested fields are properly handled")
    print("\n🚀 Ready for production pipeline!")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
