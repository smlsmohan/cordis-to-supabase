#!/usr/bin/env python3
"""
Verify Table Schema
===================

This script verifies that the Supabase table has all required columns.
"""

import os
import requests

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

def verify_table_schema():
    """Verify the table has all required columns."""
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    # Expected columns from the script
    expected_columns = [
        "id", "acronym", "status", "title", "objective",
        "startDate", "endDate",
        "frameworkProgramme", "legalBasis", "masterCall", "subCall", "fundingScheme",
        "ecMaxContribution", "totalCost",
        "org_names", "coordinator_names", "org_countries",
        "topics_codes", "topics_desc",
        "euroSciVoc_labels", "euroSciVoc_codes",
        "project_urls", "contentUpdateDate", "rcn", "grantDoi", "programmeSource",
    ]
    
    print("🔍 Verifying table schema...")
    
    try:
        # Try to get one record to see the structure
        endpoint = f"{supabase_url}/rest/v1/cordis_projects?limit=1"
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }
        
        resp = requests.get(endpoint, headers=headers)
        
        if resp.status_code == 200:
            data = resp.json()
            if data:
                actual_columns = list(data[0].keys())
                print(f"✅ Table accessible with {len(actual_columns)} columns")
            else:
                # Table exists but is empty, try to insert a test record
                print("📝 Table is empty, testing with insert...")
                test_record = {
                    "id": "SCHEMA_TEST",
                    "acronym": "TEST",
                    "title": "Schema Test",
                    "programmeSource": "test"
                }
                
                insert_endpoint = f"{supabase_url}/rest/v1/cordis_projects"
                insert_headers = {
                    **headers,
                    "Content-Type": "application/json",
                    "Prefer": "return=representation"
                }
                
                import json
                resp = requests.post(insert_endpoint, headers=insert_headers, 
                                   data=json.dumps([test_record]))
                
                if resp.status_code in (200, 201):
                    data = resp.json()
                    actual_columns = list(data[0].keys()) if data else []
                    print(f"✅ Test insert successful, found {len(actual_columns)} columns")
                else:
                    print(f"❌ Test insert failed: {resp.status_code}")
                    print(f"Response: {resp.text}")
                    return False
            
            # Check for missing columns
            missing_columns = [col for col in expected_columns if col not in actual_columns]
            extra_columns = [col for col in actual_columns if col not in expected_columns + ['created_at', 'updated_at']]
            
            print(f"\n📊 Column Analysis:")
            print(f"   Expected: {len(expected_columns)} columns")
            print(f"   Found: {len(actual_columns)} columns")
            
            if missing_columns:
                print(f"   ❌ Missing: {missing_columns}")
            else:
                print(f"   ✅ All expected columns present")
                
            if extra_columns:
                print(f"   ℹ️  Extra columns: {extra_columns}")
            
            print(f"\n📋 All columns found:")
            for col in sorted(actual_columns):
                status = "✅" if col in expected_columns else "ℹ️ "
                print(f"   {status} {col}")
            
            return len(missing_columns) == 0
            
        else:
            print(f"❌ Failed to access table: {resp.status_code}")
            print(f"Response: {resp.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = verify_table_schema()
    print(f"\n{'🎉 Schema verification passed!' if success else '🔧 Please run the fixed_schema.sql in Supabase'}")
