#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/check_schema.py
"""
Quick script to check if the database schema has been updated with all required columns.
"""

import os
from supabase import create_client

def main():
    print("🔍 Checking Database Schema")
    print("===========================")
    
    try:
        supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])
        
        # Try to insert a test record with all the new columns
        test_record = {
            'id': 'SCHEMA_CHECK',
            'title': 'Schema Test',
            'roles': 'coordinator|participant',
            'organization_urls': 'https://test.com',
            'contact_forms': 'contact.html',
            'cities': 'Berlin|Paris',
            'contentUpdateDate': '2024-01-01'
        }
        
        print("📝 Testing insert with new columns...")
        supabase.table('cordis_projects').insert(test_record).execute()
        print("✅ Insert successful - all new columns exist!")
        
        # Clean up test record
        supabase.table('cordis_projects').delete().eq('id', 'SCHEMA_CHECK').execute()
        print("🧹 Test record cleaned up")
        
        print("\n🎉 SCHEMA UPDATE SUCCESSFUL!")
        print("✅ Database is ready for the pipeline")
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Schema check failed: {error_msg}")
        
        if 'does not exist' in error_msg.lower():
            print("\n📋 Missing table. Please run the schema SQL:")
            print("1. Go to Supabase Dashboard → SQL Editor")
            print("2. Copy contents of simple_schema.sql")
            print("3. Paste and execute")
        elif 'column' in error_msg.lower():
            print(f"\n📋 Missing columns. Please apply schema update:")
            print("1. Go to Supabase Dashboard → SQL Editor") 
            print("2. Copy contents of simple_schema.sql")
            print("3. Paste and execute")
        
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
