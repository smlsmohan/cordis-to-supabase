#!/usr/bin/env python3
"""
Direct upload to Supabase using the client library
This approach is more reliable for large uploads
"""

import os
import json
from supabase import create_client, Client

# Set credentials directly
SUPABASE_URL = "https://bfbhbaipgbazdhghrjho.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

def upload_test_data():
    """Upload a small test dataset to verify connection"""
    print("🧪 Testing Supabase connection with sample data...")
    
    # Create Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Test data - a few sample records
    test_records = [
        {
            "id": "test_001",
            "title": "Test Project 1",
            "acronym": "TEST1",
            "status": "ACTIVE",
            "frameworkprogramme": "HORIZON",
            "startdate": "2023-01-01",
            "enddate": "2025-12-31",
            "euroscivoc_labels": "Artificial Intelligence",
            "euroscivoc_codes": "4.43",
            "org_names": "Test University",
            "org_countries": "DE",
            "org_country_names": "Germany"
        },
        {
            "id": "test_002", 
            "title": "Test Project 2",
            "acronym": "TEST2",
            "status": "COMPLETED",
            "frameworkprogramme": "H2020",
            "startdate": "2020-01-01",
            "enddate": "2023-12-31",
            "euroscivoc_labels": "Machine Learning",
            "euroscivoc_codes": "4.43.1",
            "org_names": "Test Institute",
            "org_countries": "FR",
            "org_country_names": "France"
        }
    ]
    
    try:
        # Test the connection by inserting test records
        response = supabase.table("cordis_projects").upsert(test_records).execute()
        
        print(f"✅ Test upload successful!")
        print(f"   Uploaded {len(test_records)} test records")
        print(f"   Response: {len(response.data)} records confirmed")
        
        # Clean up test data
        print("🧹 Cleaning up test data...")
        for record in test_records:
            supabase.table("cordis_projects").delete().eq("id", record["id"]).execute()
        
        print("✅ Test data cleaned up")
        print("🎉 Supabase connection is working perfectly!")
        return True
        
    except Exception as e:
        print(f"❌ Test upload failed: {str(e)}")
        return False

def main():
    print("🚀 Supabase Connection Test")
    print("=" * 40)
    
    success = upload_test_data()
    
    if success:
        print("\n🎯 Ready for production upload!")
        print("The connection is working. You can now:")
        print("1. Run the full ETL pipeline again, or")
        print("2. Use the processed data for upload")
    else:
        print("\n❌ Connection issues detected.")
        print("Please check your Supabase credentials and table schema.")

if __name__ == "__main__":
    main()
