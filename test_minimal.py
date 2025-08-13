#!/usr/bin/env python3
"""
Minimal CORDIS Test
===================

Test with just basic columns and minimal data.
"""

import os
import json
import pandas as pd
import requests

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

def test_minimal_upload():
    """Test with minimal data structure."""
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    endpoint = f"{supabase_url}/rest/v1/cordis_projects?on_conflict=id"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    
    # Test data with only basic columns
    test_data = [
        {
            "id": "TEST_001",
            "acronym": "TESTPROJECT",
            "status": "SIGNED",
            "title": "Test Project Title",
            "objective": "Test objective description",
            "startDate": "2025-01-01",
            "endDate": "2025-12-31",
            "frameworkProgramme": "HORIZON",
            "programmeSource": "test"
        }
    ]
    
    print("🧪 Testing minimal data upload...")
    print(f"📊 Test data: {test_data[0]}")
    
    try:
        # Test JSON serialization
        json_data = json.dumps(test_data, ensure_ascii=False, indent=2)
        print(f"✅ JSON serialization successful")
        print(f"📄 JSON preview:\n{json_data}")
        
        # Test upload
        print("📡 Uploading to Supabase...")
        resp = requests.post(endpoint, headers=headers, data=json_data)
        
        print(f"📊 Response status: {resp.status_code}")
        print(f"📄 Response headers: {dict(resp.headers)}")
        
        if resp.status_code in (200, 201, 204):
            print("✅ Upload successful!")
            return True
        else:
            print(f"❌ Upload failed")
            print(f"Response text: {resp.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_minimal_upload()
    print(f"\n{'✅' if success else '❌'} Test {'passed' if success else 'failed'}")
