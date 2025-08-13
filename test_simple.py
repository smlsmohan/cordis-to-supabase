#!/usr/bin/env python3
"""
Quick JSON Test
===============

Simple test to verify JSON serialization fix.
"""

import os
import json
import requests

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

def test_simple_upload():
    """Test uploading a simple record to verify the connection works."""
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    endpoint = f"{supabase_url}/rest/v1/cordis_projects?on_conflict=id"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    
    # Simple test record
    test_record = {
        "id": "TEST_123",
        "acronym": "TEST",
        "status": "TEST",
        "title": "Test Project",
        "objective": "Test objective",
        "startDate": "2025-01-01",
        "endDate": "2025-12-31",
        "frameworkProgramme": "TEST",
        "programmeSource": "test"
    }
    
    print("🧪 Testing simple record upload...")
    
    try:
        json_data = json.dumps([test_record], ensure_ascii=False)
        print(f"   📄 JSON size: {len(json_data)} chars")
        
        resp = requests.post(endpoint, headers=headers, data=json_data)
        print(f"   📡 Response: {resp.status_code}")
        
        if resp.status_code in (200, 201, 204):
            print("   ✅ Simple upload successful!")
            return True
        else:
            print(f"   ❌ Upload failed: {resp.text[:500]}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_simple_upload()
