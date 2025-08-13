#!/usr/bin/env python3
"""
Test Problematic Record
=======================

Test the specific record that's causing issues.
"""

import os
import json
import requests

# Set environment variables
os.environ["SUPABASE_URL"] = "https://bfbhbaipgbazdhghrjho.supabase.co"
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

def test_problematic_record():
    """Test the record that was failing."""
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    endpoint = f"{supabase_url}/rest/v1/cordis_projects?on_conflict=id"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    
    # The problematic record from the error
    problematic_record = {
        'id': '101223116', 
        'acronym': '2DFERROPLEX', 
        'status': 'SIGNED', 
        'title': 'Ferroelectric 2D materials heterostructures for optical neuromorphic device functionalities',
        'objective': 'The 2DFERROPLEX project aims to pioneer a new frontier in neuromorphic computing...',  # Truncated for test
        'startDate': '2025-11-01', 
        'endDate': '2029-10-31', 
        'frameworkProgramme': 'HORIZON', 
        'legalBasis': 'HORIZON.3.1', 
        'masterCall': 'HORIZON-EIC-2024-PATHFINDERCHALLENGES-01', 
        'subCall': 'HORIZON-EIC-2024-PATHFINDERCHALLENGES-01', 
        'fundingScheme': 'HORIZON-EIC', 
        'ecMaxContribution': 3893032.0, 
        'totalCost': 0.0, 
        'contentUpdateDate': '2025-06-17', 
        'rcn': 273193, 
        'grantDoi': '10.3030/101223116', 
        'programmeSource': 'HORIZONprojects'
    }
    
    print("🧪 Testing problematic record...")
    
    try:
        # Test JSON serialization
        json_data = json.dumps([problematic_record], ensure_ascii=False)
        print(f"✅ JSON serialization successful")
        print(f"📄 JSON size: {len(json_data)} characters")
        
        # Test upload
        print("📡 Testing upload...")
        resp = requests.post(endpoint, headers=headers, data=json_data)
        
        print(f"📊 Response status: {resp.status_code}")
        
        if resp.status_code in (200, 201, 204):
            print("✅ Upload successful!")
            return True
        else:
            print(f"❌ Upload failed")
            print(f"Response: {resp.text[:500]}")
            
            # Try with minimal data
            minimal_record = {
                'id': '101223116_TEST',
                'title': 'Test Record',
                'programmeSource': 'test'
            }
            
            print("\n🔄 Trying minimal record...")
            minimal_json = json.dumps([minimal_record], ensure_ascii=False)
            minimal_resp = requests.post(endpoint, headers=headers, data=minimal_json)
            
            if minimal_resp.status_code in (200, 201, 204):
                print("✅ Minimal record upload successful - issue is with data content")
            else:
                print(f"❌ Minimal record also failed: {minimal_resp.text[:200]}")
            
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_problematic_record()
