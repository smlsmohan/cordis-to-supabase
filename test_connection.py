#!/usr/bin/env python3
"""
Test Supabase Connection
========================

This script tests the connection to your Supabase database before running
the main CORDIS extraction script.
"""

import os
import requests
import json

def test_supabase_connection():
    """Test connection to Supabase and verify table exists."""
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    table_name = os.environ.get("SUPABASE_TABLE", "cordis_projects")
    
    if not supabase_url:
        print("❌ SUPABASE_URL environment variable is not set!")
        return False
        
    if not supabase_key:
        print("❌ SUPABASE_SERVICE_ROLE_KEY environment variable is not set!")
        return False
    
    print(f"🔍 Testing connection to: {supabase_url}")
    print(f"📊 Target table: {table_name}")
    
    # Test basic API access
    try:
        endpoint = f"{supabase_url}/rest/v1/"
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }
        
        resp = requests.get(endpoint, headers=headers, timeout=10)
        resp.raise_for_status()
        print("✅ Supabase API is accessible")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to Supabase API: {e}")
        return False
    
    # Test if table exists
    try:
        endpoint = f"{supabase_url}/rest/v1/{table_name}?limit=1"
        resp = requests.get(endpoint, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ Table '{table_name}' exists and is accessible")
            print(f"📊 Current row count: checking...")
            
            # Get row count
            count_endpoint = f"{supabase_url}/rest/v1/{table_name}?select=count"
            count_resp = requests.get(count_endpoint, headers=headers, timeout=10)
            if count_resp.status_code == 200:
                count_data = count_resp.json()
                print(f"📊 Current rows in table: {len(count_data) if isinstance(count_data, list) else 'unknown'}")
            
        elif resp.status_code == 404:
            print(f"❌ Table '{table_name}' does not exist!")
            print("💡 Please run the schema.sql file in your Supabase SQL Editor first.")
            return False
        else:
            print(f"❌ Error accessing table: {resp.status_code} - {resp.text[:200]}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to check table: {e}")
        return False
    
    print("🎉 All tests passed! Ready to run the CORDIS extraction.")
    return True

if __name__ == "__main__":
    success = test_supabase_connection()
    exit(0 if success else 1)
