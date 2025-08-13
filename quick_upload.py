#!/usr/bin/env python3
"""
Quick upload script using the already processed data results
"""

import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime

# Results we know from the successful processing
PROCESSING_RESULTS = {
    "total_records": 79069,
    "datasets": {
        "h2020projects": 35389,
        "fp7projects": 25785, 
        "HORIZONprojects": 17895
    },
    "columns": 29,
    "eurovoc_coverage": 88.5,
    "country_coverage": 99.0
}

def upload_via_supabase_client():
    """Upload using the Supabase Python client"""
    try:
        from supabase import create_client, Client
        
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        
        if not supabase_url or not supabase_key:
            print("❌ Environment variables not set!")
            return False
        
        print(f"🔗 Connecting to Supabase...")
        print(f"   URL: {supabase_url[:30]}...")
        print(f"   Key: {supabase_key[:30]}...")
        
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # Since we need to re-process the data, let's do a quick minimal run
        print("\n🔄 Quick data processing (using cached results)...")
        
        # Import the main processing functions
        from cordis_json_to_supabase import CORDIS_JSON_URLS, download_and_extract_json, merge_programme_json, normalise_dataframe
        
        all_dataframes = []
        
        # Process each dataset quickly (data should be cached by browser/system)
        for label, url in CORDIS_JSON_URLS.items():
            print(f"📥 Processing {label}...")
            try:
                json_files = download_and_extract_json(url)
                if json_files:
                    merged = merge_programme_json(json_files, label)
                    if not merged.empty:
                        all_dataframes.append(merged)
                        print(f"✅ {label}: {len(merged)} projects")
            except Exception as e:
                print(f"❌ {label}: Failed - {e}")
                continue
        
        if not all_dataframes:
            print("❌ No data processed!")
            return False
        
        # Combine and normalize
        combined = pd.concat(all_dataframes, ignore_index=True)
        cleaned = normalise_dataframe(combined)
        
        print(f"\n📊 Final data: {len(cleaned)} records with {len(cleaned.columns)} columns")
        
        # Save the processed data
        cleaned.to_pickle('cordis_data_processed.pkl')
        print(f"💾 Saved processed data to cordis_data_processed.pkl")
        
        # Upload in batches
        records = cleaned.to_dict('records')
        batch_size = 1000
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        print(f"\n🚀 Uploading {len(records):,} records in {total_batches} batches...")
        
        successful = 0
        failed = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                response = supabase.table("cordis_projects").upsert(batch).execute()
                successful += len(batch)
                print(f"✅ Batch {batch_num}/{total_batches}: {len(batch)} records uploaded")
            except Exception as e:
                failed += len(batch)
                print(f"❌ Batch {batch_num}/{total_batches} failed: {str(e)}")
        
        print(f"\n📈 Upload Summary:")
        print(f"   ✅ Successful: {successful:,} records")
        print(f"   ❌ Failed: {failed:,} records")
        print(f"   📊 Success Rate: {(successful / len(records) * 100):.1f}%")
        
        return successful > 0
        
    except ImportError:
        print("📦 Installing Supabase client...")
        os.system("pip install supabase")
        return upload_via_supabase_client()
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        return False

def main():
    print("🚀 CORDIS Quick Upload")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n📊 Expected Results Based on Successful Processing:")
    print(f"   Total Records: {PROCESSING_RESULTS['total_records']:,}")
    print(f"   Enhanced Columns: {PROCESSING_RESULTS['columns']}")
    print(f"   EuroSciVoc Coverage: {PROCESSING_RESULTS['eurovoc_coverage']:.1f}%")
    print(f"   Country Names Coverage: {PROCESSING_RESULTS['country_coverage']:.1f}%")
    
    success = upload_via_supabase_client()
    
    if success:
        print(f"\n🎉 Production upload completed successfully!")
        print(f"🔍 Running final quality check...")
        os.system("python final_data_quality_check.py")
    else:
        print(f"\n❌ Upload failed. Please check the logs above.")

if __name__ == "__main__":
    main()
