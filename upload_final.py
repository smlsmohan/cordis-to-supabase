#!/usr/bin/env python3
import os
import sys

# Manually set environment variables for this session
os.environ['SUPABASE_URL'] = 'https://bfbhbaipgbazdhghrjho.supabase.co'
os.environ['SUPABASE_SERVICE_ROLE_KEY'] = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs'

from cordis_json_to_supabase import *

def upload_with_fixed_env():
    print("🚀 CORDIS Upload with Fixed Environment")
    print("=" * 50)
    
    # Verify environment variables
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    print(f"✅ SUPABASE_URL: {supabase_url[:30]}...")
    print(f"✅ SUPABASE_KEY: {supabase_key[:30]}...")
    
    print("\n🔄 Quick data processing...")
    
    all_dataframes = []
    
    # Process each dataset
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
        return
    
    # Combine and normalize
    print("\n🔗 Combining datasets...")
    combined = pd.concat(all_dataframes, ignore_index=True)
    
    print("🧹 Normalizing data...")
    cleaned = normalise_dataframe(combined)
    
    print(f"\n📊 Final data: {len(cleaned)} records with {len(cleaned.columns)} columns")
    
    # Save processed data
    print("💾 Saving processed data...")
    cleaned.to_pickle('cordis_data_processed.pkl')
    print(f"✅ Saved {len(cleaned)} records to cordis_data_processed.pkl")
    
    # Upload to Supabase
    print("\n🚀 Uploading to Supabase...")
    try:
        push_to_supabase(cleaned, batch_size=50, test_mode=False, test_rows=None)
        print(f"\n🎉 Upload completed! Processed {len(cleaned)} records.")
        
        # Run quality check
        print("\n🔍 Running final quality check...")
        os.system("python final_data_quality_check.py")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    upload_with_fixed_env()
