#!/usr/bin/env python3
"""
Save processed data and upload to Supabase
This script saves the already processed data and uploads it
"""

import os
import pandas as pd
from cordis_json_to_supabase import *

def save_and_upload_data():
    """Re-run just the final steps to save and upload data"""
    print("🔄 Re-running final processing and upload...")
    
    # Check environment variables
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        print("❌ Environment variables not set!")
        return
    
    print(f"✅ Environment variables configured")
    print(f"   SUPABASE_URL: {supabase_url[:30]}...")
    print(f"   SUPABASE_KEY: {supabase_key[:30]}...")
    
    # Process all datasets (this is fast since we're not downloading again)
    print("\n🔄 Quick re-processing of datasets...")
    
    all_dataframes = []
    
    # Process each dataset
    for label, url in CORDIS_JSON_URLS.items():
        print(f"\n📥 Re-downloading and processing {label}...")
        try:
            json_files = download_and_extract_json(url)
            if json_files:
                merged = merge_programme_json(json_files, label)
                if not merged.empty:
                    all_dataframes.append(merged)
                    print(f"✅ {label}: {len(merged)} projects processed")
        except Exception as e:
            print(f"❌ {label}: Failed - {e}")
            continue
    
    if not all_dataframes:
        print("❌ No data processed!")
        return
    
    # Combine datasets
    print(f"\n🔗 Combining {len(all_dataframes)} datasets...")
    combined = pd.concat(all_dataframes, ignore_index=True)
    print(f"📊 Final dataset: {len(combined)} rows, {len(combined.columns)} columns")
    
    # Show distribution
    if 'programmeSource' in combined.columns:
        distribution = combined['programmeSource'].value_counts()
        print("📈 Data distribution:")
        for prog, count in distribution.items():
            print(f"  {prog}: {count} records")
    
    # Normalize
    print("\n🧹 Normalizing data...")
    cleaned = normalise_dataframe(combined)
    
    # Save processed data
    print("💾 Saving processed data...")
    cleaned.to_pickle('cordis_data_processed.pkl')
    print(f"✅ Saved {len(cleaned)} records to cordis_data_processed.pkl")
    
    # Upload to Supabase
    print("\n🚀 Uploading to Supabase...")
    if not cleaned.empty:
        push_to_supabase(cleaned, batch_size=50, test_mode=False, test_rows=None)
        print(f"\n🎉 Upload completed! Processed {len(cleaned)} records.")
    else:
        print("❌ No valid data to upload!")

if __name__ == "__main__":
    save_and_upload_data()
