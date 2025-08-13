#!/usr/bin/env python3
"""
Complete Production Upload for CORDIS ETL Pipeline
Handles environment variable verification and final upload to Supabase
"""

import os
import sys
import pandas as pd
from datetime import datetime

def check_environment_variables():
    """Check if required Supabase environment variables are set"""
    print("\n🔍 Checking Environment Variables")
    print("=" * 50)
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url:
        print("❌ SUPABASE_URL not found")
        return False, None, None
    else:
        print(f"✅ SUPABASE_URL: {supabase_url[:30]}...")
    
    if not supabase_key:
        print("❌ SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) not found")
        return False, None, None
    else:
        print(f"✅ SUPABASE_KEY: {supabase_key[:30]}...")
    
    return True, supabase_url, supabase_key

def load_processed_data():
    """Load the processed CORDIS data"""
    data_file = 'cordis_data_processed.pkl'
    
    if not os.path.exists(data_file):
        print(f"❌ Processed data file not found: {data_file}")
        print("Please run the main ETL pipeline first:")
        print("python cordis_json_to_supabase.py")
        return None
    
    print(f"\n📂 Loading processed data from {data_file}")
    df = pd.read_pickle(data_file)
    print(f"✅ Loaded {len(df):,} records")
    
    return df

def upload_to_supabase(df, url, key):
    """Upload the processed data to Supabase"""
    try:
        from supabase import create_client, Client
        
        print(f"\n🚀 Starting upload to Supabase")
        print("=" * 50)
        
        supabase: Client = create_client(url, key)
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Upload in batches of 1000 (Supabase limit)
        batch_size = 1000
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        print(f"📊 Uploading {len(records):,} records in {total_batches} batches")
        
        successful_uploads = 0
        failed_uploads = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                response = supabase.table("cordis_projects").insert(batch).execute()
                successful_uploads += len(batch)
                print(f"✅ Batch {batch_num}/{total_batches}: {len(batch)} records uploaded")
            except Exception as e:
                failed_uploads += len(batch)
                print(f"❌ Batch {batch_num}/{total_batches} failed: {str(e)}")
                
                # Try uploading records individually for this batch
                print(f"   🔄 Trying individual record upload for batch {batch_num}")
                for j, record in enumerate(batch):
                    try:
                        supabase.table("cordis_projects").insert(record).execute()
                        successful_uploads += 1
                        failed_uploads -= 1
                    except Exception as record_error:
                        print(f"   ❌ Record {j+1} in batch {batch_num} failed: {str(record_error)}")
        
        print(f"\n📈 Upload Summary:")
        print(f"   ✅ Successful: {successful_uploads:,} records")
        print(f"   ❌ Failed: {failed_uploads:,} records")
        print(f"   📊 Success Rate: {(successful_uploads / len(records) * 100):.1f}%")
        
        return successful_uploads, failed_uploads
        
    except ImportError:
        print("❌ Supabase client not installed. Installing...")
        os.system("pip install supabase")
        return upload_to_supabase(df, url, key)
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        return 0, len(df)

def run_final_quality_check():
    """Run the final data quality check on uploaded data"""
    print(f"\n🔍 Running final data quality check...")
    try:
        os.system("python final_data_quality_check.py")
    except Exception as e:
        print(f"⚠️  Quality check failed: {str(e)}")

def main():
    """Main execution function"""
    print("🚀 CORDIS Production Upload")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Check environment variables
    env_ok, url, key = check_environment_variables()
    if not env_ok:
        print("\n❌ Environment variables not configured!")
        print("\nPlease set the required environment variables:")
        print("export SUPABASE_URL='https://your-project-id.supabase.co'")
        print("export SUPABASE_SERVICE_ROLE_KEY='your-service-role-key'")
        print("\nThen run this script again.")
        sys.exit(1)
    
    # Step 2: Load processed data
    df = load_processed_data()
    if df is None:
        sys.exit(1)
    
    # Step 3: Confirm upload
    print(f"\n⚠️  Ready to upload {len(df):,} records to Supabase")
    response = input("Proceed with upload? (y/N): ").strip().lower()
    if response != 'y':
        print("Upload cancelled.")
        sys.exit(0)
    
    # Step 4: Upload to Supabase
    successful, failed = upload_to_supabase(df, url, key)
    
    # Step 5: Run quality check if upload was successful
    if successful > 0:
        run_final_quality_check()
    
    print(f"\n🎉 Production upload completed!")
    print(f"   ✅ {successful:,} records uploaded successfully")
    if failed > 0:
        print(f"   ❌ {failed:,} records failed")

if __name__ == "__main__":
    main()
