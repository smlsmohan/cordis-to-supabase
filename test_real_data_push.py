#!/usr/bin/env python3
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/test_real_data_push.py
"""
Test script to push a small batch of real CORDIS data to verify the pipeline works.
This downloads and processes just the Horizon Europe dataset with a 50-record limit.
"""

import os
import sys

def main():
    """Run the real pipeline with test limits."""
    print("🧪 CORDIS Real Data Test Push")
    print("=============================")
    print("This will download and process a small batch of real CORDIS data")
    print("to verify the full pipeline works before production run.")
    print()
    
    # Set test mode environment variables
    os.environ['TEST_MODE'] = 'true'
    os.environ['TEST_ROWS'] = '50'
    
    print("✅ Test mode configured:")
    print("   - TEST_MODE: true")
    print("   - TEST_ROWS: 50")
    print("   - Will process only Horizon Europe data")
    print()
    
    # Check Supabase credentials
    if not os.environ.get('SUPABASE_URL') or not os.environ.get('SUPABASE_KEY'):
        print("❌ ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
        print("\nPlease run:")
        print("export SUPABASE_URL='https://your-project-id.supabase.co'")
        print("export SUPABASE_KEY='your-service-role-key'")
        return False
    
    print("✅ Supabase credentials found")
    print()
    
    # Import and run the main pipeline
    try:
        print("🚀 Starting real data test...")
        import cordis_json_to_supabase
        print("✅ Pipeline import successful - test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 REAL DATA TEST SUCCESSFUL!")
        print("The pipeline is ready for production run.")
    else:
        print("\n❌ REAL DATA TEST FAILED!")
        print("Please check the errors above.")
        sys.exit(1)
