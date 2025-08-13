#!/usr/bin/env python3

import os
import sys

# Set test mode before importing the main module
os.environ['TEST_MODE'] = 'true'
os.environ['TEST_ROWS'] = '1000'

# Import and run the main function
from cordis_json_to_supabase import main

if __name__ == "__main__":
    print("🧪 CORDIS JSON ETL Test Mode (1000 rows)")
    print("========================================")
    
    # Check environment variables
    if not os.environ.get("SUPABASE_URL"):
        print("❌ SUPABASE_URL not set. Please set environment variables:")
        print("   export SUPABASE_URL='your_supabase_url'")
        print("   export SUPABASE_SERVICE_ROLE_KEY='your_service_key'")
        sys.exit(1)
    
    main()
