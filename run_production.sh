#!/bin/bash
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/run_production.sh
# Production run script for CORDIS ETL pipeline

echo "🚀 Starting CORDIS ETL Pipeline - PRODUCTION MODE"
echo "================================================="

# Navigate to project directory
cd /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase

# Ensure we're in production mode (unset test variables)
unset TEST_MODE
unset TEST_ROWS

echo "✅ Environment configured for production"
echo "   - TEST_MODE: unset (production mode)"
echo "   - TEST_ROWS: unset (all records)"
echo ""

# Check for required environment variables
if [[ -z "$SUPABASE_URL" || -z "$SUPABASE_KEY" ]]; then
    echo "❌ ERROR: Required Supabase environment variables not set!"
    echo "   Please set SUPABASE_URL and SUPABASE_KEY"
    echo ""
    echo "   export SUPABASE_URL='your-supabase-url'"
    echo "   export SUPABASE_KEY='your-service-role-key'"
    exit 1
fi

echo "✅ Supabase configuration verified"
echo "   - URL: ${SUPABASE_URL:0:30}..."
echo "   - Key: ${SUPABASE_KEY:0:20}..."
echo ""

# Verify Python environment
python_version=$(python --version 2>&1)
echo "🐍 Python environment: $python_version"

# Check for required packages
echo "📦 Checking required packages..."
python -c "import pandas, requests, supabase" 2>/dev/null && echo "✅ All required packages available" || {
    echo "❌ Missing required packages. Installing..."
    pip install pandas requests supabase
}

echo ""
echo "🎯 Starting full pipeline execution..."
echo "   Expected records: ~79,069 unique projects"
echo "   Estimated time: 30-45 minutes"
echo "   Press Ctrl+C to cancel"
echo ""

# Run the pipeline
python cordis_json_to_supabase.py

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 PRODUCTION PIPELINE COMPLETED SUCCESSFULLY!"
    echo "================================================="
    echo "✅ All CORDIS data has been processed and uploaded to Supabase"
    echo "✅ New columns (roles, organization_urls, contact_forms, cities) are populated"
    echo ""
    echo "Next steps:"
    echo "1. Verify data in Supabase dashboard"
    echo "2. Test queries with new nested fields"
    echo "3. Set up any additional indexes if needed"
else
    echo ""
    echo "❌ PRODUCTION PIPELINE FAILED!"
    echo "================================"
    echo "Please check the error messages above and retry"
    exit 1
fi
