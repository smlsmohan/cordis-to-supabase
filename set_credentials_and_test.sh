#!/bin/bash
# filepath: /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/set_credentials_and_test.sh
# Script to set proper credentials and run the test

echo "🔐 CORDIS Pipeline - Set Credentials and Test"
echo "=============================================="
echo ""
echo "Please enter your Supabase credentials:"
echo ""

# Get SUPABASE_URL
echo -n "Enter your Supabase URL (e.g., https://abcdefgh.supabase.co): "
read SUPABASE_URL

# Get SUPABASE_SERVICE_ROLE_KEY
echo -n "Enter your Supabase service role key: "
read -s SUPABASE_SERVICE_ROLE_KEY
echo ""

# Validate inputs
if [[ -z "$SUPABASE_URL" || -z "$SUPABASE_SERVICE_ROLE_KEY" ]]; then
    echo "❌ ERROR: Both URL and key are required!"
    exit 1
fi

# Export the variables
export SUPABASE_URL="$SUPABASE_URL"
export SUPABASE_SERVICE_ROLE_KEY="$SUPABASE_SERVICE_ROLE_KEY"

echo ""
echo "✅ Credentials set successfully!"
echo "   URL: ${SUPABASE_URL:0:30}..."
echo "   Key: ${SUPABASE_SERVICE_ROLE_KEY:0:20}..."
echo ""

# Set test mode
export TEST_MODE=true
export TEST_ROWS=50

echo "🧪 Running test with 50 records..."
echo ""

# Run the test
python cordis_json_to_supabase.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 TEST SUCCESSFUL!"
    echo "The pipeline is ready for production."
    echo ""
    echo "To run the full production pipeline:"
    echo "1. Unset test variables: unset TEST_MODE TEST_ROWS"
    echo "2. Run: python cordis_json_to_supabase.py"
else
    echo ""
    echo "❌ TEST FAILED!"
    echo "Please check the error messages above."
fi
