#!/bin/bash

# Test script for CORDIS JSON ETL Pipeline
# This script runs the pipeline in test mode with only 1000 rows

echo "🧪 Running CORDIS JSON ETL Pipeline in TEST MODE (1000 rows)"
echo "============================================================"

# Set your Supabase credentials (same as main script)
export SUPABASE_URL="https://bfbhbaipgbazdhghrjho.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"
export SUPABASE_TABLE="cordis_projects"

# Set test mode environment variables
export TEST_MODE=true
export TEST_ROWS=1000

echo "📍 Supabase URL: $SUPABASE_URL"
echo "📊 Target table: $SUPABASE_TABLE"
echo "🧪 Test mode: $TEST_MODE (max $TEST_ROWS rows)"

# Check if virtual environment exists and activate it
if [ -d ".venv" ]; then
    echo "🐍 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  No virtual environment found at .venv"
fi

# Install requirements if needed
if [ -f "requirements.txt" ]; then
    echo "📦 Installing Python dependencies..."
    pip install -r requirements.txt
fi

# Run the test
echo ""
echo "🚀 Starting test run..."
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python cordis_json_to_supabase.py

echo "✅ Test run completed!"
