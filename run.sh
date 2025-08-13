#!/bin/bash

# CORDIS to Supabase Runner Script
# This script sets up the environment variables and runs the CORDIS data extraction

# Set your Supabase credentials
export SUPABASE_URL="https://bfbhbaipgbazdhghrjho.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

# Optional: Set custom table name (defaults to cordis_projects)
# export SUPABASE_TABLE="cordis_projects"

# Check if service role key is set
if [ -z "$SUPABASE_SERVICE_ROLE_KEY" ]; then
    echo "❌ Error: SUPABASE_SERVICE_ROLE_KEY environment variable is not set!"
    echo "Please set your Supabase service role key:"
    echo "export SUPABASE_SERVICE_ROLE_KEY=\"your-service-role-key-here\""
    exit 1
fi

echo "🚀 Starting CORDIS data extraction..."
echo "📍 Supabase URL: $SUPABASE_URL"
echo "📊 Target table: ${SUPABASE_TABLE:-cordis_projects}"

# Run the Python script with the virtual environment
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python cordis_to_supabase.py
