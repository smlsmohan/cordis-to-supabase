#!/bin/bash

# CORDIS JSON to Supabase Runner Script
# This script sets up the environment variables and runs the CORDIS JSON data extraction

# Set your Supabase credentials
export SUPABASE_URL="https://bfbhbaipgbazdhghrjho.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmYmhiYWlwZ2JhemRoZ2hyamhvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDM4Mjk2MiwiZXhwIjoyMDY5OTU4OTYyfQ.DT9FjhijNE88DGb336z9cfOoiGQA0cRrlRzho_TU2Xs"

# Optional: Set custom table name (defaults to cordis_projects)
export SUPABASE_TABLE="cordis_projects"

echo "🚀 Starting CORDIS JSON data extraction..."
echo "📍 Supabase URL: $SUPABASE_URL"
echo "📊 Target table: ${SUPABASE_TABLE:-cordis_projects}"
echo "📋 Data sources:"
echo "  1. Horizon Europe (PRIMARY): https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip"
echo "  2. Horizon 2020: https://cordis.europa.eu/data/cordis-h2020projects-json.zip"
echo "  3. FP7: https://cordis.europa.eu/data/cordis-fp7projects-json.zip"
echo ""

# Run the Python script with the virtual environment
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python cordis_json_to_supabase.py
