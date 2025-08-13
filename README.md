# CORDIS to Supabase ETL Pipeline

This project downloads CORDIS (Community Research and Development Information Service) project data from the European Commission and uploads it to a Supabase database.

## 🚀 Quick Start

### 1. Prerequisites Setup

Your Supabase URL is already configured: `https://bfbhbaipgbazdhghrjho.supabase.co`

**You need to:**
1. Get your Supabase service role key from your Supabase dashboard
2. Create the database table using the provided schema

### 2. Database Setup

Run the SQL in `schema.sql` in your Supabase SQL Editor to create the required table:

```sql
-- Copy and paste the contents of schema.sql into your Supabase SQL Editor
```

### 3. Environment Variables

Set your Supabase service role key:

```bash
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key-here"
```

### 4. Test Connection

Test your setup before running the full extraction:

```bash
./test_connection.py
```

### 5. Run the ETL Pipeline

```bash
./run.sh
```

## 📁 Files Overview

- `cordis_to_supabase.py` - Main ETL script
- `requirements.txt` - Python dependencies (already installed)
- `schema.sql` - Database schema for Supabase
- `run.sh` - Convenience script to run the ETL with proper environment
- `test_connection.py` - Test script to verify Supabase connectivity

## 🔧 Manual Setup (Alternative)

If you prefer to set environment variables manually:

```bash
export SUPABASE_URL="https://bfbhbaipgbazdhghrjho.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
export SUPABASE_TABLE="cordis_projects"  # Optional, defaults to cordis_projects

# Run the script
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python cordis_to_supabase.py
```

## 📊 Data Sources

The script downloads and processes data from:
- Horizon Europe projects
- Horizon 2020 projects  
- FP7 projects

## 🔍 What's Next?

1. **Get your Supabase service role key** from your Supabase project dashboard
2. **Run the schema.sql** in your Supabase SQL Editor
3. **Test the connection** using `test_connection.py`
4. **Run the full pipeline** using `run.sh`

Let me know if you need help with any of these steps!
