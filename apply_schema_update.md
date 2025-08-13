# Database Schema Update Instructions

## Step 1: Apply Schema Update to Supabase

1. **Login to your Supabase Dashboard**
   - Go to https://supabase.com/dashboard
   - Navigate to your project

2. **Open SQL Editor**
   - Click on "SQL Editor" in the left sidebar
   - Click "New query"

3. **Execute the Schema Update**
   - Copy the contents of `simple_schema.sql`
   - Paste into the SQL Editor
   - Click "Run" to execute

⚠️ **WARNING**: This will drop and recreate the `cordis_projects` table, removing all existing data.

## Step 2: Verify Schema Update

After running the schema update, verify the table structure:

```sql
-- Check table structure
\d cordis_projects;

-- Or use this query:
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_name = 'cordis_projects'
ORDER BY ordinal_position;
```

## Step 3: Run Production Pipeline

After schema update is complete, run the full pipeline:

```bash
cd /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase
unset TEST_MODE
unset TEST_ROWS
python cordis_json_to_supabase.py
```

## Expected Results:
- **Total Records**: ~79,069 unique projects
- **New Columns Populated**:
  - `roles` - All organization roles (coordinator, participant, etc.)
  - `organization_urls` - Organization website URLs
  - `contact_forms` - Contact form URLs  
  - `cities` - Organization cities
- **Processing Time**: ~30-45 minutes for full dataset

## Monitoring:
The pipeline will show progress updates including:
- Download progress for each dataset
- Processing statistics for each JSON file
- Upload batch progress with success rates
- Final summary with total records processed
