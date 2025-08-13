-- Fixed CORDIS Projects Table Schema for Supabase
-- Run this SQL in your Supabase SQL Editor to create the required table

-- Drop the existing table to start fresh
DROP TABLE IF EXISTS cordis_projects CASCADE;

-- Create the table with all required columns (with proper quoting for camelCase)
CREATE TABLE cordis_projects (
    id TEXT PRIMARY KEY,
    acronym TEXT,
    status TEXT,
    title TEXT,
    objective TEXT,
    "startDate" DATE,
    "endDate" DATE,
    "frameworkProgramme" TEXT,
    "legalBasis" TEXT,
    "masterCall" TEXT,
    "subCall" TEXT,
    "fundingScheme" TEXT,
    "ecMaxContribution" NUMERIC,
    "totalCost" NUMERIC,
    org_names TEXT,
    coordinator_names TEXT,
    org_countries TEXT,
    topics_codes TEXT,
    topics_desc TEXT,
    "euroSciVoc_labels" TEXT,
    "euroSciVoc_codes" TEXT,
    project_urls TEXT,
    "contentUpdateDate" DATE,
    rcn BIGINT,
    "grantDoi" TEXT,
    "programmeSource" TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_cordis_projects_programme_source 
ON cordis_projects("programmeSource");

CREATE INDEX IF NOT EXISTS idx_cordis_projects_start_date 
ON cordis_projects("startDate");

CREATE INDEX IF NOT EXISTS idx_cordis_projects_framework 
ON cordis_projects("frameworkProgramme");

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create a trigger to automatically update the updated_at column
DROP TRIGGER IF EXISTS update_cordis_projects_updated_at ON cordis_projects;
CREATE TRIGGER update_cordis_projects_updated_at
    BEFORE UPDATE ON cordis_projects
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Test the table by inserting a sample record
INSERT INTO cordis_projects (
    id, 
    acronym, 
    title, 
    "programmeSource"
) VALUES (
    'TEST_001', 
    'TEST', 
    'Test Project', 
    'test'
) ON CONFLICT (id) DO UPDATE SET 
    acronym = EXCLUDED.acronym,
    title = EXCLUDED.title,
    updated_at = NOW();

-- Verify the table was created correctly
SELECT 
    column_name, 
    data_type, 
    is_nullable 
FROM information_schema.columns 
WHERE table_name = 'cordis_projects' 
ORDER BY ordinal_position;
