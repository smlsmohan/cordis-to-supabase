-- Minimal CORDIS Projects Table Schema for Testing
-- Run this SQL in your Supabase SQL Editor

-- Drop the existing table if it exists
DROP TABLE IF EXISTS cordis_projects;

-- Create a minimal table for testing
CREATE TABLE cordis_projects (
    id TEXT PRIMARY KEY,
    acronym TEXT,
    status TEXT,
    title TEXT,
    objective TEXT,
    "startDate" DATE,
    "endDate" DATE,
    "frameworkProgramme" TEXT,
    "programmeSource" TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create an index on the programme source for faster filtering
CREATE INDEX IF NOT EXISTS idx_cordis_projects_programme_source 
ON cordis_projects("programmeSource");

-- Test the table
INSERT INTO cordis_projects (id, acronym, title, "programmeSource") 
VALUES ('TEST_001', 'TEST', 'Test Project', 'test')
ON CONFLICT (id) DO UPDATE SET 
    acronym = EXCLUDED.acronym,
    title = EXCLUDED.title,
    updated_at = NOW();

SELECT * FROM cordis_projects WHERE id = 'TEST_001';
