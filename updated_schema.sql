-- Updated CORDIS Projects Table Schema
-- This script recreates the table with all the new organization columns

-- Drop the existing table if it exists
DROP TABLE IF EXISTS cordis_projects;

-- Create the updated table with all columns
CREATE TABLE cordis_projects (
    -- Primary identifier
    id TEXT PRIMARY KEY,
    
    -- Basic project information
    acronym TEXT,
    status TEXT,
    title TEXT,
    objective TEXT,
    
    -- Timeline
    startDate DATE,
    endDate DATE,
    contentUpdateDate DATE,
    
    -- Programme information
    frameworkProgramme TEXT,
    legalBasis TEXT,
    masterCall TEXT,
    subCall TEXT,
    fundingScheme TEXT,
    programmeSource TEXT,
    
    -- Financial information
    ecMaxContribution NUMERIC,
    totalCost NUMERIC,
    
    -- Organization information (updated with new columns)
    org_names TEXT,
    roles TEXT,                    -- All roles (coordinator, participant, etc.)
    org_countries TEXT,
    organization_urls TEXT,        -- Organization website URLs
    contact_forms TEXT,           -- Contact form URLs
    cities TEXT,                  -- Organization cities
    
    -- Topics and classification
    topics_codes TEXT,
    topics_desc TEXT,
    euroSciVoc_labels TEXT,
    euroSciVoc_codes TEXT,
    
    -- Additional information
    project_urls TEXT,
    rcn TEXT,
    grantDoi TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX idx_cordis_projects_acronym ON cordis_projects(acronym);
CREATE INDEX idx_cordis_projects_status ON cordis_projects(status);
CREATE INDEX idx_cordis_projects_framework ON cordis_projects(frameworkProgramme);
CREATE INDEX idx_cordis_projects_programme_source ON cordis_projects(programmeSource);
CREATE INDEX idx_cordis_projects_start_date ON cordis_projects(startDate);
CREATE INDEX idx_cordis_projects_org_countries ON cordis_projects(org_countries);
CREATE INDEX idx_cordis_projects_roles ON cordis_projects(roles);

-- Enable Row Level Security (optional, for multi-tenant scenarios)
ALTER TABLE cordis_projects ENABLE ROW LEVEL SECURITY;

-- Create a policy to allow all operations (adjust as needed for your security requirements)
CREATE POLICY "Enable all operations for all users" ON cordis_projects
    FOR ALL USING (true);

-- Add a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_cordis_projects_updated_at 
    BEFORE UPDATE ON cordis_projects 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add helpful comments
COMMENT ON TABLE cordis_projects IS 'CORDIS European research projects data from Horizon Europe, H2020, and FP7 programmes';
COMMENT ON COLUMN cordis_projects.id IS 'Unique project identifier';
COMMENT ON COLUMN cordis_projects.roles IS 'All organization roles separated by | (coordinator, participant, etc.)';
COMMENT ON COLUMN cordis_projects.org_names IS 'All organization names separated by |';
COMMENT ON COLUMN cordis_projects.organization_urls IS 'Organization website URLs separated by |';
COMMENT ON COLUMN cordis_projects.contact_forms IS 'Organization contact form URLs separated by |';
COMMENT ON COLUMN cordis_projects.cities IS 'Organization cities separated by |';
COMMENT ON COLUMN cordis_projects.programmeSource IS 'Source programme: HORIZONprojects, h2020projects, or fp7projects';

-- Show table structure
\d cordis_projects;
