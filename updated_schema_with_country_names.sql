-- Updated CORDIS Projects Schema with Country Names
-- Execute this in your Supabase SQL Editor to add the missing column

-- Add the org_country_names column if it doesn't exist
ALTER TABLE cordis_projects 
ADD COLUMN IF NOT EXISTS org_country_names TEXT;

-- Update the table to ensure all column names match the script expectations
-- (These commands will only execute if columns don't already exist with correct names)

-- Rename columns to match lowercase convention if needed
DO $$
BEGIN
    -- Check and rename startDate to startdate
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'startDate') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "startDate" TO startdate;
    END IF;
    
    -- Check and rename endDate to enddate
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'endDate') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "endDate" TO enddate;
    END IF;
    
    -- Check and rename frameworkProgramme to frameworkprogramme
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'frameworkProgramme') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "frameworkProgramme" TO frameworkprogramme;
    END IF;
    
    -- Check and rename legalBasis to legalbasis
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'legalBasis') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "legalBasis" TO legalbasis;
    END IF;
    
    -- Check and rename masterCall to mastercall
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'masterCall') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "masterCall" TO mastercall;
    END IF;
    
    -- Check and rename subCall to subcall
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'subCall') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "subCall" TO subcall;
    END IF;
    
    -- Check and rename fundingScheme to fundingscheme
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'fundingScheme') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "fundingScheme" TO fundingscheme;
    END IF;
    
    -- Check and rename ecMaxContribution to ecmaxcontribution
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'ecMaxContribution') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "ecMaxContribution" TO ecmaxcontribution;
    END IF;
    
    -- Check and rename totalCost to totalcost
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'totalCost') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "totalCost" TO totalcost;
    END IF;
    
    -- Check and rename euroSciVoc_labels to euroscivoc_labels
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'euroSciVoc_labels') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "euroSciVoc_labels" TO euroscivoc_labels;
    END IF;
    
    -- Check and rename euroSciVoc_codes to euroscivoc_codes
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'euroSciVoc_codes') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "euroSciVoc_codes" TO euroscivoc_codes;
    END IF;
    
    -- Check and rename contentUpdateDate to contentupdatedate
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'contentUpdateDate') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "contentUpdateDate" TO contentupdatedate;
    END IF;
    
    -- Check and rename grantDoi to grantdoi
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'grantDoi') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "grantDoi" TO grantdoi;
    END IF;
    
    -- Check and rename programmeSource to programmesource
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'cordis_projects' AND column_name = 'programmeSource') THEN
        ALTER TABLE cordis_projects RENAME COLUMN "programmeSource" TO programmesource;
    END IF;
    
END $$;

-- Verify the schema
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'cordis_projects' 
ORDER BY ordinal_position;
