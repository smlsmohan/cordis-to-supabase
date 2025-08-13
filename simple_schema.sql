-- Simple table recreation script for Supabase
-- Execute this in your Supabase SQL Editor

DROP TABLE IF EXISTS cordis_projects;

CREATE TABLE cordis_projects (
    id TEXT PRIMARY KEY,
    acronym TEXT,
    status TEXT,
    title TEXT,
    objective TEXT,
    startDate DATE,
    endDate DATE,
    frameworkProgramme TEXT,
    legalBasis TEXT,
    masterCall TEXT,
    subCall TEXT,
    fundingScheme TEXT,
    ecMaxContribution NUMERIC,
    totalCost NUMERIC,
    
    -- Updated organization columns
    org_names TEXT,
    roles TEXT,
    org_countries TEXT,
    organization_urls TEXT,
    contact_forms TEXT,
    cities TEXT,
    
    -- Topic and classification columns
    topics_codes TEXT,
    topics_desc TEXT,
    euroSciVoc_labels TEXT,
    euroSciVoc_codes TEXT,
    
    project_urls TEXT,
    contentUpdateDate DATE,
    rcn TEXT,
    grantDoi TEXT,
    programmeSource TEXT
);
