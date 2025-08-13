import os
import io
import json
import zipfile
import requests
import pandas as pd
import datetime
import numpy as np
from typing import Dict, List, Any, Optional

"""
cordis_json_to_supabase.py
==========================

This script downloads CORDIS project data from JSON sources (preferred over CSV),
processes and merges the data intelligently to minimize null values, and uploads
to Supabase. Horizon Europe data is treated as primary source with all records.

Data Sources (in priority order):
1. Horizon Europe (PRIMARY) - https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip
2. Horizon 2020 - https://cordis.europa.eu/data/cordis-h2020projects-json.zip  
3. FP7 - https://cordis.europa.eu/data/cordis-fp7projects-json.zip

The script intelligently merges data from multiple JSON files within each dataset
to populate columns with minimal null values.
"""

# URLs for the official CORDIS JSON archives (prioritized)
CORDIS_JSON_URLS = {
    "HORIZONprojects": "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip",
    "h2020projects": "https://cordis.europa.eu/data/cordis-h2020projects-json.zip", 
    "fp7projects": "https://cordis.europa.eu/data/cordis-fp7projects-json.zip",
}

# Columns expected in the Supabase table
ALLOWED_COLUMNS = [
    "id", "acronym", "status", "title", "objective",
    "startdate", "enddate",
    "frameworkprogramme", "legalbasis", "mastercall", "subcall", "fundingscheme",
    "ecmaxcontribution", "totalcost",
    "org_names", "roles", "org_countries", "org_country_names", "organization_urls", "contact_forms", "cities",
    "topics_codes", "topics_desc",
    "euroscivoc_labels", "euroscivoc_codes",
    "project_urls", "contentupdatedate", "rcn", "grantdoi", "programmesource",
]

def download_and_extract_json(url: str) -> Dict[str, Any]:
    """Download a zip archive and return a dictionary of filename → parsed JSON data."""
    print(f"📥 Downloading {url}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    json_files = {}
    
    for name in z.namelist():
        if name.endswith(".json"):
            print(f"  📄 Extracting {name}...")
            try:
                json_content = z.read(name).decode('utf-8')
                json_data = json.loads(json_content)
                json_files[name] = json_data
                print(f"    ✅ Loaded {len(json_data) if isinstance(json_data, list) else 1} records")
            except Exception as e:
                print(f"    ❌ Failed to parse {name}: {e}")
                continue
    
    return json_files

def extract_project_data(json_data: Any, source_name: str) -> pd.DataFrame:
    """Extract project data from JSON and convert to DataFrame."""
    if isinstance(json_data, list):
        projects = json_data
    elif isinstance(json_data, dict):
        # Handle different JSON structures
        if 'projects' in json_data:
            projects = json_data['projects']
        elif 'project' in json_data:
            projects = json_data['project'] 
        else:
            # Assume the dict itself contains project data
            projects = [json_data]
    else:
        print(f"    ⚠️  Unexpected JSON structure in {source_name}")
        return pd.DataFrame()
    
    if not projects:
        return pd.DataFrame()
    
    # Handle different types of files
    if source_name == 'project.json':
        # Main project file - flatten all data with proper field mapping
        flattened_projects = []
        for project in projects:
            flat_project = flatten_json(project)
            
            # Ensure critical fields are properly mapped
            if 'id' not in flat_project and 'projectid' in flat_project:
                flat_project['id'] = flat_project['projectid']
            
            # Map common alternative field names for better data quality
            field_alternatives = {
                'frameworkprogramme': ['programme', 'framework'],
                'legalbasis': ['legal_basis', 'legalbasis'],
                'ecmaxcontribution': ['maxcontribution', 'ec_max_contribution', 'ecmaxcontribution'],
                'totalcost': ['total_cost', 'budget', 'totalbudget'],
                'startdate': ['start_date', 'startdate'],
                'enddate': ['end_date', 'enddate'],
                'contentupdatedate': ['content_update_date', 'contentupdatedate'],
                'grantdoi': ['grant_doi', 'doi'],
            }
            
            for target_field, alternatives in field_alternatives.items():
                if target_field not in flat_project or not flat_project[target_field]:
                    for alt in alternatives:
                        if alt in flat_project and flat_project[alt]:
                            flat_project[target_field] = flat_project[alt]
                            break
            
            flattened_projects.append(flat_project)
        
        df = pd.DataFrame(flattened_projects)
        
        # Ensure 'id' column exists and convert to proper integer string
        if 'id' in df.columns:
            df['id'] = df['id'].apply(convert_id_to_integer_string)
        elif 'projectID' in df.columns:
            df['id'] = df['projectID'].apply(convert_id_to_integer_string)
        
        print(f"    📊 Extracted {len(df)} projects with {len(df.columns)} columns")
        return df
        
    elif source_name == 'organization.json':
        return process_organization_data(projects)
    elif source_name == 'topics.json':
        return process_topics_data(projects)
    elif source_name == 'euroSciVoc.json':
        return process_eurovoc_data(projects)
    elif source_name in ['webLink.json', 'webLinks.json']:
        return process_weblink_data(projects)
    elif source_name == 'legalBasis.json':
        return process_legalbasis_data(projects)
    else:
        # Other files - try to extract as tabular data
        flattened_projects = []
        for project in projects:
            flat_project = flatten_json(project)
            flattened_projects.append(flat_project)
        
        df = pd.DataFrame(flattened_projects)
        
        # Ensure 'id' column exists
        if 'id' in df.columns:
            df['id'] = df['id'].apply(convert_id_to_integer_string)
        elif 'projectID' in df.columns:
            df['id'] = df['projectID'].apply(convert_id_to_integer_string)
        
        print(f"    📊 Extracted {len(df)} projects with {len(df.columns)} columns")
        return df

def get_country_name(country_code: str) -> str:
    """Convert country code to country name."""
    country_mapping = {
        # EU Countries
        'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'HR': 'Croatia',
        'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DK': 'Denmark', 'EE': 'Estonia',
        'FI': 'Finland', 'FR': 'France', 'DE': 'Germany', 'GR': 'Greece',
        'HU': 'Hungary', 'IE': 'Ireland', 'IT': 'Italy', 'LV': 'Latvia',
        'LT': 'Lithuania', 'LU': 'Luxembourg', 'MT': 'Malta', 'NL': 'Netherlands',
        'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'SK': 'Slovakia',
        'SI': 'Slovenia', 'ES': 'Spain', 'SE': 'Sweden',
        
        # Associated Countries
        'NO': 'Norway', 'CH': 'Switzerland', 'UK': 'United Kingdom', 'GB': 'United Kingdom',
        'IS': 'Iceland', 'LI': 'Liechtenstein', 'TR': 'Turkey', 'UA': 'Ukraine',
        'MD': 'Moldova', 'ME': 'Montenegro', 'MK': 'North Macedonia', 'AL': 'Albania',
        'RS': 'Serbia', 'BA': 'Bosnia and Herzegovina', 'XK': 'Kosovo',
        'GE': 'Georgia', 'AM': 'Armenia', 'TN': 'Tunisia', 'MA': 'Morocco',
        'IL': 'Israel', 'FO': 'Faroe Islands',
        
        # Other Common Countries
        'US': 'United States', 'CA': 'Canada', 'AU': 'Australia', 'JP': 'Japan',
        'KR': 'South Korea', 'CN': 'China', 'IN': 'India', 'BR': 'Brazil',
        'RU': 'Russia', 'ZA': 'South Africa', 'NZ': 'New Zealand', 'SG': 'Singapore',
        'HK': 'Hong Kong', 'TW': 'Taiwan', 'TH': 'Thailand', 'MY': 'Malaysia',
        'MX': 'Mexico', 'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia',
        'PE': 'Peru', 'EG': 'Egypt', 'SA': 'Saudi Arabia', 'AE': 'United Arab Emirates',
        'QA': 'Qatar', 'KW': 'Kuwait', 'JO': 'Jordan', 'LB': 'Lebanon',
        'PK': 'Pakistan', 'BD': 'Bangladesh', 'LK': 'Sri Lanka', 'NP': 'Nepal',
        'MM': 'Myanmar', 'VN': 'Vietnam', 'ID': 'Indonesia', 'PH': 'Philippines',
        'KE': 'Kenya', 'NG': 'Nigeria', 'GH': 'Ghana', 'ET': 'Ethiopia',
        'TZ': 'Tanzania', 'UG': 'Uganda', 'RW': 'Rwanda', 'SN': 'Senegal',
        'CI': "Côte d'Ivoire", 'BF': 'Burkina Faso', 'ML': 'Mali', 'NE': 'Niger',
        'TD': 'Chad', 'CM': 'Cameroon', 'CF': 'Central African Republic',
        'CD': 'Democratic Republic of Congo', 'CG': 'Republic of Congo',
        'GA': 'Gabon', 'GQ': 'Equatorial Guinea', 'ST': 'São Tomé and Príncipe',
        'AO': 'Angola', 'NA': 'Namibia', 'BW': 'Botswana', 'ZW': 'Zimbabwe',
        'ZM': 'Zambia', 'MW': 'Malawi', 'MZ': 'Mozambique', 'MG': 'Madagascar',
        'MU': 'Mauritius', 'SC': 'Seychelles', 'KM': 'Comoros', 'DJ': 'Djibouti',
        'SO': 'Somalia', 'ER': 'Eritrea', 'SS': 'South Sudan', 'SD': 'Sudan',
        'LY': 'Libya', 'DZ': 'Algeria', 'MR': 'Mauritania', 'GM': 'Gambia',
        'GW': 'Guinea-Bissau', 'GN': 'Guinea', 'SL': 'Sierra Leone', 'LR': 'Liberia',
        'BJ': 'Benin', 'TG': 'Togo', 'GH': 'Ghana'
    }
    
    if not country_code or pd.isna(country_code):
        return ''
    
    code = str(country_code).strip().upper()
    return country_mapping.get(code, code)  # Return code if name not found

def process_organization_data(org_data: List[Dict]) -> pd.DataFrame:
    """Process organization data and aggregate by project."""
    df = pd.DataFrame(org_data)
    if df.empty or 'projectID' not in df.columns:
        print(f"    📊 Extracted 0 projects with 0 columns")
        return pd.DataFrame()
    
    # Group by project and aggregate organization info
    result = []
    for project_id, group in df.groupby('projectID'):
        org_names = []
        roles = []
        countries = []
        country_names = []
        org_urls = []
        contact_forms = []
        cities = []
        
        for _, org in group.iterrows():
            if pd.notna(org.get('name')):
                org_names.append(str(org['name']))
            if pd.notna(org.get('role')):
                roles.append(str(org['role']))
            if pd.notna(org.get('country')):
                country_code = str(org['country'])
                countries.append(country_code)
                # Add country name
                country_name = get_country_name(country_code)
                if country_name:
                    country_names.append(country_name)
            if pd.notna(org.get('organizationURL')):
                org_urls.append(str(org['organizationURL']))
            if pd.notna(org.get('contactForm')):
                contact_forms.append(str(org['contactForm']))
            if pd.notna(org.get('city')):
                cities.append(str(org['city']))
        
        result.append({
            'id': convert_id_to_integer_string(project_id),
            'org_names': ' | '.join(unique_values(org_names)) if org_names else '',
            'roles': ' | '.join(unique_values(roles)) if roles else '',
            'org_countries': ' | '.join(unique_values(countries)) if countries else '',
            'org_country_names': ' | '.join(unique_values(country_names)) if country_names else '',
            'organization_urls': ' | '.join(unique_values(org_urls)) if org_urls else '',
            'contact_forms': ' | '.join(unique_values(contact_forms)) if contact_forms else '',
            'cities': ' | '.join(unique_values(cities)) if cities else ''
        })
    
    result_df = pd.DataFrame(result)
    print(f"    📊 Extracted {len(result_df)} projects with {len(result_df.columns)} columns")
    return result_df

def process_topics_data(topics_data: List[Dict]) -> pd.DataFrame:
    """Process topics data and aggregate by project."""
    df = pd.DataFrame(topics_data)
    if df.empty or 'projectID' not in df.columns:
        print(f"    📊 Extracted 0 projects with 0 columns")
        return pd.DataFrame()
    
    # Group by project and aggregate topic info
    result = []
    for project_id, group in df.groupby('projectID'):
        codes = []
        descriptions = []
        
        for _, topic in group.iterrows():
            if pd.notna(topic.get('topic')):
                codes.append(str(topic['topic']))
            if pd.notna(topic.get('title')):
                descriptions.append(str(topic['title']))
        
        result.append({
            'id': convert_id_to_integer_string(project_id),
            'topics_codes': ' | '.join(unique_values(codes)) if codes else '',
            'topics_desc': ' | '.join(unique_values(descriptions)) if descriptions else ''
        })
    
    result_df = pd.DataFrame(result)
    print(f"    📊 Extracted {len(result_df)} projects with {len(result_df.columns)} columns")
    return result_df

def process_eurovoc_data(eurovoc_data: List[Dict]) -> pd.DataFrame:
    """Process EuroSciVoc data and aggregate by project."""
    df = pd.DataFrame(eurovoc_data)
    if df.empty or 'projectID' not in df.columns:
        print(f"    📊 Extracted 0 projects with 0 columns")
        return pd.DataFrame()
    
    # Group by project and aggregate EuroSciVoc info
    result = []
    for project_id, group in df.groupby('projectID'):
        codes = []
        labels = []
        
        for _, item in group.iterrows():
            # Try multiple field name variations for codes
            if pd.notna(item.get('euroSciVocCode')):
                codes.append(str(item['euroSciVocCode']))
            elif pd.notna(item.get('code')):
                codes.append(str(item['code']))
            elif pd.notna(item.get('euroSciVoc')):
                codes.append(str(item['euroSciVoc']))
                
            # Try multiple field name variations for labels/titles
            if pd.notna(item.get('euroSciVocTitle')):
                labels.append(str(item['euroSciVocTitle']))
            elif pd.notna(item.get('title')):
                labels.append(str(item['title']))
            elif pd.notna(item.get('label')):
                labels.append(str(item['label']))
            elif pd.notna(item.get('euroSciVocLabel')):
                labels.append(str(item['euroSciVocLabel']))
        
        result.append({
            'id': convert_id_to_integer_string(project_id),
            'euroscivoc_codes': ' | '.join(unique_values(codes)) if codes else '',
            'euroscivoc_labels': ' | '.join(unique_values(labels)) if labels else ''
        })
    
    result_df = pd.DataFrame(result)
    print(f"    📊 Extracted {len(result_df)} projects with {len(result_df.columns)} columns")
    return result_df

def process_legalbasis_data(legalbasis_data: List[Dict]) -> pd.DataFrame:
    """Process legal basis data and aggregate by project."""
    df = pd.DataFrame(legalbasis_data)
    if df.empty or 'projectID' not in df.columns:
        print(f"    📊 Extracted 0 projects with 0 columns")
        return pd.DataFrame()
    
    # Group by project and get legal basis info
    result = []
    for project_id, group in df.groupby('projectID'):
        legal_basis = group.iloc[0].get('legalBasis', '')
        
        result.append({
            'id': convert_id_to_integer_string(project_id),
            'legalbasis': str(legal_basis) if pd.notna(legal_basis) else ''
        })
    
    result_df = pd.DataFrame(result)
    print(f"    📊 Extracted {len(result_df)} projects with {len(result_df.columns)} columns")
    return result_df

def process_weblink_data(weblink_data: List[Dict]) -> pd.DataFrame:
    """Process weblink data and aggregate by project."""
    df = pd.DataFrame(weblink_data)
    if df.empty or 'projectID' not in df.columns:
        print(f"    📊 Extracted 0 projects with 0 columns")
        return pd.DataFrame()
    
    # Group by project and aggregate URL info
    result = []
    for project_id, group in df.groupby('projectID'):
        urls = []
        
        for _, link in group.iterrows():
            if pd.notna(link.get('physUrl')):
                urls.append(str(link['physUrl']))
            elif pd.notna(link.get('url')):
                urls.append(str(link['url']))
        
        result.append({
            'id': convert_id_to_integer_string(project_id),
            'project_urls': ' | '.join(unique_values(urls)) if urls else ''
        })
    
    result_df = pd.DataFrame(result)
    print(f"    📊 Extracted {len(result_df)} projects with {len(result_df.columns)} columns")
    return result_df

def flatten_json(json_obj: Any, prefix: str = '') -> Dict[str, Any]:
    """Flatten nested JSON objects with improved field mapping for CORDIS data."""
    flattened = {}
    
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            # Map CORDIS field names to our database column names
            mapped_key = map_cordis_field_name(key, prefix)
            
            if isinstance(value, (dict, list)) and key in ['organizations', 'topics', 'euroSciVoc', 'webLinks']:
                # Handle special nested structures
                flattened.update(process_nested_field(value, key))
            elif isinstance(value, dict):
                # Recursively flatten nested objects
                flattened.update(flatten_json(value, mapped_key))
            elif isinstance(value, list) and value:
                if isinstance(value[0], dict):
                    # For list of dicts, try to extract meaningful data
                    flattened[mapped_key] = extract_list_values(value)
                else:
                    # For simple lists, join with pipe separator
                    flattened[mapped_key] = ' | '.join(str(v) for v in value if v)
            else:
                flattened[mapped_key] = value
    else:
        flattened[prefix or 'value'] = json_obj
    
    return flattened

def map_cordis_field_name(field_name: str, prefix: str = '') -> str:
    """Map CORDIS JSON field names to our database column names."""
    # Common CORDIS field mappings
    field_mappings = {
        # Basic project info
        'projectID': 'id',
        'acronym': 'acronym',
        'status': 'status', 
        'title': 'title',
        'objective': 'objective',
        
        # Dates
        'startDate': 'startdate',
        'endDate': 'enddate',
        'contentUpdateDate': 'contentupdatedate',
        
        # Programme info
        'frameworkProgramme': 'frameworkprogramme',
        'programme': 'frameworkprogramme',
        'legalBasis': 'legalbasis',
        'masterCall': 'mastercall',
        'subCall': 'subcall',
        'fundingScheme': 'fundingscheme',
        'call': 'mastercall',
        
        # Financial
        'ecMaxContribution': 'ecmaxcontribution',
        'totalCost': 'totalcost',
        'maxContribution': 'ecmaxcontribution',
        'budget': 'totalcost',
        
        # IDs and references
        'rcn': 'rcn',
        'grantDoi': 'grantdoi',
        'doi': 'grantdoi',
    }
    
    # Create the key (with prefix if provided)
    full_key = f"{prefix}_{field_name}" if prefix else field_name
    
    # Try direct mapping first
    if field_name in field_mappings:
        return field_mappings[field_name]
    
    # Try mapping the full key
    if full_key in field_mappings:
        return field_mappings[full_key]
    
    # Return original key (lowercase for consistency)
    return full_key.lower()

def process_nested_field(data: Any, field_type: str) -> Dict[str, str]:
    """Process nested fields like organizations, topics, etc."""
    result = {}
    
    if not data:
        return result
    
    if isinstance(data, list):
        items = data
    else:
        items = [data]
    
    if field_type == 'organizations':
        names = []
        roles = []
        countries = []
        country_names = []
        org_urls = []
        contact_forms = []
        cities = []
        
        for org in items:
            if isinstance(org, dict):
                if org.get('name'):
                    names.append(str(org['name']))
                if org.get('role'):
                    roles.append(str(org['role']))
                if org.get('country'):
                    country_code = str(org['country'])
                    countries.append(country_code)
                    # Add country name
                    country_name = get_country_name(country_code)
                    if country_name:
                        country_names.append(country_name)
                if org.get('organizationURL'):
                    org_urls.append(str(org['organizationURL']))
                if org.get('contactForm'):
                    contact_forms.append(str(org['contactForm']))
                if org.get('city'):
                    cities.append(str(org['city']))
        
        result['org_names'] = ' | '.join(unique_values(names))
        result['roles'] = ' | '.join(unique_values(roles))
        result['org_countries'] = ' | '.join(unique_values(countries))
        result['org_country_names'] = ' | '.join(unique_values(country_names))
        result['organization_urls'] = ' | '.join(unique_values(org_urls))
        result['contact_forms'] = ' | '.join(unique_values(contact_forms))
        result['cities'] = ' | '.join(unique_values(cities))
    
    elif field_type == 'topics':
        codes = []
        descriptions = []
        
        for topic in items:
            if isinstance(topic, dict):
                if topic.get('code'):
                    codes.append(str(topic['code']))
                if topic.get('description'):
                    descriptions.append(str(topic['description']))
                # Try alternative field names
                if topic.get('topic'):
                    codes.append(str(topic['topic']))
                if topic.get('title'):
                    descriptions.append(str(topic['title']))
        
        result['topics_codes'] = ' | '.join(unique_values(codes))
        result['topics_desc'] = ' | '.join(unique_values(descriptions))
    
    elif field_type == 'euroSciVoc':
        codes = []
        labels = []
        
        for item in items:
            if isinstance(item, dict):
                if item.get('code'):
                    codes.append(str(item['code']))
                if item.get('label'):
                    labels.append(str(item['label']))
                # Try alternative field names
                if item.get('euroSciVocCode'):
                    codes.append(str(item['euroSciVocCode']))
                if item.get('euroSciVocTitle'):
                    labels.append(str(item['euroSciVocTitle']))
        
        result['euroscivoc_codes'] = ' | '.join(unique_values(codes))
        result['euroscivoc_labels'] = ' | '.join(unique_values(labels))
    
    elif field_type == 'webLinks':
        urls = []
        
        for link in items:
            if isinstance(link, dict):
                if link.get('url'):
                    urls.append(str(link['url']))
                elif link.get('physUrl'):
                    urls.append(str(link['physUrl']))
        
        result['project_urls'] = ' | '.join(unique_values(urls))
    
    return result

def extract_list_values(lst: List[Any]) -> str:
    """Extract and concatenate values from a list of items."""
    values = []
    for item in lst:
        if isinstance(item, dict):
            # Try to get a meaningful value from the dict
            for key in ['name', 'title', 'label', 'description', 'value']:
                if key in item and item[key]:
                    values.append(str(item[key]))
                    break
        else:
            values.append(str(item))
    
    return ' | '.join(unique_values(values))

def unique_values(values: List[str]) -> List[str]:
    """Return unique values preserving order."""
    seen = set()
    unique = []
    for val in values:
        val = val.strip()
        if val and val not in seen:
            seen.add(val)
            unique.append(val)
    return unique

def merge_programme_json(json_files: Dict[str, Any], programme_label: str) -> pd.DataFrame:
    """Merge JSON files from a single programme using project.json as main table."""
    print(f"🔧 Processing {programme_label} dataset...")
    
    # Step 1: Find and process the main project file
    main_df = None
    for filename, json_data in json_files.items():
        if filename == 'project.json':
            print(f"  📄 Processing main table: {filename}...")
            main_df = extract_project_data(json_data, filename)
            if not main_df.empty:
                main_df['id'] = main_df['id'].apply(convert_id_to_integer_string)
                print(f"    📊 Main table: {len(main_df)} projects with {len(main_df.columns)} columns")
            break
    
    if main_df is None or main_df.empty:
        print(f"  ❌ No project.json found or empty in {programme_label}")
        return pd.DataFrame()
    
    # Step 2: Process and join other files (excluding policyPriorities and webItem)
    excluded_files = ['project.json', 'policyPriorities.json', 'webItem.json']
    
    for filename, json_data in json_files.items():
        if filename not in excluded_files:
            print(f"  📄 Processing and joining: {filename}...")
            df = extract_project_data(json_data, filename)
            
            if not df.empty and 'id' in df.columns:
                df['id'] = df['id'].apply(convert_id_to_integer_string)
                
                # Left join with main table (keep all projects from main table)
                before_cols = len(main_df.columns)
                main_df = pd.merge(main_df, df, on='id', how='left', suffixes=('', '_new'))
                
                # Handle duplicate columns from merge - prefer non-null values
                for col in df.columns:
                    if col != 'id':
                        new_col = f"{col}_new"
                        if new_col in main_df.columns:
                            # Fill NaN values in original column with values from new column
                            # Also replace empty strings with new values
                            mask = (main_df[col].isna() | (main_df[col] == '')) & main_df[new_col].notna() & (main_df[new_col] != '')
                            if mask.any():
                                main_df.loc[mask, col] = main_df.loc[mask, new_col]
                            main_df = main_df.drop(columns=[new_col])
                        elif col not in main_df.columns:
                            # Add new column if it doesn't exist
                            main_df[col] = df.set_index('id')[col].reindex(main_df['id'])
                
                after_cols = len(main_df.columns)
                added_cols = after_cols - before_cols
                print(f"    📊 Joined: {len(df)} records, added {added_cols} new columns")
            else:
                print(f"    ⚠️  Skipped {filename}: empty or no 'id' column")
    
    # Add programme source
    main_df['programmeSource'] = programme_label
    
    print(f"  ✅ {programme_label} complete: {len(main_df)} projects with {len(main_df.columns)} columns")
    return main_df

def clean_text_field(text: Any, max_length: int = 10000) -> Optional[str]:
    """Clean and truncate text fields."""
    if pd.isna(text) or text is None:
        return None
    
    text_str = str(text).strip()
    if not text_str:
        return None
    
    # Remove problematic characters
    text_str = text_str.replace('\x00', '').replace('\r', ' ').replace('\n', ' ')
    
    # Truncate if too long
    if len(text_str) > max_length:
        text_str = text_str[:max_length-3] + "..."
    
    return text_str

def convert_id_to_integer_string(val):
    """Convert ID values to proper integer strings (remove decimal points)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    
    # Handle string representations of numbers with decimals
    if isinstance(val, str):
        try:
            # Try to convert to float first, then to int to remove decimal
            float_val = float(val)
            if pd.isna(float_val) or np.isinf(float_val):
                return None
            return str(int(float_val))
        except (ValueError, OverflowError):
            return val  # Return as-is if not a number
    
    # Handle numeric types
    if isinstance(val, (np.integer, int)):
        return str(int(val))
    
    if isinstance(val, (np.floating, float)):
        if pd.isna(val) or np.isinf(val):
            return None
        return str(int(val))
    
    # Fallback to string conversion
    try:
        return str(val)
    except:
        return None

def sanitize_for_json(val):
    """Enhanced JSON sanitizer with better error handling."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        if pd.isna(val) or np.isinf(val):
            return None
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime, pd.Timestamp)):
        try:
            return val.strftime("%Y-%m-%d")
        except:
            return None
    
    # Handle text fields with cleaning
    if isinstance(val, str):
        return clean_text_field(val)
    
    # Convert to string and clean
    try:
        result = clean_text_field(str(val))
        return result
    except:
        return None

def normalise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise DataFrame for Supabase with enhanced data cleaning."""
    print(f"🧹 Normalizing DataFrame with {len(df)} rows...")
    
    # Keep only allowed columns
    available_cols = [c for c in ALLOWED_COLUMNS if c in df.columns]
    df = df[available_cols].copy()
    
    print(f"  📊 Using {len(available_cols)} columns: {available_cols}")
    
    # Convert known date columns
    date_columns = ["startdate", "enddate", "contentupdatedate"]
    for col in date_columns:
        if col in df.columns:
            print(f"  📅 Processing date column: {col}")
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime("%Y-%m-%d")
    
    # Cast numeric columns
    numeric_columns = ["ecmaxcontribution", "totalcost", "rcn"]
    for col in numeric_columns:
        if col in df.columns:
            print(f"  🔢 Processing numeric column: {col}")
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Special handling for ID columns that must be integers
    id_columns = ["id", "rcn"]
    for col in id_columns:
        if col in df.columns:
            print(f"  🆔 Processing ID column: {col}")
            df[col] = df[col].apply(convert_id_to_integer_string)
    
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    
    # Sanitize each cell
    print(f"  🧼 Sanitizing all values...")
    for col in df.columns:
        df[col] = df[col].apply(sanitize_for_json)
    
    # Remove rows with no meaningful data (only id and programmeSource)
    essential_cols = [c for c in ['title', 'acronym', 'objective'] if c in df.columns]
    if essential_cols:
        mask = df[essential_cols].notna().any(axis=1)
        df = df[mask]
        print(f"  🎯 Kept {len(df)} rows with meaningful data")
    
    # Remove duplicate IDs - keep the first occurrence
    if 'id' in df.columns:
        initial_count = len(df)
        df = df.drop_duplicates(subset=['id'], keep='first')
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            print(f"  🗑️  Removed {duplicates_removed} duplicate IDs")
    
    return df

def push_to_supabase(df: pd.DataFrame, batch_size: int = 50, test_mode: bool = False, test_rows: int = 1000) -> None:
    """Upload DataFrame to Supabase with enhanced error handling."""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    table_name = os.environ.get("SUPABASE_TABLE", "cordis_projects")
    
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")
    
    # Apply test mode limit if enabled
    if test_mode and len(df) > test_rows:
        print(f"🧪 TEST MODE: Using only first {test_rows} rows out of {len(df)} total rows")
        df = df.head(test_rows).copy()

    endpoint = f"{supabase_url}/rest/v1/{table_name}?on_conflict=id"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    records = df.to_dict(orient="records")
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    print(f"📤 Uploading {len(records)} records in {total_batches} batches...")
    
    successful_uploads = 0
    failed_uploads = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1
        
        print(f"  📦 Batch {batch_num}/{total_batches} ({len(batch)} rows)...")
        
        try:
            # Enhanced JSON serialization
            json_data = json.dumps(batch, default=str, ensure_ascii=False)
            
            # Check JSON size
            size_mb = len(json_data.encode('utf-8')) / (1024 * 1024)
            if size_mb > 5:  # 5MB limit
                print(f"    ⚠️  Large batch size: {size_mb:.1f}MB")
            
        except Exception as e:
            print(f"    ❌ JSON serialization error: {e}")
            failed_uploads += len(batch)
            continue
            
        try:
            resp = requests.post(endpoint, headers=headers, data=json_data, timeout=60)
            
            if resp.status_code in (200, 201, 204):
                print(f"    ✅ Success")
                successful_uploads += len(batch)
            else:
                print(f"    ❌ HTTP {resp.status_code}: {resp.text[:200]}")
                failed_uploads += len(batch)
                
                # Continue with next batch instead of failing completely
                continue
                
        except Exception as e:
            print(f"    ❌ Upload error: {e}")
            failed_uploads += len(batch)
            continue
    
    print(f"\n📊 Upload Summary:")
    print(f"  ✅ Successful: {successful_uploads} records")
    print(f"  ❌ Failed: {failed_uploads} records")
    print(f"  📈 Success rate: {successful_uploads/(successful_uploads+failed_uploads)*100:.1f}%")

def main() -> None:
    """Main function - process each dataset separately then append them."""
    print("🚀 Starting CORDIS JSON ETL Pipeline...")
    print("📋 Processing each dataset independently, then appending")
    
    # Check for test mode
    test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
    test_rows = int(os.environ.get("TEST_ROWS", "1000"))
    
    if test_mode:
        print(f"🧪 TEST MODE ENABLED: Will upload maximum {test_rows} rows")
    
    all_dataframes = []
    
    # Process each dataset independently
    for label, url in CORDIS_JSON_URLS.items():
        print(f"\n{'='*50}")
        print(f"Processing {label}")
        print(f"{'='*50}")
        
        try:
            json_files = download_and_extract_json(url)
            if json_files:
                # Merge files within this dataset using project.json as main table
                merged = merge_programme_json(json_files, label)
                if not merged.empty:
                    all_dataframes.append(merged)
                    print(f"✅ {label}: {len(merged)} projects processed")
                else:
                    print(f"⚠️  {label}: No data extracted")
            else:
                print(f"❌ {label}: No JSON files found")
        except Exception as e:
            print(f"❌ {label}: Failed - {e}")
            continue
    
    if not all_dataframes:
        print("❌ No data processed from any source!")
        return
    
    # Simply append all datasets (no relationships between datasets)
    print(f"\n🔗 Appending {len(all_dataframes)} datasets...")
    
    combined = pd.concat(all_dataframes, ignore_index=True)
    
    print(f"\n📊 Final dataset: {len(combined)} rows, {len(combined.columns)} columns")
    
    # Show data distribution by programme
    if 'programmeSource' in combined.columns:
        distribution = combined['programmeSource'].value_counts()
        print("📈 Data distribution:")
        for prog, count in distribution.items():
            print(f"  {prog}: {count} records")
    
    # Normalize and upload
    cleaned = normalise_dataframe(combined)
    if not cleaned.empty:
        # Save processed data before upload
        print("💾 Saving processed data...")
        cleaned.to_pickle('cordis_data_processed.pkl')
        print(f"✅ Saved {len(cleaned)} records to cordis_data_processed.pkl")
        
        push_to_supabase(cleaned, batch_size=50, test_mode=test_mode, test_rows=test_rows)
        if test_mode:
            print(f"\n🧪 TEST MODE completed! Processed {min(len(cleaned), test_rows)} records.")
        else:
            print(f"\n🎉 Pipeline completed! Processed {len(cleaned)} records.")
    else:
        print("❌ No valid data to upload after cleaning!")

if __name__ == "__main__":
    main()
