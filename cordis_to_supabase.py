import os
import io
import zipfile
import requests
import pandas as pd
import datetime
import numpy as np
from supabase import create_client, Client

# ===============================
# CONFIG — set your Supabase creds
# ===============================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_TABLE = "cordis_projects"

# Official CORDIS dataset URLs
CORDIS_URLS = {
    "HORIZONprojects": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
    "h2020projects": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
    "fp7projects": "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip",
}

# Columns allowed in Supabase table (must match your table exactly)
ALLOWED_COLUMNS = [
    "id", "acronym", "status", "title", "objective", "startDate", "endDate",
    "frameworkProgramme", "legalBasis", "masterCall", "subCall", "fundingScheme",
    "ecMaxContribution", "totalCost",
    "org_names", "coordinator_names", "org_countries",
    "topics_codes", "topics_desc",
    "euroSciVoc_labels", "euroSciVoc_codes",
    "project_urls", "contentUpdateDate", "rcn", "grantDoi", "programmeSource",
]

# ===============================
# Helpers
# ===============================

def download_and_extract(url: str) -> dict:
    print(f"Downloading {url}...")
    resp = requests.get(url)
    resp.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    files = {name: z.read(name) for name in z.namelist() if name.endswith(".csv")}
    return files

def read_csv_bytes(data_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(
        io.BytesIO(data_bytes),
        sep=";",
        quotechar='"',
        low_memory=False,
        on_bad_lines="skip",
    )

def unique_join(values, sep=" | "):
    out = []
    for v in values:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if s and s not in out:
            out.append(s)
    return sep.join(out)

def merge_programme(files: dict, programme_label: str) -> pd.DataFrame:
    project = read_csv_bytes(files["project.csv"])
    project["id"] = project["id"].astype(str)

    def agg_child(filename, key_field, agg_fields):
        if filename not in files:
            return pd.DataFrame()
        df = read_csv_bytes(files[filename])
        df[key_field] = df[key_field].astype(str)
        parts = []
        for col in agg_fields:
            if col in df.columns:
                parts.append(
                    df.groupby(key_field)[col].apply(lambda s: unique_join(s)).rename(col)
                )
        return pd.concat(parts, axis=1) if parts else pd.DataFrame()

    org_agg   = agg_child("organization.csv", "projectID", ["name", "role", "country"])
    topics    = agg_child("topics.csv",       "projectID", ["code", "description"])
    euro_agg  = agg_child("euroSciVoc.csv",   "projectID", ["code", "label"])
    web_agg   = agg_child("webLink.csv",      "projectID", ["physUrl"])

    flat = project.copy()
    for agg in [org_agg, topics, euro_agg, web_agg]:
        if not agg.empty:
            flat = flat.merge(agg, how="left", left_on="id", right_index=True)

    flat["programmeSource"] = programme_label
    return flat

# JSON-safe sanitizer
def sanitize_for_json(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime, pd.Timestamp)):
        return val.strftime("%Y-%m-%d")
    return str(val)

def clean_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    # keep only allowed columns
    df = df[[c for c in ALLOWED_COLUMNS if c in df.columns]].copy()

    # dates → ISO strings
    for col in ["startDate", "endDate", "contentUpdateDate"]:
        if col in df.columns:
            df.loc[:, col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # numeric normalization (cast strings like "3893032" → numbers)
    for col in ["ecMaxContribution", "totalCost", "rcn"]:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")

    # NaN → None
    df = df.where(pd.notnull(df), None)

    # ensure every cell is JSON-safe
    for col in df.columns:
        df.loc[:, col] = df[col].apply(sanitize_for_json)

    return df

def push_to_supabase(df: pd.DataFrame, batch_size=200):
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    data = df.to_dict(orient="records")
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        print(f"Pushing batch {i//batch_size + 1} ({len(batch)} rows)...")
        try:
            # returning="minimal" prevents client from trying to parse an empty/none JSON body
            supabase.table(SUPABASE_TABLE).upsert(
                batch,
                on_conflict="id",
                returning="minimal",
            ).execute()
        except Exception as e:
            print(f"❌ Error in batch {i//batch_size + 1}: {e}")
            print("First row in failing batch:", batch[0] if batch else None)
            raise

# ===============================
# Main
# ===============================

def main():
    all_dfs = []
    for label, url in CORDIS_URLS.items():
        files = download_and_extract(url)
        merged = merge_programme(files, label)
        all_dfs.append(merged)

    final_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Final merged dataset: {len(final_df)} rows, {len(final_df.columns)} columns")

    clean_df = clean_for_supabase(final_df)
    push_to_supabase(clean_df, batch_size=200)

if __name__ == "__main__":
    main()
