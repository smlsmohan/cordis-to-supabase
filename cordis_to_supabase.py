import os
import io
import zipfile
import requests
import pandas as pd
from supabase import create_client, Client

# ===============================
# CONFIG — set your Supabase creds
# ===============================
SUPABASE_URL = os.environ.get("SUPABASE_URL")       # e.g. "https://xxxx.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")       # service role key
SUPABASE_TABLE = "cordis_projects"

# Official CORDIS dataset URLs
CORDIS_URLS = {
    "HORIZONprojects": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
    "h2020projects": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
    "fp7projects": "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip"
}

# ===============================
# Functions
# ===============================

def download_and_extract(url):
    print(f"Downloading {url}...")
    resp = requests.get(url)
    resp.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    files = {name: z.read(name) for name in z.namelist() if name.endswith(".csv")}
    return files

def read_csv_bytes(data_bytes):
    return pd.read_csv(io.BytesIO(data_bytes), sep=";", quotechar='"', low_memory=False, on_bad_lines="skip")

def unique_join(values, sep=" | "):
    clean = []
    for v in values:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if s and s not in clean:
            clean.append(s)
    return sep.join(clean)

def merge_programme(files, programme_label):
    project = read_csv_bytes(files["project.csv"])
    project["id"] = project["id"].astype(str)

    def agg_child(filename, key_field, agg_fields):
        if filename not in files:
            return pd.DataFrame()
        df = read_csv_bytes(files[filename])
        df[key_field] = df[key_field].astype(str)
        aggs = []
        for col in agg_fields:
            if col in df.columns:
                aggs.append(df.groupby(key_field)[col].apply(lambda s: unique_join(s)).rename(col))
        return pd.concat(aggs, axis=1) if aggs else pd.DataFrame()

    org_agg = agg_child("organization.csv", "projectID", ["name", "role", "country"])
    topics_agg = agg_child("topics.csv", "projectID", ["code", "description"])
    euro_agg = agg_child("euroSciVoc.csv", "projectID", ["code", "label"])
    web_agg = agg_child("webLink.csv", "projectID", ["physUrl"])

    flat = project.copy()
    for agg in [org_agg, topics_agg, euro_agg, web_agg]:
        if not agg.empty:
            flat = flat.merge(agg, how="left", left_on="id", right_index=True)

    flat["programmeSource"] = programme_label
    return flat

def push_to_supabase(df: pd.DataFrame):
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    data_dicts = df.to_dict(orient="records")
    batch_size = 500  # Supabase limit
    for i in range(0, len(data_dicts), batch_size):
        batch = data_dicts[i:i+batch_size]
        print(f"Pushing batch {i//batch_size+1} ({len(batch)} rows)...")
        supabase.table(SUPABASE_TABLE).upsert(batch).execute()

# ===============================
# Main pipeline
# ===============================
def main():
    all_dfs = []
    for label, url in CORDIS_URLS.items():
        files = download_and_extract(url)
        merged_df = merge_programme(files, label)
        all_dfs.append(merged_df)

    final_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Final merged dataset: {len(final_df)} rows, {len(final_df.columns)} columns")

    # Push to Supabase
    push_to_supabase(final_df)

if __name__ == "__main__":
    main()
