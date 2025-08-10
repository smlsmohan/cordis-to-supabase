import os
import io
import json
import zipfile
import requests
import pandas as pd
import datetime
import numpy as np

"""
cordis_to_supabase.py
====================

This script downloads all of the publicly available CORDIS project data
(Horizon Europe, Horizon 2020 and FP7), extracts the semicolon‑separated
CSV files, joins them into a single flat table and uploads the result
to a Supabase table using plain HTTP requests. It’s designed for use
in a GitHub Actions workflow, but can also be run locally as long as
your Supabase project URL and service role key are available via
environment variables.

Prerequisites:

* A Supabase table named ``cordis_projects`` matching the schema
  defined in the accompanying SQL snippet. The script will upsert
  rows using the project ``id`` as the primary key.
* Environment variables ``SUPABASE_URL`` and ``SUPABASE_KEY`` set to
  your project URL and service role key. When running in GitHub
  Actions, store these values as repository secrets and expose them
  in the workflow job’s ``env`` section.
* Python dependencies listed in ``requirements.txt`` (pandas,
  numpy and requests).

To run locally:

.. code:: bash

    pip install -r requirements.txt
    export SUPABASE_URL=https://your-project.supabase.co
    export SUPABASE_KEY=your-service-role-key
    python cordis_to_supabase.py

When executed, the script will fetch the CORDIS datasets, merge
organizational details, topics, EuroSciVoc vocabulary and weblinks
onto each project, normalise fields for JSON and upload them in
manageable batches (default 100 rows) to Supabase.

If any batch fails to upload, the script prints the HTTP status
code and the beginning of the response body along with the first
row of the failing batch to aid debugging.
"""

# URLs for the official CORDIS CSV archives
CORDIS_URLS = {
    "HORIZONprojects": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
    "h2020projects": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
    "fp7projects": "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip",
}

# Columns expected in the Supabase table. Ensure your table matches
# these names exactly. Extra columns will be dropped.
ALLOWED_COLUMNS = [
    "id", "acronym", "status", "title", "objective",
    "startDate", "endDate",
    "frameworkProgramme", "legalBasis", "masterCall", "subCall", "fundingScheme",
    "ecMaxContribution", "totalCost",
    "org_names", "coordinator_names", "org_countries",
    "topics_codes", "topics_desc",
    "euroSciVoc_labels", "euroSciVoc_codes",
    "project_urls", "contentUpdateDate", "rcn", "grantDoi", "programmeSource",
]

def download_and_extract(url: str) -> dict:
    """Download a zip archive from ``url`` and return a dictionary of
    filename → bytes for all CSV files contained within. Raises if
    the request fails."""
    print(f"Downloading {url}...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    return {
        name: z.read(name)
        for name in z.namelist()
        if name.endswith(".csv")
    }

def read_csv_bytes(data: bytes) -> pd.DataFrame:
    """Read semicolon‑separated CSV bytes into a pandas DataFrame."""
    return pd.read_csv(
        io.BytesIO(data),
        sep=";",
        quotechar='"',
        low_memory=False,
        on_bad_lines="skip",
    )

def unique_join(values, sep: str = " | ") -> str:
    """Concatenate a sequence of values into a unique, deduplicated string."""
    out = []
    for v in values:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if s and s not in out:
            out.append(s)
    return sep.join(out)

def merge_programme(files: dict, programme_label: str) -> pd.DataFrame:
    """Merge a single CORDIS programme into a flat DataFrame.

    Takes a dictionary of CSV bytes produced by ``download_and_extract`` and
    returns a DataFrame with one row per project. The ``programme_label``
    is stored in the ``programmeSource`` column for downstream filtering.
    """
    project = read_csv_bytes(files["project.csv"])
    project["id"] = project["id"].astype(str)

    def aggregate_child(filename: str, key_field: str, fields: list) -> pd.DataFrame:
        if filename not in files:
            return pd.DataFrame()
        df = read_csv_bytes(files[filename])
        df[key_field] = df[key_field].astype(str)
        aggregates = []
        for col in fields:
            if col in df.columns:
                aggregates.append(
                    df.groupby(key_field)[col]
                    .apply(lambda s: unique_join(s))
                    .rename(col)
                )
        return pd.concat(aggregates, axis=1) if aggregates else pd.DataFrame()

    org_agg = aggregate_child("organization.csv", "projectID", ["name", "role", "country"])
    topics_agg = aggregate_child("topics.csv", "projectID", ["code", "description"])
    euro_agg = aggregate_child("euroSciVoc.csv", "projectID", ["code", "label"])
    web_agg = aggregate_child("webLink.csv", "projectID", ["physUrl"])

    flat = project.copy()
    for agg in [org_agg, topics_agg, euro_agg, web_agg]:
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

def normalise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise a DataFrame for Supabase:
    - Keep only ``ALLOWED_COLUMNS``
    - Convert dates to ISO strings
    - Parse numeric strings into numbers
    - Replace NaNs with None
    - Sanitise every value for JSON
    """
    df = df[[c for c in ALLOWED_COLUMNS if c in df.columns]].copy()

    # Convert known date columns
    for col in ["startDate", "endDate", "contentUpdateDate"]:
        if col in df.columns:
            df.loc[:, col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # Cast numeric columns
    for col in ["ecMaxContribution", "totalCost", "rcn"]:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")

    df = df.where(pd.notnull(df), None)

    # Sanitise each cell
    for col in df.columns:
        df.loc[:, col] = df[col].apply(sanitize_for_json)

    return df

def push_to_supabase(df: pd.DataFrame, batch_size: int = 100) -> None:
    """Upload the DataFrame to Supabase using raw HTTP upserts.

    Requires environment variables ``SUPABASE_URL`` and ``SUPABASE_KEY``. The
    payload is sent in batches to avoid exceeding request size limits.
    The ``Prefer`` header instructs PostgREST to merge duplicates (upsert) and
    return no response body (minimal). If a request fails, the status
    code and first part of the response body are printed and an
    exception is raised to halt the script.
    """
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")

    # Use on_conflict=id so PostgREST knows which column to use for upserts.
    # Without this, a POST can be rejected with a 405 if the server
    # doesn't know how to handle duplicates. See Supabase docs for details.
    endpoint = f"{supabase_url}/rest/v1/{SUPABASE_TABLE}?on_conflict=id"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        print(f"Pushing batch {i // batch_size + 1} ({len(batch)} rows)...")
        resp = requests.post(endpoint, headers=headers, data=json.dumps(batch))
        if resp.status_code not in (200, 201, 204):
            body_preview = resp.text[:1000]
            print(f"❌ Error {resp.status_code}: {body_preview}")
            print("First row in failing batch:", batch[0] if batch else None)
            resp.raise_for_status()

# Name of the Supabase table – adjust if you use a different one
SUPABASE_TABLE = os.environ.get("SUPABASE_TABLE", "cordis_projects")

def main() -> None:
    # Download and merge all programmes
    dfs = []
    for label, url in CORDIS_URLS.items():
        files = download_and_extract(url)
        merged = merge_programme(files, label)
        dfs.append(merged)

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Final merged dataset: {len(combined)} rows, {len(combined.columns)} columns")
    cleaned = normalise_dataframe(combined)
    push_to_supabase(cleaned, batch_size=100)

if __name__ == "__main__":
    main()
