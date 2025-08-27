#!/usr/bin/env python3
"""
UCVM_works.py — single-file OpenAlex ETL for UCVM dashboard

What this script does
---------------------
- Reads a roster CSV containing at least a column "OpenAlexID" (e.g., A########## or https://openalex.org/A##########).
- Fetches all works for each author via OpenAlex (cursor pagination), with retries/backoff and a
  proper User-Agent header.
- Flattens nested JSON with sep="__" so columns match expected keys.
- Adds convenience string columns: authors, institutions, concepts_list.
- Writes two compiled CSVs (lifetime and last5y) and then deduplicates the last5y into the path
  provided by --output.
- Logs to both file and console so GitHub Actions shows useful details.

Usage (as in your workflow):
    python etl/UCVM_works.py \
        --input data/roster_with_metrics.csv \
        --output data/openalex_all_authors_last5y_key_fields_dedup.csv

Notes
-----
- The script does NOT depend on any other local module (no utils_openalex import).
- The output directory is derived from --output; logs and compiled intermediate files live there.
- If zero authors are processed, the script exits nonzero so CI flags it.
"""

from __future__ import annotations

import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import Dict, Any, Iterable, List, Optional, Tuple
import argparse

import requests
import pandas as pd
from pandas import json_normalize

# ----------------------------
# CLI
# ----------------------------
parser = argparse.ArgumentParser(description="UCVM OpenAlex ETL (single-file)")
parser.add_argument("--input", "-i", required=True, help="Path to input faculty roster CSV")
parser.add_argument("--output", "-o", required=True, help="Path to deduplicated last-5-years output CSV")
args = parser.parse_args()

INPUT_ROSTER = args.input
OUTPUT_LAST5_DEDUP = args.output
OUTPUT_DIR = os.path.dirname(OUTPUT_LAST5_DEDUP) or "data"

# ----------------------------
# Config
# ----------------------------
MAILTO = os.getenv("OPENALEX_MAILTO", "jdebuck@ucalgary.ca")
BASE_URL = "https://api.openalex.org/works"
PER_PAGE = int(os.getenv("OPENALEX_PER_PAGE", "200"))
MAX_RETRIES = int(os.getenv("OPENALEX_MAX_RETRIES", "6"))
BACKOFF_BASE = float(os.getenv("OPENALEX_BACKOFF_BASE", "1.6"))
TIMEOUT = int(os.getenv("OPENALEX_TIMEOUT", "30"))
RETRIABLE_STATUS = {429, 500, 502, 503, 504}
HEADERS = {
    "User-Agent": f"UCVM-ETL (mailto:{MAILTO})",
    "Accept": "application/json",
}

# Key fields expected downstream / in dashboard
KEY_FIELDS_FOR_OUTPUT = [
    "id", "doi", "display_name", "publication_year", "type", "cited_by_count",
    "open_access__oa_status", "host_venue__display_name", "primary_location__source__display_name",
    "primary_topic__display_name", "primary_topic__field__display_name", "primary_topic__subfield__display_name",
    "biblio__volume", "biblio__issue", "biblio__first_page", "biblio__last_page", "fwci",
    "authors", "institutions", "concepts_list"
]
KEY_FIELDS_FOR_OUTPUT_WITH_TAGS = KEY_FIELDS_FOR_OUTPUT + ["author_name", "author_openalex_id"]

# ----------------------------
# Helpers
# ----------------------------

def _ensure_openalex_uri(author_id: str) -> str:
    """Accepts 'A##########' or full 'https://openalex.org/A##########' and returns full URI."""
    if not isinstance(author_id, str):
        return ""
    aid = author_id.strip()
    if not aid:
        return ""
    if aid.startswith("http://") or aid.startswith("https://"):
        return aid
    return f"https://openalex.org/{aid}"


def safe_join(items: Iterable[str], sep: str = "; ") -> str:
    return sep.join(sorted({(x or "").strip() for x in items if (x or "").strip()}))


def extract_string_lists_from_row(row: pd.Series) -> Tuple[str, str, str]:
    """Builds authors, institutions, concepts_list strings from still-nested list fields if present.

    After json_normalize(sep="__"), list-of-dicts fields (like authorships, concepts) remain Python lists.
    We parse those lists here.
    """
    # Authors
    authors_joined = ""
    if "authorships" in row and isinstance(row["authorships"], list):
        author_names: List[str] = []
        for a in row["authorships"]:
            try:
                nm = a.get("author", {}).get("display_name", "")
                if nm:
                    author_names.append(nm)
            except Exception:
                continue
        authors_joined = safe_join(author_names)

    # Institutions
    inst_joined = ""
    if "authorships" in row and isinstance(row["authorships"], list):
        inst_names: List[str] = []
        for a in row["authorships"]:
            try:
                insts = a.get("institutions", []) or []
                for inst in insts:
                    nm = inst.get("display_name", "")
                    if nm:
                        inst_names.append(nm)
            except Exception:
                continue
        inst_joined = safe_join(inst_names)

    # Concepts
    concepts_joined = ""
    if "concepts" in row and isinstance(row["concepts"], list):
        concept_names: List[str] = []
        for c in row["concepts"]:
            try:
                nm = c.get("display_name", "")
                if nm:
                    concept_names.append(nm)
            except Exception:
                continue
        concepts_joined = safe_join(concept_names)

    return authors_joined, inst_joined, concepts_joined


def add_convenience_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adds/derives authors, institutions, concepts_list, and ensures fwci column exists."""
    if df.empty:
        return df

    # Ensure fwci column exists (OpenAlex doesn't provide FWCI; keep as NaN unless provided upstream)
    if "fwci" not in df.columns:
        df["fwci"] = pd.NA

    # Build string-joined convenience columns from nested lists per row
    if any(col in df.columns for col in ("authorships", "concepts")):
        vals = df.apply(extract_string_lists_from_row, axis=1, result_type="expand")
        # vals has 3 columns if not empty
        if not vals.empty:
            df["authors"] = vals[0]
            df["institutions"] = vals[1]
            df["concepts_list"] = vals[2]

    # Ensure the explicit convenience columns exist even if lists were absent
    for col in ("authors", "institutions", "concepts_list"):
        if col not in df.columns:
            df[col] = ""

    return df


def append_df_to_csv(df: pd.DataFrame, path: str, fixed_cols: Optional[List[str]] = None) -> None:
    """Append rows using a *fixed schema* so the compiled CSV always has the same
    number/order of columns. This avoids downstream tokenizing errors when
    reading the compiled CSV back (mismatched header vs. rows).
    """
    if df.empty:
        logging.info(f"append_df_to_csv: nothing to write to {path} (empty df).")
        return

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    # Enforce a stable schema: add any missing fixed columns as empty, then reorder exactly
    if fixed_cols:
        for col in fixed_cols:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[fixed_cols]

    write_header = not os.path.exists(path)
    df.to_csv(path, index=False, header=write_header, mode=("w" if write_header else "a"))


    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    if fixed_cols:
        present = [col for col in fixed_cols if col in df.columns]
        missing = [col for col in fixed_cols if col not in df.columns]
        if missing:
            logging.debug(f"append_df_to_csv: missing columns for {os.path.basename(path)}: {missing}")
        df = df[present]

    if not os.path.exists(path):
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode='a', index=False, header=False)


def deduplicate_compiled(input_csv_path: str, output_csv_path: str) -> None:
    if not os.path.exists(input_csv_path):
        logging.warning(f"Input file for deduplication does not exist: {input_csv_path}")
        return

    df = pd.read_csv(input_csv_path)
    before = len(df)
    dedup_df = df.drop_duplicates(subset=["id", "doi"], keep="first")
    after = len(dedup_df)
    logging.info(f"Deduplicating {before} -> {after} rows")
    os.makedirs(os.path.dirname(output_csv_path) or ".", exist_ok=True)
    dedup_df.to_csv(output_csv_path, index=False)


# ----------------------------
# OpenAlex fetch (cursor pagination + backoff) — self-contained in this file
# ----------------------------

def fetch_author_works_filtered(full_author_id: str, years_back: int = 5) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch all works for an author using OpenAlex cursor pagination, flatten with sep="__",
    return (df_all, df_lastN). Adds author tags to df_lastN. Does NOT throw on HTTP errors; logs instead."""
    author_uri = _ensure_openalex_uri(full_author_id)
    if not author_uri:
        logging.warning("fetch_author_works_filtered: empty/invalid author id")
        return pd.DataFrame(), pd.DataFrame()

    current_year = datetime.now().year
    min_year = current_year - years_back + 1

    params = {
        "filter": f"author.id:{author_uri}",
        "per-page": PER_PAGE,
        "cursor": "*",
    }

    works_all: List[Dict[str, Any]] = []
    retries = 0

    logging.info(f"OpenAlex fetch for {author_uri} (last {years_back} years >= {min_year})")

    while True:
        try:
            resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        except requests.RequestException as e:
            logging.exception(f"OpenAlex request exception: {e}")
            break

        if resp.status_code in RETRIABLE_STATUS:
            delay = BACKOFF_BASE ** retries
            logging.warning(
                f"OpenAlex {resp.status_code} at cursor {params.get('cursor')!r}; retry {retries+1}/{MAX_RETRIES} in {delay:.1f}s"
            )
            time.sleep(delay)
            retries += 1
            if retries > MAX_RETRIES:
                logging.error("Max retries exceeded; aborting fetch for this author.")
                break
            continue

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logging.exception(f"HTTP error from OpenAlex: {e}")
            break

        data = resp.json()
        results = data.get("results", [])
        logging.debug(f"Fetched {len(results)} results at cursor {params.get('cursor')!r}")
        if not results:
            break

        works_all.extend(results)
        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break

        params["cursor"] = next_cursor
        retries = 0  # reset after success

    if not works_all:
        logging.info("No works returned from OpenAlex for this author.")
        return pd.DataFrame(), pd.DataFrame()

    # Flatten nested JSON into columns using the __ separator
    df_all = pd.json_normalize(works_all, sep="__")

    # Normalize/ensure key convenience columns exist
    if "publication_year" in df_all.columns:
        df_all["publication_year"] = pd.to_numeric(df_all["publication_year"], errors="coerce")
        df_last = df_all[df_all["publication_year"] >= min_year].copy()
    else:
        logging.warning("publication_year missing in df_all; last-N-years subset will be empty")
        df_last = df_all.iloc[0:0].copy()

    # Add convenience columns (authors, institutions, concepts_list, fwci placeholder)
    df_all = add_convenience_columns(df_all)
    df_last = add_convenience_columns(df_last)

    # Tag with author for downstream grouping
    df_last["author_name"] = author_uri.rsplit("/", 1)[-1]
    df_last["author_openalex_id"] = author_uri

    # Optional visibility for schema drift
    missing = [c for c in KEY_FIELDS_FOR_OUTPUT_WITH_TAGS if c not in df_all.columns]
    if missing:
        logging.debug(f"Flattened df_all missing expected columns: {missing}")

    return df_all, df_last


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_dir = os.path.join(OUTPUT_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),  # show in GH Actions console too
        ],
    )

    compiled_lifetime_path = os.path.join(OUTPUT_DIR, "openalex_all_authors_lifetime.csv")
    compiled_last5_path   = os.path.join(OUTPUT_DIR, "openalex_all_authors_last5y_key_fields.csv")

    # Start fresh each run to avoid legacy headers/rows mismatch from previous runs
    for p in (compiled_lifetime_path, compiled_last5_path, OUTPUT_LAST5_DEDUP):
        try:
            os.remove(p)
            logging.info(f"Removed old artifact: {p}")
        except FileNotFoundError:
            pass

    # Load roster
    logging.info(f"Reading roster from {INPUT_ROSTER}")
    try:
        roster = pd.read_csv(INPUT_ROSTER)
    except Exception as e:
        logging.exception(f"Failed to read roster CSV: {e}")
        sys.exit(1)

    # Detect columns for name and OpenAlexID
    def get_row_identifiers(row: pd.Series) -> Tuple[str, str]:
        author_id = row.get("OpenAlexID")
        name = row.get("Name") or row.get("Author") or row.get("FullName") or ""
        if not isinstance(name, str) or not name.strip():
            name = str(author_id or "").strip() or "Unknown"
        return name, str(author_id or "").strip()

    processed = 0
    skipped_missing_id = 0

    for idx, row in roster.iterrows():
        author_name, author_id = get_row_identifiers(row)
        if not author_id:
            skipped_missing_id += 1
            logging.info(f"Skipping row {idx} — missing OpenAlexID")
            continue

        logging.info(f"Processing {author_name} ({author_id})")
        try:
            df_all, df_last5 = fetch_author_works_filtered(author_id)
        except Exception:
            logging.exception(f"Error fetching works for {author_name} ({author_id})")
            continue

        if not df_all.empty:
            append_df_to_csv(df_all, compiled_lifetime_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
            logging.info(f"Appended {len(df_all)} lifetime works for {author_name}")
        else:
            logging.info(f"No lifetime works for {author_name}")

        if not df_last5.empty:
            append_df_to_csv(df_last5, compiled_last5_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
            logging.info(f"Appended {len(df_last5)} last-5y works for {author_name}")
            processed += 1
        else:
            logging.info(f"No last-5y works for {author_name}")

    logging.info(f"Total skipped rows due to missing ID: {skipped_missing_id}")

    # Deduplicate compiled last5 into the requested --output file
    if os.path.exists(compiled_last5_path):
        try:
            deduplicate_compiled(compiled_last5_path, OUTPUT_LAST5_DEDUP)
            logging.info(f"Deduplicated file written to {OUTPUT_LAST5_DEDUP}")
        except Exception:
            # As a last-resort safety, show the first few suspicious lines around the error point
            logging.exception("Deduplication failed while reading compiled CSV. This usually means a schema mismatch. We now enforce a fixed schema per write and start fresh each run, so this should not recur.")
            sys.exit(1)
    else:
        logging.warning(f"No compiled last-5y file found at {compiled_last5_path}; nothing to deduplicate.")

    if processed == 0:
        logging.error("No authors processed with last-5y output — failing run so CI flags it.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)

    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)
