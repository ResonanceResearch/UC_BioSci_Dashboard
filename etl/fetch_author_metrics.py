#!/usr/bin/env python3
"""
Append OpenAlex author metrics to a roster file.

New in this version:
- If a row has only an OpenAlex ID or only an ORCID, the script will
  look up the missing identifier via the OpenAlex API and add it to the
  output (and to the in-memory dataframe before metrics are fetched).

What stays the same:
- Robust detection of the OpenAlex ID column (accepts many header variants
  and ID formats: raw A..., openalex:..., human URL, API URL).
- Gentle API usage (User-Agent with optional mailto, retry with backoff,
  delay between calls).
- Outputs H_index, I10_index, Works_count, Total_citations (same names),
  and logs simple deltas vs a previous output if requested.

Usage examples:
    python fetch_author_metrics.py \
      --input data/faculty.csv \
      --output data/faculty_with_metrics.csv \
      --email you@university.ca --log-diffs

Supported input types: .csv, .tsv, .xlsx, .xls
Output is always CSV.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

OPENALEX_BASE = "https://api.openalex.org"

# ------------------------- Logging -------------------------

def setup_logging() -> None:
    os.makedirs("data/logs", exist_ok=True)
    logfile = os.path.join("data/logs", f"fetch_author_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(logfile, encoding="utf-8"),
        ],
    )
    # Also log to stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(sh)

    logging.info("Logging to %s", logfile)

# ------------------------- Column detection -------------------------

def _normalize(s: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def find_openalex_col(columns) -> Optional[str]:
    """Return the column that contains OpenAlex Author IDs.

    Accepts common variants such as:
    - OpenAlexID / OpenAlex ID / openalex_id / openalex author id
    - Any header whose normalized form contains BOTH "openalex" and "id".
    """
    norm_map = {_normalize(c): c for c in columns}

    # Direct patterns
    for norm, real in norm_map.items():
        if "openalex" in norm and norm.endswith("id"):
            return real
        if norm in {
            "openalexid", "openalexauthorid", "openalex_id",
            "openalexauthor_id", "openalexauthorid",
        }:
            return real

    # Last resort: a literal "openalex" column
    for norm, real in norm_map.items():
        if norm == "openalex":
            return real
    return None


def find_orcid_col(columns) -> Optional[str]:
    """Return the column that contains ORCIDs (if present)."""
    norm_map = {_normalize(c): c for c in columns}
    candidates = [
        "orcid", "orcidid", "orcid_id", "orcidiD", "orcididentifier",
        "orcidlink", "orcidurl", "orcid_i_d",
    ]
    for norm, real in norm_map.items():
        if norm in candidates:
            return real
        if "orcid" in norm:
            return real
    return None

# ------------------------- ID normalization -------------------------

def normalize_author_id(author_id: str) -> str:
    """Convert various forms of an OpenAlex author id into a canonical API URL.
    Supported inputs include raw IDs (A...), openalex: prefix, and https URLs.
    Returns an API URL: https://api.openalex.org/authors/Axxxxxx
    """
    aid = (str(author_id) if author_id is not None else "").strip()
    if not aid or aid.lower() in {"nan", "none"}:
        return ""

    # Human site URL -> API endpoint
    if aid.startswith("https://openalex.org/") or aid.startswith("http://openalex.org/"):
        last = aid.rstrip("/").split("/")[-1]
        if last and last[0].lower() == "a":
            last = "A" + last[1:]
        return f"{OPENALEX_BASE}/authors/{last}"

    # Already API URL
    if aid.startswith("https://api.openalex.org/") or aid.startswith("http://api.openalex.org/"):
        return aid

    # openalex: prefix
    if aid.lower().startswith("openalex:"):
        aid = aid.split(":", 1)[1]

    # Bare ID -> ensure uppercase A
    if aid and aid[0].lower() == "a":
        aid = "A" + aid[1:]

    return f"{OPENALEX_BASE}/authors/{aid}"


def normalize_orcid(orcid: str) -> str:
    """Return ORCID in bare 16-digit form with hyphens (e.g., 0000-0002-1825-0097).
    Accepts full URLs or bare values; returns "" for missing.
    """
    val = (str(orcid) if orcid is not None else "").strip()
    if not val or val.lower() in {"nan", "none"}:
        return ""
    # Extract the last 19 chars if a URL was provided
    m = re.search(r"(\d{4}-\d{4}-\d{4}-[\dX]{4})", val)
    if m:
        return m.group(1)
    # Insert hyphens if a compact 16-char form
    digits = re.sub(r"[^0-9X]", "", val)
    if len(digits) == 16:
        return f"{digits[0:4]}-{digits[4:8]}-{digits[8:12]}-{digits[12:16]}"
    return val

# ------------------------- HTTP helpers -------------------------

def build_session(email: Optional[str]) -> requests.Session:
    session = requests.Session()
    ua = "openalex-metrics/1.2"
    if email:
        ua += f" ({email})"
    session.headers.update({"User-Agent": ua})
    session.timeout = 30
    return session


def _get(session: requests.Session, url: str, params: Dict[str, Any], *, max_tries: int = 3, backoff: float = 1.0) -> Optional[requests.Response]:
    for attempt in range(1, max_tries + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                logging.warning("HTTP %s from %s; retrying (attempt %d/%d)", resp.status_code, url, attempt, max_tries)
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logging.warning("Request error: %s; retrying (attempt %d/%d)", e, attempt, max_tries)
            time.sleep(backoff)
            backoff *= 2
    logging.error("Failed after %d attempts: %s", max_tries, url)
    return None

# ------------------------- OpenAlex fetch -------------------------

def fetch_author(url_or_id: str, session: requests.Session, *, email: Optional[str], max_tries: int = 3, backoff: float = 1.0) -> Optional[Dict[str, Any]]:
    """Fetch author data from OpenAlex. Returns JSON dict or None on failure."""
    url = normalize_author_id(url_or_id)
    if not url:
        return None
    params = {
        # h_index, i10_index live under summary_stats
        "select": "id,display_name,works_count,cited_by_count,orcid,summary_stats",
    }
    if email:
        params["mailto"] = email
    resp = _get(session, url, params, max_tries=max_tries, backoff=backoff)
    if not resp:
        return None
    try:
        return resp.json()
    except Exception:
        logging.error("Could not parse JSON from %s", url)
        return None


def fetch_by_orcid(orcid: str, session: requests.Session, *, email: Optional[str]) -> Optional[Dict[str, Any]]:
    """Fetch an author object using an ORCID (bare or URL)."""
    norm = normalize_orcid(orcid)
    if not norm:
        return None
    # OpenAlex supports path form /authors/orcid:<id>
    url = f"{OPENALEX_BASE}/authors/orcid:{norm}"
    params: Dict[str, Any] = {"select": "id,display_name,works_count,cited_by_count,orcid,summary_stats"}
    if email:
        params["mailto"] = email
    resp = _get(session, url, params, max_tries=3, backoff=1.0)
    if not resp:
        return None
    try:
        return resp.json()
    except Exception:
        logging.error("Could not parse JSON from %s", url)
        return None

# ------------------------- Transform -------------------------

def extract_metrics(author_json: Dict[str, Any]) -> Dict[str, Any]:
    ss = author_json.get("summary_stats") or {}
    return {
        "Display_name": author_json.get("display_name"),
        "OpenAlexID": author_json.get("id"),
        "ORCID": author_json.get("orcid"),
        "H_index": ss.get("h_index"),
        "I10_index": ss.get("i10_index"),
        "Works_count": author_json.get("works_count"),
        "Total_citations": author_json.get("cited_by_count"),
    }

# ------------------------- IO -------------------------

def read_input(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path.lower())[1]
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if ext in [".csv", ".tsv"]:
        sep = "," if ext == ".csv" else "\t"
        return pd.read_csv(path, sep=sep)
    raise ValueError("Unsupported input format. Use .csv, .tsv, .xlsx, or .xls")


def write_output(df: pd.DataFrame, out_path: str) -> None:
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    logging.info("[ok] Wrote: %s", out_path)

# ------------------------- Cross-resolve IDs -------------------------

def resolve_missing_ids(df: pd.DataFrame, *, openalex_col: Optional[str], orcid_col: Optional[str], session: requests.Session, email: Optional[str], delay: float) -> pd.DataFrame:
    """For each row, if either OpenAlexID or ORCID is missing but the other exists, look up the missing one.
    Returns an updated DataFrame with both columns filled where possible.
    The function does not write to disk; it only updates the in-memory df.
    """
    # Ensure we have explicit columns in the df for output consistency
    if openalex_col is None:
        openalex_col = "OpenAlexID"
        if "OpenAlexID" not in df.columns:
            df[openalex_col] = ""
    if orcid_col is None:
        orcid_col = "ORCID"
        if "ORCID" not in df.columns:
            df[orcid_col] = ""

    cache_by_openalex: Dict[str, Dict[str, Any]] = {}
    cache_by_orcid: Dict[str, Dict[str, Any]] = {}

    for idx, row in df.iterrows():
        raw_openalex = str(row.get(openalex_col) or "").strip()
        raw_orcid = str(row.get(orcid_col) or "").strip()

        have_openalex = bool(raw_openalex)
        have_orcid = bool(normalize_orcid(raw_orcid))

        author_obj: Optional[Dict[str, Any]] = None

        if have_openalex and have_orcid:
            # Nothing to do
            continue

        if have_openalex and not have_orcid:
            key = normalize_author_id(raw_openalex)
            author_obj = cache_by_openalex.get(key)
            if not author_obj:
                author_obj = fetch_author(raw_openalex, session, email=email)
                if author_obj:
                    cache_by_openalex[key] = author_obj
                    time.sleep(delay)
            if author_obj:
                df.at[idx, orcid_col] = author_obj.get("orcid") or ""
            continue

        if have_orcid and not have_openalex:
            norm = normalize_orcid(raw_orcid)
            author_obj = cache_by_orcid.get(norm)
            if not author_obj:
                author_obj = fetch_by_orcid(norm, session, email=email)
                if author_obj:
                    cache_by_orcid[norm] = author_obj
                    time.sleep(delay)
            if author_obj:
                df.at[idx, openalex_col] = author_obj.get("id") or ""
            continue

    return df

# ------------------------- Main -------------------------

def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="Append OpenAlex metrics to a roster file (now ORCID-aware).")
    parser.add_argument("--input", "-i", required=True, help="Path to input CSV/TSV/Excel file")
    parser.add_argument("--output", "-o", default=None, help="Path to output CSV (default: <input>_with_metrics.csv)")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (s) between API calls to be gentle on rate limits")
    parser.add_argument("--email", type=str, default=None, help="Contact email for User-Agent and mailto, e.g., name@ucalgary.ca")
    parser.add_argument("--log-diffs", action="store_true", help="If an older output exists, log per-row metric deltas")
    args = parser.parse_args()

    in_path = args.input
    out_path = args.output or f"{os.path.splitext(in_path)[0]}_with_metrics.csv"

    # Determine contact email
    email = args.email or os.environ.get("OPENALEX_MAILTO") or os.environ.get("CONTACT_EMAIL")

    # Read input
    try:
        df = read_input(in_path)
    except Exception as e:
        logging.error("Failed to read input: %s", e)
        sys.exit(1)

    # Detect ID columns
    openalex_col = find_openalex_col(df.columns)
    orcid_col = find_orcid_col(df.columns)

    if openalex_col is None and orcid_col is None:
        logging.error("No OpenAlex ID or ORCID column detected. Please add one.")
        sys.exit(2)

    session = build_session(email)

    # First: fill missing IDs using the other identifier, if present
    df = resolve_missing_ids(df, openalex_col=openalex_col, orcid_col=orcid_col, session=session, email=email, delay=args.delay)

    # Build results rows
    out_rows = []
    prev_out_path = out_path if args.log_diffs else None
    prev_df: Optional[pd.DataFrame] = None
    if prev_out_path and os.path.exists(prev_out_path):
        try:
            prev_df = pd.read_csv(prev_out_path)
        except Exception:
            prev_df = None

    # Ensure explicit columns exist and remember their names for output
    if openalex_col is None:
        openalex_col = "OpenAlexID"
    if orcid_col is None:
        orcid_col = "ORCID"

    # Iterate
    for _, row in df.iterrows():
        author_id_val = row.get(openalex_col)
        orcid_val = row.get(orcid_col)

        author_json: Optional[Dict[str, Any]] = None

        # Prefer OpenAlex ID if available; otherwise try ORCID
        if author_id_val and str(author_id_val).strip():
            author_json = fetch_author(str(author_id_val), session, email=email)
            if author_json:
                time.sleep(args.delay)
        elif orcid_val and normalize_orcid(str(orcid_val)):
            author_json = fetch_by_orcid(str(orcid_val), session, email=email)
            if author_json:
                time.sleep(args.delay)

        if not author_json:
            out_rows.append({
                "Display_name": None,
                "OpenAlexID": str(author_id_val or ""),
                "ORCID": normalize_orcid(str(orcid_val or "")) or "",
                "H_index": None,
                "I10_index": None,
                "Works_count": None,
                "Total_citations": None,
            })
            continue

        m = extract_metrics(author_json)
        out_rows.append(m)

    out_df = pd.DataFrame(out_rows)

    # If requested, log simple deltas vs previous output
    if prev_df is not None and len(prev_df) == len(out_df):
        for col in ("H_index", "I10_index", "Works_count", "Total_citations"):
            if col in prev_df.columns and col in out_df.columns:
                diffs = out_df[col].fillna(0).astype(float) - prev_df[col].fillna(0).astype(float)
                changed = int((diffs != 0).sum())
                total_delta = float(diffs.sum())
                logging.info("[diff] %s: %d rows changed; total delta = %s", col, changed, total_delta)

    # Merge original dataframe with metrics on best-effort key (OpenAlexID/ORCID/Display_name)
    # Prefer to append columns rather than drop any existing ones.
    # We'll align rows by order to avoid accidental mismatches.
    merged = df.copy()
    # Ensure these columns exist in merged and overwrite with the finalized identifiers from out_df
    merged[openalex_col] = out_df["OpenAlexID"]
    merged[orcid_col] = out_df["ORCID"]
    # Append metrics
    for col in ["Display_name", "H_index", "I10_index", "Works_count", "Total_citations"]:
        merged[col] = out_df[col]

    write_output(merged, out_path)


if __name__ == "__main__":
    main()
