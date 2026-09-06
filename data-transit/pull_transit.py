#!/usr/bin/env python3
"""
Track A ingestion: Bay Area public transit schedules (GTFS).

Primary source: 511.org SF Bay Open Data Portal -- a single consolidated Regional
GTFS feed (operator_id=RG) covering 20+ agencies (BART, AC Transit, Caltrain,
SFMTA, VTA, Golden Gate Transit, ...):
    http://api.511.org/transit/datafeeds?api_key=API_KEY&operator_id=RG
    http://api.511.org/transit/gtfsoperators?api_key=API_KEY   (operator list)

What this script does
---------------------
1. Loads the 511 API token (API_511_TOKEN).
      - If present: pulls the Regional (RG) consolidated feed + operator list.
      - If MISSING/BLANK: prints a loud, explicit warning and falls back to
        downloading a couple of individual *public* agency feeds that need no key
        (BART, and others if reachable), so there is still something to inspect.
        (This graceful fallback is specific to Track A by design; the trade track
        has no such fallback and hard-fails on a missing key.)
2. Downloads the zip(s), unzips, loads every .txt file with pandas, and writes
   each one out as its own CSV in raw_csv/ (stops.csv, routes.csv, trips.csv,
   stop_times.csv, calendar.csv, calendar_dates.csv, shapes.csv, agency.csv, plus
   any GTFS+ extras such as fare/direction files).
3. Prints a final summary: row count + columns per file, distinct agency/route
   counts, and the min/max service date across calendar.txt / calendar_dates.txt.

This track is fully self-contained: deleting the sibling data-trade/ folder has no
effect on it, and vice versa.

Run:
    python3 data-transit/pull_transit.py            # RG feed if token set, else BART
    python3 data-transit/pull_transit.py --dry-run  # show the plan, no downloads
    python3 data-transit/pull_transit.py --help
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --------------------------------------------------------------------------- #
# Paths & constants
# --------------------------------------------------------------------------- #
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
RAW_CSV_DIR = SCRIPT_DIR / "raw_csv"
DOWNLOAD_DIR = SCRIPT_DIR / "_download"
SETTINGS_LOCAL = REPO_ROOT / ".claude" / "settings.local.json"

API_BASE = "http://api.511.org/transit"
REGIONAL_OPERATOR = "RG"

# No-key public static feeds used only when API_511_TOKEN is absent.
FALLBACK_FEEDS = {
    "bart": "http://www.bart.gov/dev/schedules/google_transit.zip",
    "caltrain": "http://data.trilliumtransit.com/gtfs/caltrain-ca-us/caltrain-ca-us.zip",
}

REQUEST_TIMEOUT = 120
MAX_RETRIES = 5


class MissingKeyError(RuntimeError):
    """Raised only if fallback is disabled and the token is missing."""


# --------------------------------------------------------------------------- #
# Key loading (env first, then .claude/settings.local.json)
# --------------------------------------------------------------------------- #
def load_token(name: str = "API_511_TOKEN") -> str:
    """Return the token from env or .claude/settings.local.json, or '' if unset.

    A blank token is returned as '' (not an error here) so the caller can decide
    to fall back to public feeds -- but the missing-token path is always announced
    loudly, never silently skipped.
    """
    val = os.environ.get(name, "")
    if not val.strip() and SETTINGS_LOCAL.is_file():
        try:
            data = json.loads(SETTINGS_LOCAL.read_text())
            val = str(data.get("env", {}).get(name, "") or "")
        except (OSError, ValueError) as exc:
            print(f"[warn] Could not read {SETTINGS_LOCAL}: {exc}")
            val = ""
    return val.strip()


# --------------------------------------------------------------------------- #
# HTTP with retry / backoff
# --------------------------------------------------------------------------- #
def request_with_retry(url: str, params: dict | None, *, label: str,
                       stream: bool = False) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params or {}, timeout=REQUEST_TIMEOUT, stream=stream)
        except requests.RequestException as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"[retry] {label}: network error ({exc}); "
                  f"attempt {attempt}/{MAX_RETRIES}, waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = 2 ** attempt
            print(f"[retry] {label}: HTTP {resp.status_code}; "
                  f"attempt {attempt}/{MAX_RETRIES}, waiting {wait}s")
            time.sleep(wait)
            continue
        return resp
    raise RuntimeError(f"{label}: giving up after {MAX_RETRIES} attempts "
                       f"(last error: {last_exc})")


# --------------------------------------------------------------------------- #
# Operator list
# --------------------------------------------------------------------------- #
def fetch_operators(token: str) -> pd.DataFrame | None:
    """Fetch gtfsoperators and save as operators.csv. Returns df or None."""
    resp = request_with_retry(
        f"{API_BASE}/gtfsoperators",
        {"api_key": token, "Format": "json"},
        label="gtfsoperators",
    )
    if resp.status_code == 401 or resp.status_code == 403:
        raise MissingKeyError(
            f"511 rejected the token (HTTP {resp.status_code}). "
            f"Check API_511_TOKEN. Response head: {resp.text[:200]}"
        )
    if resp.status_code != 200:
        print(f"[warn] gtfsoperators returned HTTP {resp.status_code}; skipping operator list.")
        return None
    # 511 JSON responses are UTF-8 with a BOM; decode defensively.
    text = resp.content.decode("utf-8-sig", errors="replace")
    try:
        data = json.loads(text)
    except ValueError as exc:
        print(f"[warn] gtfsoperators: could not parse JSON ({exc}); skipping.")
        return None
    df = pd.json_normalize(data)
    out = RAW_CSV_DIR / "operators.csv"
    df.to_csv(out, index=False)
    print(f"[operators] {len(df)} operators -> {out.name}")
    return df


# --------------------------------------------------------------------------- #
# GTFS zip download + extraction
# --------------------------------------------------------------------------- #
def download_zip(url: str, params: dict | None, label: str) -> bytes | None:
    resp = request_with_retry(url, params, label=label, stream=False)
    if resp.status_code in (401, 403):
        raise MissingKeyError(
            f"{label}: access denied (HTTP {resp.status_code}). Response head: {resp.text[:200]}"
        )
    if resp.status_code != 200:
        print(f"[warn] {label}: HTTP {resp.status_code}; skipping this feed.")
        return None
    content = resp.content
    ctype = resp.headers.get("Content-Type", "")
    if not (content[:2] == b"PK" or "zip" in ctype.lower()):
        head = content[:200].decode("utf-8", errors="replace").replace("\n", " ")
        print(f"[warn] {label}: response does not look like a zip "
              f"(Content-Type={ctype!r}); head: {head}")
        return None
    return content


def extract_feed(zip_bytes: bytes, prefix: str, label: str) -> list[dict]:
    """Load every .txt in the zip with pandas and write each as a CSV.

    `prefix` is prepended to output filenames (e.g. 'bart_') so multiple fallback
    feeds don't collide; for the single Regional feed it is ''.
    Returns a list of per-file records for the summary.
    """
    records: list[dict] = []
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Keep the raw zip too, for reproducibility/inspection.
    raw_zip = DOWNLOAD_DIR / f"{prefix or 'regional_'}gtfs.zip"
    raw_zip.write_bytes(zip_bytes)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if not txt_names:
            print(f"[warn] {label}: zip contained no .txt files "
                  f"(members: {zf.namelist()[:10]}).")
        for name in sorted(txt_names):
            base = Path(name).name  # flatten any nested paths
            stem = base[:-4]        # strip .txt
            out_name = f"{prefix}{stem}.csv"
            out_path = RAW_CSV_DIR / out_name
            try:
                with zf.open(name) as fh:
                    # GTFS is text/csv; keep everything as string to avoid mangling
                    # zero-padded IDs and YYYYMMDD dates.
                    df = pd.read_csv(fh, dtype=str, low_memory=False)
            except Exception as exc:  # noqa: BLE001 - one bad file shouldn't kill the run
                print(f"[warn] {label}: could not parse {name} ({exc}); skipping.")
                records.append({"file": out_name, "source_txt": name,
                                "rows": None, "columns": [], "error": str(exc)})
                continue
            df.to_csv(out_path, index=False)
            records.append({"file": out_name, "source_txt": name,
                            "rows": int(len(df)), "columns": list(df.columns),
                            "error": None})
            print(f"[extract] {out_name}: {len(df):,} rows, {len(df.columns)} cols")
    return records


# --------------------------------------------------------------------------- #
# Summary helpers
# --------------------------------------------------------------------------- #
def _read_csv(name: str) -> pd.DataFrame | None:
    p = RAW_CSV_DIR / name
    if not p.is_file():
        return None
    try:
        return pd.read_csv(p, dtype=str, low_memory=False)
    except Exception:  # noqa: BLE001
        return None


def compute_service_dates(prefixes: list[str]) -> tuple[str | None, str | None]:
    """Min/max service date across calendar(.csv) and calendar_dates(.csv)."""
    dates: list[str] = []
    for pref in prefixes:
        cal = _read_csv(f"{pref}calendar.csv")
        if cal is not None:
            for col in ("start_date", "end_date"):
                if col in cal.columns:
                    dates += [d for d in cal[col].dropna().tolist() if str(d).isdigit()]
        cald = _read_csv(f"{pref}calendar_dates.csv")
        if cald is not None and "date" in cald.columns:
            dates += [d for d in cald["date"].dropna().tolist() if str(d).isdigit()]
    if not dates:
        return None, None
    return min(dates), max(dates)


def count_distinct(prefixes: list[str], filename: str, col: str) -> int:
    total = set()
    for pref in prefixes:
        df = _read_csv(f"{pref}{filename}")
        if df is not None and col in df.columns:
            total |= set(df[col].dropna().tolist())
    return len(total)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull Bay Area GTFS (511 Regional, or public feeds as fallback).")
    p.add_argument("--dry-run", action="store_true", help="Print the plan and exit without downloading.")
    p.add_argument("--no-fallback", action="store_true",
                   help="Do NOT fall back to public feeds; hard-fail if the token is missing.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    token = load_token()

    print("=" * 70)
    print("Track A - Bay Area GTFS (511 Regional feed)")
    print("=" * 70)
    print(f"Output dir : {RAW_CSV_DIR}")
    print(f"Token      : {'present' if token else 'MISSING'}")

    if args.dry_run:
        print("\n[dry-run] No downloads will be made.")
        if token:
            print(f"[dry-run] Would fetch operator list: {API_BASE}/gtfsoperators")
            print(f"[dry-run] Would download Regional feed: {API_BASE}/datafeeds?operator_id={REGIONAL_OPERATOR}")
        else:
            print("[dry-run] Token missing -> would fall back to public feeds:")
            for name, url in FALLBACK_FEEDS.items():
                print(f"[dry-run]   - {name}: {url}")
        return 0

    all_records: list[dict] = []
    prefixes: list[str] = []
    mode = None
    errored_feeds: list[dict] = []

    if token:
        # ---- Primary path: 511 Regional consolidated feed ------------------
        mode = "regional-511"
        try:
            fetch_operators(token)
        except MissingKeyError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] operator list failed ({exc}); continuing to feed download.")
        zip_bytes = download_zip(
            f"{API_BASE}/datafeeds",
            {"api_key": token, "operator_id": REGIONAL_OPERATOR},
            label=f"datafeeds RG",
        )
        if zip_bytes:
            all_records += extract_feed(zip_bytes, prefix="", label="Regional")
            prefixes.append("")
        else:
            errored_feeds.append({"feed": "RG", "detail": "download failed / not a zip"})
    else:
        # ---- Fallback path: public agency feeds, no key --------------------
        print("\n" + "!" * 70)
        print("! API_511_TOKEN is MISSING or BLANK.")
        print("! Falling back to individual PUBLIC agency feeds (no key required).")
        print("! Set API_511_TOKEN in .claude/settings.local.json (or your env)")
        print("! to pull the full consolidated 20+ agency Regional feed instead.")
        print("!" * 70)
        if args.no_fallback:
            raise MissingKeyError(
                "API_511_TOKEN is missing/blank and --no-fallback was set. Aborting."
            )
        mode = "fallback-public"
        for name, url in FALLBACK_FEEDS.items():
            print(f"\n[fallback] {name}: {url}")
            try:
                zip_bytes = download_zip(url, None, label=f"{name} feed")
            except Exception as exc:  # noqa: BLE001
                print(f"[warn] {name}: {exc}")
                errored_feeds.append({"feed": name, "detail": str(exc)})
                continue
            if zip_bytes:
                all_records += extract_feed(zip_bytes, prefix=f"{name}_", label=name)
                prefixes.append(f"{name}_")
            else:
                errored_feeds.append({"feed": name, "detail": "download failed / not a zip"})

    # ---- Summary -----------------------------------------------------------
    ok_records = [r for r in all_records if r.get("rows") is not None]
    total_rows = sum(r["rows"] for r in ok_records)
    n_agencies = count_distinct(prefixes, "agency.csv", "agency_id") or \
        count_distinct(prefixes, "agency.csv", "agency_name")
    n_routes = count_distinct(prefixes, "routes.csv", "route_id")
    min_date, max_date = compute_service_dates(prefixes)

    summary = {
        "track": "transit",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": mode,
        "files": [r["file"] for r in ok_records],
        "total_files": len(ok_records),
        "total_rows": int(total_rows),
        "distinct_agencies": int(n_agencies),
        "distinct_routes": int(n_routes),
        "service_date_min": min_date,
        "service_date_max": max_date,
        "date_range": [min_date, max_date] if min_date else None,
        "errored_feeds": errored_feeds,
        "per_file": {r["file"]: {"rows": r["rows"], "n_columns": len(r["columns"])}
                     for r in ok_records},
    }
    (RAW_CSV_DIR / "_ingest_summary.json").write_text(json.dumps(summary, indent=2))

    print("\n" + "=" * 70)
    print("TRACK A SUMMARY")
    print("=" * 70)
    print(f"Mode              : {mode}")
    print(f"Files written     : {len(ok_records)}")
    for r in ok_records:
        print(f"  - {r['file']:<28} {r['rows']:>10,} rows  "
              f"[{', '.join(r['columns'][:6])}{'...' if len(r['columns']) > 6 else ''}]")
    print(f"Total rows        : {total_rows:,}")
    print(f"Distinct agencies : {n_agencies:,}")
    print(f"Distinct routes   : {n_routes:,}")
    print(f"Service dates      : {min_date} .. {max_date}")
    if errored_feeds:
        print(f"Errored feeds     : {errored_feeds}")
    if not ok_records:
        print("\n[!] No GTFS files were written. Nothing fabricated -- see warnings above.")
        return 2
    print("\nDone.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MissingKeyError as exc:
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        sys.exit(3)
