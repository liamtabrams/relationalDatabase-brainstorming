#!/usr/bin/env python3
"""
Track B ingestion: San Francisco / Bay Area imports & exports.

Source: U.S. Census Bureau International Trade Time Series API (port-level, by HS
commodity) -- the "porths" datasets:
    https://api.census.gov/data/timeseries/intltrade/imports/porths
    https://api.census.gov/data/timeseries/intltrade/exports/porths

What this script does
---------------------
1. Loads the Census API key (CENSUS_API_KEY). Fails loudly if missing/blank.
2. Fetches variables.json + examples.json for BOTH endpoints and *confirms the
   real field names* rather than guessing (imports and exports use different
   commodity/value fields). The discovered field catalogs are saved as CSVs.
3. Verifies the San Francisco, CA port code (default 2809) against the API's own
   data before pulling.
4. Pulls monthly data for the SF port, both imports and exports, at HS 4-digit
   granularity, across the last 5 full calendar years, including value, shipping
   weight, trading-partner country, and mode-of-transportation fields wherever
   the endpoint provides them.
5. Saves:
       raw_csv/imports_by_month_hs_country.csv
       raw_csv/exports_by_month_hs_country.csv
       raw_csv/commodities.csv   (HS code -> description, deduped from the data)
       raw_csv/countries.csv     (country code -> name, deduped from the data)
       raw_csv/imports_variables.csv / exports_variables.csv  (field catalogs)
       raw_csv/_ingest_summary.json  (machine-readable summary for compare step)
6. Prints a final summary: total rows, actual date range retrieved, distinct HS
   codes and countries, and every month that came back empty or errored.

This track is fully self-contained: deleting the sibling data-transit/ folder has
no effect on it, and vice versa.

Run:
    python3 data-trade/pull_trade.py            # last 5 full years, HS4, SF port
    python3 data-trade/pull_trade.py --dry-run  # show the plan, make no calls
    python3 data-trade/pull_trade.py --help     # all options
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
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
SETTINGS_LOCAL = REPO_ROOT / ".claude" / "settings.local.json"

IMPORTS_BASE = "https://api.census.gov/data/timeseries/intltrade/imports/porths"
EXPORTS_BASE = "https://api.census.gov/data/timeseries/intltrade/exports/porths"

DEFAULT_SF_PORT = "2809"          # San Francisco, CA (verified at runtime)
DEFAULT_COMM_LVL = "HS4"          # HS 4-digit keeps rows in the tens of thousands
DEFAULT_DELAY_S = 0.6             # polite delay between month requests
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5

# Fields we *want*, per endpoint. Anything not actually present in variables.json
# is dropped automatically, so a schema change never turns a request into a 400.
COUNTRY_FIELDS = ["CTY_CODE", "CTY_NAME"]
PORT_FIELDS = ["PORT", "PORT_NAME"]
TIME_FIELDS = ["YEAR", "MONTH"]

IMPORTS_WISHLIST = [
    "I_COMMODITY", "I_COMMODITY_LDESC", "I_COMMODITY_SDESC",   # commodity
    "GEN_VAL_MO", "CON_VAL_MO", "GEN_CIF_MO",                  # value
    "GEN_QY1_MO", "GEN_QY2_MO", "UNIT_QY1", "UNIT_QY2",        # quantity
    "AIR_VAL_MO", "AIR_WGT_MO",                                # mode: air
    "VES_VAL_MO", "VES_WGT_MO",                                # mode: vessel
    "CNT_VAL_MO", "CNT_WGT_MO",                                # containerized
]
EXPORTS_WISHLIST = [
    "E_COMMODITY", "E_COMMODITY_LDESC", "E_COMMODITY_SDESC",   # commodity
    "ALL_VAL_MO",                                              # value
    "QTY_1_MO", "QTY_2_MO", "UNIT_QY1", "UNIT_QY2",           # quantity
    "AIR_VAL_MO", "AIR_WGT_MO",                                # mode: air
    "VES_VAL_MO", "VES_WGT_MO",                                # mode: vessel
    "CNT_VAL_MO", "CNT_WGT_MO",                                # containerized
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MissingKeyError(RuntimeError):
    """Raised when CENSUS_API_KEY is missing or blank."""


class CensusAuthError(RuntimeError):
    """Raised when the API reports an invalid/unauthorized key."""


# --------------------------------------------------------------------------- #
# Key loading (env first, then .claude/settings.local.json, else fail loud)
# --------------------------------------------------------------------------- #
def load_required_key(name: str) -> str:
    """Return a non-empty API key or raise MissingKeyError.

    Resolution order:
      1. os.environ[name]  (this is how Claude Code injects the settings.local
         env block, and the required path per the project brief)
      2. .claude/settings.local.json -> env -> name  (so a plain `python3` run
         from a checkout also works)
    An empty string is treated as missing -- never a silent empty-string fallback.
    """
    val = os.environ.get(name, "")
    source = "environment"
    if not val.strip() and SETTINGS_LOCAL.is_file():
        try:
            data = json.loads(SETTINGS_LOCAL.read_text())
            val = str(data.get("env", {}).get(name, "") or "")
            source = str(SETTINGS_LOCAL)
        except (OSError, ValueError) as exc:
            raise MissingKeyError(
                f"Could not read {SETTINGS_LOCAL} while looking for {name}: {exc}"
            ) from exc
    if not val.strip():
        raise MissingKeyError(
            f"\n{name} is not set (or is blank).\n"
            f"  - Set it in your environment:  export {name}=your_key_here\n"
            f"  - or fill it into: {SETTINGS_LOCAL}\n"
            f"    (the \"env\" block there: {{\"{name}\": \"your_key_here\"}})\n"
            f"Get a free Census key at https://api.census.gov/data/key_signup.html\n"
        )
    print(f"[key] {name} loaded from {source}.")
    return val.strip()


# --------------------------------------------------------------------------- #
# HTTP with retry / backoff
# --------------------------------------------------------------------------- #
def request_with_retry(url: str, params: dict, *, label: str) -> requests.Response:
    """GET with exponential backoff. Raises on auth errors and on final failure."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"[retry] {label}: network error ({exc}); "
                  f"attempt {attempt}/{MAX_RETRIES}, waiting {wait}s")
            time.sleep(wait)
            continue

        # Detect an invalid-key response and fail loudly (do not treat as "empty").
        body_head = (resp.text or "")[:400].lower()
        if resp.status_code in (401, 403) or "invalid key" in body_head or \
                "a valid key" in body_head or "valid api key" in body_head:
            raise CensusAuthError(
                f"{label}: Census API rejected the key "
                f"(HTTP {resp.status_code}). Response head:\n{resp.text[:400]}"
            )

        if resp.status_code == 429:  # rate limited -> back off and retry
            wait = 2 ** attempt
            print(f"[retry] {label}: HTTP 429 rate limited; "
                  f"attempt {attempt}/{MAX_RETRIES}, waiting {wait}s")
            time.sleep(wait)
            continue

        if 500 <= resp.status_code < 600:  # transient server error -> retry
            wait = 2 ** attempt
            print(f"[retry] {label}: HTTP {resp.status_code}; "
                  f"attempt {attempt}/{MAX_RETRIES}, waiting {wait}s")
            time.sleep(wait)
            continue

        return resp

    raise RuntimeError(f"{label}: giving up after {MAX_RETRIES} attempts "
                       f"(last error: {last_exc})")


# --------------------------------------------------------------------------- #
# Metadata discovery
# --------------------------------------------------------------------------- #
def fetch_variables(base: str, label: str) -> pd.DataFrame:
    """Fetch variables.json and return a tidy DataFrame of the field catalog."""
    resp = request_with_retry(base + "/variables.json", {}, label=f"{label} variables.json")
    if resp.status_code != 200:
        print(f"[warn] {label}: variables.json returned HTTP {resp.status_code}; "
              f"proceeding with the built-in wishlist only.")
        return pd.DataFrame(columns=["name", "label", "predicateType", "required"])
    variables = resp.json().get("variables", {})
    rows = []
    for name, meta in variables.items():
        rows.append({
            "name": name,
            "label": meta.get("label", ""),
            "predicateType": meta.get("predicateType", ""),
            "required": str(meta.get("required", "")),
            "group": meta.get("group", ""),
        })
    df = pd.DataFrame(rows).sort_values("name").reset_index(drop=True)
    print(f"[meta] {label}: {len(df)} variables available.")
    return df


def confirm_examples(base: str, label: str) -> None:
    """Fetch examples.json purely to confirm the endpoint is reachable / shaped
    as expected. Non-fatal."""
    try:
        resp = request_with_retry(base + "/examples.json", {}, label=f"{label} examples.json")
        if resp.status_code == 200:
            print(f"[meta] {label}: examples.json reachable "
                  f"({len(resp.text)} bytes of usage examples).")
        else:
            print(f"[meta] {label}: examples.json HTTP {resp.status_code} (non-fatal).")
    except Exception as exc:  # noqa: BLE001 - purely informational
        print(f"[meta] {label}: examples.json check skipped ({exc}).")


def select_fields(available: set[str], wishlist: list[str]) -> list[str]:
    """Intersect wishlist with what the endpoint actually exposes, order-preserving,
    de-duplicated. This is the 'confirm, don't guess' step."""
    seen: set[str] = set()
    chosen: list[str] = []
    for f in wishlist:
        if f in seen:
            continue
        if not available or f in available:
            chosen.append(f)
            seen.add(f)
    return chosen


# --------------------------------------------------------------------------- #
# Port verification
# --------------------------------------------------------------------------- #
def verify_port(base: str, key: str, port: str, comm_lvl: str,
                probe_month: str, label: str) -> str | None:
    """Query one month for PORT/PORT_NAME filtered to `port`; return the reported
    port name if found. Best-effort -- logs and returns None on failure."""
    params = {
        "get": "PORT,PORT_NAME",
        "PORT": port,
        "COMM_LVL": comm_lvl,
        "time": probe_month,
        "key": key,
    }
    try:
        resp = request_with_retry(base, params, label=f"{label} port-verify")
    except CensusAuthError:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"[port] {label}: verification call failed ({exc}); "
              f"continuing with configured port {port}.")
        return None
    if resp.status_code != 200:
        print(f"[port] {label}: verification returned HTTP {resp.status_code} for "
              f"{probe_month}; continuing with configured port {port}.")
        return None
    try:
        rows = resp.json()
    except ValueError:
        print(f"[port] {label}: verification returned no JSON; continuing.")
        return None
    if not rows or len(rows) < 2:
        print(f"[port] {label}: no rows for port {port} in {probe_month} "
              f"(month may simply have no data); continuing.")
        return None
    header, first = rows[0], rows[1]
    rec = dict(zip(header, first))
    name = rec.get("PORT_NAME", "").strip()
    print(f"[port] {label}: port {port} -> \"{name}\".")
    return name


# --------------------------------------------------------------------------- #
# Monthly pull
# --------------------------------------------------------------------------- #
def month_iter(start_year: int, end_year: int):
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            yield y, m


def pull_endpoint(base: str, key: str, fields: list[str], port: str,
                  comm_lvl: str, months: list[tuple[int, int]], delay: float,
                  label: str) -> tuple[pd.DataFrame, list[dict]]:
    """Pull all requested months for one endpoint. Returns (dataframe, issues)."""
    frames: list[pd.DataFrame] = []
    issues: list[dict] = []
    get_list = ",".join(fields)
    total = len(months)
    for i, (y, m) in enumerate(months, start=1):
        tp = f"{y}-{m:02d}"
        params = {
            "get": get_list,
            "PORT": port,
            "COMM_LVL": comm_lvl,
            "time": tp,
            "key": key,
        }
        try:
            resp = request_with_retry(base, params, label=f"{label} {tp}")
        except CensusAuthError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[data] {label} {tp} [{i}/{total}]: ERROR {exc}")
            issues.append({"month": tp, "status": "error", "detail": str(exc)})
            continue

        if resp.status_code == 204 or not resp.text.strip():
            print(f"[data] {label} {tp} [{i}/{total}]: empty (no data).")
            issues.append({"month": tp, "status": "empty", "detail": "HTTP 204/empty body"})
            time.sleep(delay)
            continue

        if resp.status_code != 200:
            head = resp.text[:200].replace("\n", " ")
            print(f"[data] {label} {tp} [{i}/{total}]: HTTP {resp.status_code} -> {head}")
            issues.append({"month": tp, "status": f"http_{resp.status_code}", "detail": head})
            time.sleep(delay)
            continue

        try:
            rows = resp.json()
        except ValueError:
            head = resp.text[:200].replace("\n", " ")
            print(f"[data] {label} {tp} [{i}/{total}]: non-JSON body -> {head}")
            issues.append({"month": tp, "status": "bad_json", "detail": head})
            time.sleep(delay)
            continue

        if not rows or len(rows) < 2:
            print(f"[data] {label} {tp} [{i}/{total}]: header only (no rows).")
            issues.append({"month": tp, "status": "empty", "detail": "header only"})
            time.sleep(delay)
            continue

        header, data = rows[0], rows[1:]
        df = pd.DataFrame(data, columns=header)
        # The Census API appends the variables you FILTER on (e.g. PORT, COMM_LVL)
        # to the output columns. Since PORT is also in our `get` list, the response
        # carries a duplicate PORT column; drop any such duplicate labels (keeping
        # the first) so the frames have a unique column index -- otherwise a later
        # pd.concat across imports/exports fails with InvalidIndexError.
        df = df.loc[:, ~df.columns.duplicated()]
        df["time"] = tp
        frames.append(df)
        print(f"[data] {label} {tp} [{i}/{total}]: {len(df):,} rows.")
        time.sleep(delay)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame()
    return combined, issues


# --------------------------------------------------------------------------- #
# Derived lookups
# --------------------------------------------------------------------------- #
def build_lookup(frames: list[pd.DataFrame], code_candidates: list[str],
                 name_candidates: list[str]) -> pd.DataFrame:
    """Build a deduped code->description lookup from one or more dataframes."""
    parts = []
    for df in frames:
        if df.empty:
            continue
        code_col = next((c for c in code_candidates if c in df.columns), None)
        name_col = next((c for c in name_candidates if c in df.columns), None)
        if code_col is None:
            continue
        cols = [code_col] + ([name_col] if name_col else [])
        sub = df[cols].copy()
        sub.columns = ["code"] + (["description"] if name_col else [])
        parts.append(sub)
    if not parts:
        return pd.DataFrame(columns=["code", "description"])
    out = pd.concat(parts, ignore_index=True).drop_duplicates()
    out = out.dropna(subset=["code"]).sort_values("code").reset_index(drop=True)
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def load_existing_csv(path: Path) -> pd.DataFrame:
    """Load a previously-saved data CSV, dropping any echoed-predicate duplicate
    columns. pandas renames duplicate headers on read (PORT -> PORT.1), so we drop
    any 'NAME.<n>' column whose base 'NAME' is also present."""
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, low_memory=False)
    drop = [c for c in df.columns
            if "." in c and c.rsplit(".", 1)[-1].isdigit()
            and c.rsplit(".", 1)[0] in df.columns]
    if drop:
        print(f"[clean] {path.name}: dropped echoed duplicate column(s) {drop}")
        df = df.drop(columns=drop)
    return df


def finalize(imports_df: pd.DataFrame, exports_df: pd.DataFrame,
             imports_issues: list[dict], exports_issues: list[dict],
             args: argparse.Namespace) -> int:
    """Write the (cleaned) data CSVs, derived lookups, and the summary. Shared by
    a normal pull and by --summary-only, so no network is required here."""
    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    imports_path = RAW_CSV_DIR / "imports_by_month_hs_country.csv"
    exports_path = RAW_CSV_DIR / "exports_by_month_hs_country.csv"
    if not imports_df.empty:
        imports_df.to_csv(imports_path, index=False)
    if not exports_df.empty:
        exports_df.to_csv(exports_path, index=False)

    commodities = build_lookup(
        [imports_df, exports_df],
        code_candidates=["I_COMMODITY", "E_COMMODITY"],
        name_candidates=["I_COMMODITY_LDESC", "E_COMMODITY_LDESC",
                         "I_COMMODITY_SDESC", "E_COMMODITY_SDESC"],
    )
    countries = build_lookup(
        [imports_df, exports_df],
        code_candidates=["CTY_CODE"],
        name_candidates=["CTY_NAME"],
    )
    commodities.to_csv(RAW_CSV_DIR / "commodities.csv", index=False)
    countries.to_csv(RAW_CSV_DIR / "countries.csv", index=False)

    def date_range(df: pd.DataFrame) -> tuple[str, str] | None:
        if df.empty or "time" not in df.columns:
            return None
        vals = sorted(df["time"].dropna().unique())
        return (vals[0], vals[-1]) if vals else None

    def distinct_union(dfs: list[pd.DataFrame], col: str) -> int:
        """Count distinct non-null values of `col` across frames, without concat
        (so it never trips over differing columns or duplicate labels)."""
        vals: set = set()
        for df in dfs:
            if df.empty or col not in df.columns:
                continue
            s = df[col]
            if isinstance(s, pd.DataFrame):  # guard against a stray duplicate label
                s = s.iloc[:, 0]
            vals |= set(s.dropna().unique())
        return len(vals)

    imp_range = date_range(imports_df)
    exp_range = date_range(exports_df)
    all_times = []
    for df in (imports_df, exports_df):
        if not df.empty and "time" in df.columns:
            all_times += list(df["time"].dropna().unique())
    overall_range = (min(all_times), max(all_times)) if all_times else None
    requested_months = (args.end_year - args.start_year + 1) * 12

    summary = {
        "track": "trade",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "port": args.port,
        "comm_level": args.comm_level,
        "requested_months": requested_months,
        "imports_rows": int(len(imports_df)),
        "exports_rows": int(len(exports_df)),
        "total_rows": int(len(imports_df) + len(exports_df)),
        "imports_date_range": imp_range,
        "exports_date_range": exp_range,
        "date_range": overall_range,
        "distinct_hs_codes": distinct_union([imports_df], "I_COMMODITY")
                             + distinct_union([exports_df], "E_COMMODITY"),
        "distinct_countries": distinct_union([imports_df, exports_df], "CTY_CODE"),
        "imports_issues": imports_issues,
        "exports_issues": exports_issues,
        "files": sorted(p.name for p in RAW_CSV_DIR.glob("*.csv")),
    }
    (RAW_CSV_DIR / "_ingest_summary.json").write_text(json.dumps(summary, indent=2))

    print("\n" + "=" * 70)
    print("TRACK B SUMMARY")
    print("=" * 70)
    print(f"Imports rows        : {summary['imports_rows']:,}  -> {imports_path.name}")
    print(f"Exports rows        : {summary['exports_rows']:,}  -> {exports_path.name}")
    print(f"Total rows          : {summary['total_rows']:,}")
    print(f"Date range (overall): {overall_range}")
    print(f"Distinct HS codes   : {summary['distinct_hs_codes']:,}")
    print(f"Distinct countries  : {summary['distinct_countries']:,}")
    empties = [x['month'] for x in imports_issues + exports_issues if x['status'] == 'empty']
    errors = [(x['month'], x['status']) for x in imports_issues + exports_issues if x['status'] != 'empty']
    print(f"Empty months        : {len(empties)}"
          + (f"  {empties}" if empties else ""))
    print(f"Errored months      : {len(errors)}"
          + (f"  {errors}" if errors else ""))
    if imports_df.empty and exports_df.empty:
        print("\n[!] No data present. Nothing fabricated -- see the issues above.")
        return 2
    print("\nDone.")
    return 0


def parse_args() -> argparse.Namespace:
    now = datetime.now()
    last_full_year = now.year - 1
    p = argparse.ArgumentParser(description="Pull SF-port imports & exports from the Census intltrade API.")
    p.add_argument("--start-year", type=int, default=last_full_year - 4,
                   help="First calendar year (default: last full year - 4).")
    p.add_argument("--end-year", type=int, default=last_full_year,
                   help="Last calendar year (default: last full year).")
    p.add_argument("--port", default=DEFAULT_SF_PORT, help="Census port code (default 2809 = San Francisco, CA).")
    p.add_argument("--comm-level", default=DEFAULT_COMM_LVL, help="HS commodity level: HS2/HS4/HS6/HS10 (default HS4).")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY_S, help="Seconds between month requests (default 0.6).")
    p.add_argument("--dry-run", action="store_true", help="Print the plan and exit without any API calls.")
    p.add_argument("--summary-only", action="store_true",
                   help="Skip all API calls; rebuild lookups + summary from the CSVs "
                        "already in raw_csv/ (also strips any duplicate columns).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    months = list(month_iter(args.start_year, args.end_year))

    print("=" * 70)
    print("Track B - Census International Trade (SF port imports & exports)")
    print("=" * 70)
    print(f"Years         : {args.start_year}..{args.end_year} ({len(months)} months)")
    print(f"Port          : {args.port}")
    print(f"Commodity lvl : {args.comm_level}")
    print(f"Output dir    : {RAW_CSV_DIR}")

    if args.dry_run:
        print("\n[dry-run] No API calls will be made.")
        print(f"[dry-run] Imports endpoint : {IMPORTS_BASE}")
        print(f"[dry-run] Exports endpoint : {EXPORTS_BASE}")
        print(f"[dry-run] First 3 months   : {months[:3]}  ...  last: {months[-1]}")
        print(f"[dry-run] Imports wishlist : {IMPORTS_WISHLIST}")
        print(f"[dry-run] Exports wishlist : {EXPORTS_WISHLIST}")
        print("[dry-run] At runtime, fields are intersected with each endpoint's "
              "variables.json before any data call.")
        return 0

    # 0) Summary-only: rebuild from existing CSVs, no network, no key needed --
    if args.summary_only:
        print("\n[summary-only] Skipping all API calls; rebuilding from existing CSVs.")
        imports_df = load_existing_csv(RAW_CSV_DIR / "imports_by_month_hs_country.csv")
        exports_df = load_existing_csv(RAW_CSV_DIR / "exports_by_month_hs_country.csv")
        if imports_df.empty and exports_df.empty:
            print(f"[summary-only] No existing data CSVs found in {RAW_CSV_DIR}. "
                  f"Run a normal pull first.")
            return 2
        return finalize(imports_df, exports_df, [], [], args)

    # 1) Key (fail loud) -----------------------------------------------------
    key = load_required_key("CENSUS_API_KEY")

    # 2) Metadata discovery --------------------------------------------------
    imp_vars = fetch_variables(IMPORTS_BASE, "imports")
    exp_vars = fetch_variables(EXPORTS_BASE, "exports")
    imp_vars.to_csv(RAW_CSV_DIR / "imports_variables.csv", index=False)
    exp_vars.to_csv(RAW_CSV_DIR / "exports_variables.csv", index=False)
    confirm_examples(IMPORTS_BASE, "imports")
    confirm_examples(EXPORTS_BASE, "exports")

    imp_available = set(imp_vars["name"]) if not imp_vars.empty else set()
    exp_available = set(exp_vars["name"]) if not exp_vars.empty else set()

    imp_fields = select_fields(imp_available, IMPORTS_WISHLIST + COUNTRY_FIELDS + PORT_FIELDS + TIME_FIELDS)
    exp_fields = select_fields(exp_available, EXPORTS_WISHLIST + COUNTRY_FIELDS + PORT_FIELDS + TIME_FIELDS)
    print(f"[fields] imports -> {imp_fields}")
    print(f"[fields] exports -> {exp_fields}")

    # 3) Verify port (best-effort) ------------------------------------------
    probe = f"{args.end_year}-01"
    verify_port(IMPORTS_BASE, key, args.port, args.comm_level, probe, "imports")

    # 4) Pull ----------------------------------------------------------------
    print("\n--- Imports ---")
    imports_df, imports_issues = pull_endpoint(
        IMPORTS_BASE, key, imp_fields, args.port, args.comm_level, months, args.delay, "imports")
    print("\n--- Exports ---")
    exports_df, exports_issues = pull_endpoint(
        EXPORTS_BASE, key, exp_fields, args.port, args.comm_level, months, args.delay, "exports")

    # 5+6) Save lookups + write/print summary --------------------------------
    return finalize(imports_df, exports_df, imports_issues, exports_issues, args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MissingKeyError as exc:
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        sys.exit(3)
    except CensusAuthError as exc:
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        sys.exit(4)
