# relationalDatabase-brainstorming

Two independent, standalone data-ingestion tracks for evaluating candidate
datasets for a 7-week relational-database course project (PostgreSQL + Metabase).

Each track pulls as much real data as is reasonably possible, saves everything to
local CSVs, and prints a summary so we can compare **table structure, row counts,
and time coverage** before committing to one.

```
.
├── data-transit/          # Track A: Bay Area public transit (GTFS)
│   ├── pull_transit.py
│   └── raw_csv/           # <- CSV outputs land here (gitignored)
├── data-trade/            # Track B: SF / Bay Area imports & exports (Census)
│   ├── pull_trade.py
│   └── raw_csv/           # <- CSV outputs land here (gitignored)
├── compare_tracks.py      # side-by-side comparison of both tracks' output
├── requirements.txt
└── .claude/settings.local.json   # your API keys (gitignored, never committed)
```

The two tracks are **fully independent** — you can delete either folder without
affecting the other. `compare_tracks.py` only reads whatever output folders exist.

---

## 1. Setup

```bash
python3 -m venv .venv && source .venv/bin/activate     # optional
pip install -r requirements.txt                        # pandas + requests
```

## 2. API keys

Both tracks need a free API key. Put them in **`.claude/settings.local.json`**,
which is gitignored so the keys can never be committed:

```json
{
  "env": {
    "API_511_TOKEN": "your-511-token-here",
    "CENSUS_API_KEY": "your-census-key-here"
  }
}
```

- **511 transit token** (free): https://511.org/open-data/token
- **Census API key** (free): https://api.census.gov/data/key_signup.html

The scripts read the keys from `os.environ` first (this is how Claude Code
injects the `env` block above), and fall back to reading
`.claude/settings.local.json` directly — so `python3 data-trade/pull_trade.py`
works from a plain terminal too. You can also just export them:

```bash
export API_511_TOKEN=...
export CENSUS_API_KEY=...
```

A missing/blank **Census** key is a **hard, loud error** (the trade track has no
fallback). A missing **511** token is announced loudly and the transit track
falls back to public agency feeds (see below).

## 3. Run

```bash
python3 data-transit/pull_transit.py      # Track A: Bay Area GTFS
python3 data-trade/pull_trade.py          # Track B: SF-port trade
python3 compare_tracks.py                 # side-by-side comparison
```

Both accept `--dry-run` (print the plan, make no network calls) and `--help`.

---

## Track A — Bay Area public transit (GTFS)

Primary source: the 511.org **Regional** consolidated GTFS feed
(`operator_id=RG`), covering 20+ agencies (BART, AC Transit, Caltrain, SFMTA,
VTA, Golden Gate Transit, …).

- Fetches the operator list (`gtfsoperators`) → `raw_csv/operators.csv`.
- Downloads the RG zip, unzips it, loads every `.txt` with pandas, and writes
  each as its own CSV (`stops.csv`, `routes.csv`, `trips.csv`, `stop_times.csv`,
  `calendar.csv`, `calendar_dates.csv`, `shapes.csv`, `agency.csv`, plus any
  GTFS+ extras present in the feed).
- **Fallback (no token):** downloads a couple of individual **public** static
  feeds that need no key (BART, Caltrain), writing them with an agency prefix
  (`bart_stops.csv`, …). This is a deliberate Track-A convenience so there's
  always something to inspect.
- Summary: rows + columns per file, distinct agency/route counts, and the
  min/max service date across `calendar.txt` / `calendar_dates.txt`.

Options: `--no-fallback` (hard-fail instead of using public feeds), `--dry-run`.

## Track B — SF / Bay Area imports & exports (Census)

Primary source: the U.S. Census Bureau **International Trade Time Series API**,
port-level by HS commodity (`porths`):
`.../timeseries/intltrade/imports/porths` and `.../exports/porths`.

- **Confirms field names at runtime**: fetches `variables.json` (and checks
  `examples.json`) for *both* endpoints and intersects them with the desired
  field list, so it never guesses a field name (imports use `I_COMMODITY` /
  `GEN_VAL_MO`, exports use `E_COMMODITY` / `ALL_VAL_MO`, etc.). The discovered
  catalogs are saved as `imports_variables.csv` / `exports_variables.csv`.
- **Verifies the SF port code** (default `2809` = San Francisco, CA) against the
  API's own data before pulling.
- Pulls **monthly** data for the SF port, imports + exports, at **HS 4-digit**
  granularity, across the **last 5 full calendar years**, including value,
  shipping weight, trading-partner country, and mode-of-transportation fields
  wherever the endpoint provides them.
- Loops month-by-month with retry/backoff and a polite delay; logs every month
  that comes back empty or errored (never fabricates data).
- Outputs: `imports_by_month_hs_country.csv`, `exports_by_month_hs_country.csv`,
  `commodities.csv` (HS → description) and `countries.csv` (code → name), both
  deduped from the pulled data.
- Summary: total rows, actual date range retrieved, distinct HS codes and
  countries, and any empty/errored months.

Options: `--start-year`, `--end-year`, `--port`, `--comm-level` (HS2/HS4/HS6/
HS10), `--delay`, `--dry-run`, and `--summary-only` (skip all API calls and
rebuild the lookups + summary from the CSVs already in `raw_csv/`, also stripping
any duplicate columns — useful if a run downloaded everything but you just want
to regenerate the summary without re-pulling).

---

## Notes / caveats

- **These scripts are meant to run on your machine**, where the API keys live
  and outbound network access to `api.511.org`, `api.census.gov`, and
  `www.bart.gov` is open. (In restricted/sandboxed environments those hosts may
  be blocked by egress policy.)
- Raw CSV outputs and downloaded zips are **gitignored** — they're large and
  fully regenerable by re-running the scripts.
- If a call fails, the script skips it and records it in the summary rather than
  inventing rows.
