#!/usr/bin/env python3
"""
Side-by-side comparison of the two candidate datasets.

Reads whatever each track produced in its raw_csv/ folder and prints a table of:
total files/tables, total rows, total disk size, and date range covered -- so the
team can judge which dataset gives richer material for 7 weeks of SQL + Metabase.

This script is intentionally decoupled from the two ingestion scripts: it only
reads their output folders. If a track's folder is missing (e.g. you deleted
data-trade/ entirely), it is simply reported as "not present" -- the other track
still compares fine.

Run it after either or both ingestion scripts:
    python3 compare_tracks.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent
TRACKS = {
    "transit (GTFS)": REPO_ROOT / "data-transit" / "raw_csv",
    "trade (Census)": REPO_ROOT / "data-trade" / "raw_csv",
}


def human_bytes(n: int) -> str:
    x = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024 or unit == "TB":
            return f"{x:,.1f} {unit}"
        x /= 1024
    return f"{n} B"


def scan_track(raw_dir: Path) -> dict | None:
    if not raw_dir.is_dir():
        return None
    csvs = sorted(raw_dir.glob("*.csv"))
    total_rows = 0
    countable = True
    for c in csvs:
        try:
            # Count rows without holding the whole frame in memory.
            with c.open("r", encoding="utf-8", errors="replace") as fh:
                n = sum(1 for _ in fh)
            total_rows += max(0, n - 1)  # minus header
        except Exception:  # noqa: BLE001
            countable = False
    total_bytes = sum(p.stat().st_size for p in raw_dir.glob("*") if p.is_file())

    # Prefer the machine-readable summary each ingestion script writes.
    date_range = None
    summary_path = raw_dir / "_ingest_summary.json"
    if summary_path.is_file():
        try:
            s = json.loads(summary_path.read_text())
            dr = s.get("date_range")
            if dr:
                date_range = f"{dr[0]} .. {dr[1]}"
        except Exception:  # noqa: BLE001
            pass

    return {
        "n_files": len(csvs),
        "total_rows": total_rows if countable else None,
        "total_bytes": total_bytes,
        "date_range": date_range or "n/a",
        "files": [c.name for c in csvs],
    }


def main() -> int:
    print("=" * 78)
    print("DATASET COMPARISON  -  transit (GTFS)  vs  trade (Census intltrade)")
    print("=" * 78)

    results = {name: scan_track(path) for name, path in TRACKS.items()}

    rows = [
        ("Tables / CSV files", lambda r: f"{r['n_files']:,}"),
        ("Total rows",         lambda r: f"{r['total_rows']:,}" if r['total_rows'] is not None else "n/a"),
        ("Total disk size",    lambda r: human_bytes(r['total_bytes'])),
        ("Date range covered", lambda r: r['date_range']),
    ]

    col_names = list(TRACKS.keys())
    w_label = 20
    w_col = 34
    header = f"{'Metric':<{w_label}}" + "".join(f"{c:<{w_col}}" for c in col_names)
    print(header)
    print("-" * len(header))
    for label, fn in rows:
        line = f"{label:<{w_label}}"
        for name in col_names:
            r = results[name]
            line += f"{(fn(r) if r else 'not present'):<{w_col}}"
        print(line)

    print("\nPer-track files:")
    for name in col_names:
        r = results[name]
        print(f"\n  {name}:")
        if not r:
            print("    (folder not present)")
            continue
        if not r["files"]:
            print("    (no CSVs yet -- run the ingestion script)")
        for f in r["files"]:
            print(f"    - {f}")

    present = [r for r in results.values() if r]
    if len(present) < 2 or any(r["n_files"] == 0 for r in present):
        print("\nNote: run both ingestion scripts first for a full comparison:")
        print("  python3 data-transit/pull_transit.py")
        print("  python3 data-trade/pull_trade.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
