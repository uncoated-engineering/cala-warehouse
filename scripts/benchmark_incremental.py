#!/usr/bin/env python3
"""Full-refresh vs incremental load of stg_cala_entries on the committed seeds.

Four runs, same seeds:
  1. full refresh, bounded to the first half of the events (entries_as_of)
  2. incremental: picks up the second half via the per-id watermark
  3. full refresh, everything
  4. incremental again: nothing new, should touch zero rows

Reports rows in the table after each run, rows written, and the model's
execution time from dbt's run_results.json. Prints a Markdown table.
Run from the repo root: uv run python scripts/benchmark_incremental.py
"""
import json
import pathlib
import subprocess
import sys
import time

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "dbt"
DB = DBT_DIR / "cala_warehouse.duckdb"
MODEL = "stg_cala_entries"


def dbt(*args):
    cmd = ["uv", "run", "dbt", "run", "--select", MODEL, "-q", *args]
    t0 = time.perf_counter()
    subprocess.run(cmd, cwd=DBT_DIR, check=True)
    wall = time.perf_counter() - t0
    results = json.loads((DBT_DIR / "target" / "run_results.json").read_text())
    node = next(r for r in results["results"] if r["unique_id"].endswith(MODEL))
    return wall, node["execution_time"], node["adapter_response"].get("rows_affected")


def count():
    con = duckdb.connect(str(DB), read_only=True)
    try:
        return con.execute(f"select count(*) from staging.{MODEL}").fetchone()[0]
    finally:
        con.close()


def midpoint():
    con = duckdb.connect(str(DB), read_only=True)
    try:
        return con.execute(
            "select quantile_cont(recorded_at, 0.5) from raw.cala_entry_events"
        ).fetchone()[0]
    finally:
        con.close()


def main():
    if not DB.exists():
        sys.exit("run `make seed` first")
    mid = midpoint().isoformat()
    total = duckdb.connect(str(DB), read_only=True).execute(
        "select count(*) from raw.cala_entry_events"
    ).fetchone()[0]

    steps = [
        ("full refresh, first half (`entries_as_of`)",
         ["--full-refresh", "--vars", json.dumps({"entries_as_of": mid})]),
        ("incremental, second half", []),
        ("full refresh, all events", ["--full-refresh"]),
        ("incremental, no new events", []),
    ]
    rows = []
    before = 0
    for label, args in steps:
        wall, exec_time, affected = dbt(*args)
        after = count()
        written = after if "full" in label else after - before
        rows.append((label, after, written, exec_time, wall))
        before = after

    print(f"source rows: {total} (`cala_entry_events`), split at {mid}\n")
    print("| run | rows in table | rows written | model time (s) | dbt wall (s) |")
    print("|---|---:|---:|---:|---:|")
    for label, after, written, exec_time, wall in rows:
        print(f"| {label} | {after} | {written} | {exec_time:.2f} | {wall:.1f} |")


if __name__ == "__main__":
    main()
