"""`cala-extract`: land cala's tables in the warehouse, once.

    CALA_PG_URL=postgres://... cala-extract [--verify] [--full-refresh]

No orchestrator: run it from cron, Dagster, a GitHub Action, or by hand.
Every run is idempotent, so running it twice is safe and the second run
loads nothing for the streams. After the load, every run honours the forget
events it landed and re-applies the erasures on record (see erasure.py).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from cala_extract.erasure import FORGET_EVENT_TYPES
from cala_extract.paths import DEFAULT_DATASET, DEFAULT_DUCKDB, DEFAULT_PIPELINES_DIR
from cala_extract.pipeline import DESTINATIONS, ForeignTables, extract, format_summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cala-extract", description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--source-url",
        default=os.environ.get("CALA_PG_URL"),
        help="cala's Postgres (default: $CALA_PG_URL). Needs SELECT only.",
    )
    parser.add_argument("--destination", choices=DESTINATIONS, default="duckdb")
    parser.add_argument(
        "--duckdb-path",
        default=str(DEFAULT_DUCKDB),
        help="DuckDB file for the duckdb destination (default: the file dbt builds from)",
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="schema / dataset to land in (default: raw)")
    parser.add_argument("--pipelines-dir", default=str(DEFAULT_PIPELINES_DIR), help=argparse.SUPPRESS)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="drop the landed tables and the outbox watermark, then load everything",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="count every source table in the snapshot and compare with the destination after the run",
    )
    parser.add_argument("--page-size", type=int, default=5000)
    parser.add_argument(
        "--forget-event-types",
        default=",".join(FORGET_EVENT_TYPES),
        help="event types that mean the application forgot the entity; an erasure is filed for each (default: %(default)s)",
    )
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    args = parser.parse_args(argv)

    if not args.source_url:
        parser.error("--source-url or CALA_PG_URL is required")

    try:
        summary = extract(
            args.source_url,
            destination=args.destination,
            duckdb_path=args.duckdb_path,
            dataset=args.dataset,
            pipelines_dir=args.pipelines_dir,
            full_refresh=args.full_refresh,
            verify=args.verify,
            page_size=args.page_size,
            forget_event_types=tuple(t.strip() for t in args.forget_event_types.split(",") if t.strip()),
        )
    except ForeignTables as exc:
        print(f"cala-extract: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary.as_dict(), indent=2) if args.json else format_summary(summary))
    return 1 if summary.verification_failures else 0


if __name__ == "__main__":
    sys.exit(main())
