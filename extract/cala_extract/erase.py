"""`cala-erase`: honour an erasure request in the warehouse, now.

    cala-erase account <uuid> --reason "DSAR-42" [--requested-by ops] [--with-entries]
    cala-erase transaction|entry|account-set|journal <uuid> --reason ...
    cala-erase list                # the log, oldest first
    cala-erase reapply             # re-redact every logged erasure on every row

The personal fields of that entity (name, description, external_id, metadata,
as far as the entity has them) are set to null in every landed copy: its
events, its current-state row and the outbox payloads that announced it.
Amounts, ids, sequences and timestamps are never touched, so the accounting
controls still hold. Every request is appended to cala_erasure_log, and
`cala-extract` re-applies it after each run, because cala itself still holds
the data. See erasure.py for the design and the dbt test
assert_erased_entities_hold_no_personal_data for the proof.

Rebuild the marts afterwards (`make build-extracted`, or `dbt build` on a
seeded warehouse): the staging views read the redacted rows at once, the
tables do not.
"""

from __future__ import annotations

import argparse
import json
import sys

from cala_extract.erasure import Store, erase, known_erasures, read_log, reapply, write_log
from cala_extract.paths import DEFAULT_DATASET, DEFAULT_DUCKDB, DEFAULT_PIPELINES_DIR
from cala_extract.pipeline import DESTINATIONS, build_pipeline

KIND_ARGS = {"account": "account", "account-set": "account_set", "transaction": "transaction", "entry": "entry", "journal": "journal"}


def _destination_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--destination", choices=DESTINATIONS, default="duckdb")
    parser.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB), help="DuckDB file (default: the file dbt builds from)")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="schema / dataset the raw tables live in (default: raw)")
    parser.add_argument("--pipelines-dir", default=str(DEFAULT_PIPELINES_DIR), help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help="print the log rows as JSON")


def _print_rows(rows, as_json: bool) -> None:
    if as_json:
        print(json.dumps([dict(r.as_dict() if hasattr(r, "as_dict") else r) for r in rows], indent=2, default=str))
        return
    if not rows:
        print("nothing logged")
        return
    dicts = [r.as_dict() if hasattr(r, "as_dict") else r for r in rows]
    width = max(len(d["table_name"]) for d in dicts)
    for d in dicts:
        print(
            f"{str(d['applied_at'])[:19]}  {d['source']:<12} {d['entity_kind']:<11} {d['entity_id']}  "
            f"{d['table_name']:<{width}}  matched {d['rows_matched']:>5}  redacted {d['rows_redacted']:>5}"
            + (f"  ({d['reason']})" if d.get("reason") else "")
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cala-erase", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for arg, kind in KIND_ARGS.items():
        p = sub.add_parser(arg, help=f"erase one {kind.replace('_', ' ')}")
        p.add_argument("entity_id")
        p.add_argument("--reason", required=True, help="why: ticket, request id, legal basis")
        p.add_argument("--requested-by", default=None, help="who asked (recorded in the log)")
        if kind == "account":
            p.add_argument("--with-entries", action="store_true", help="also erase the entries posted to the account")
        _destination_args(p)
    _destination_args(sub.add_parser("list", help="print cala_erasure_log"))
    _destination_args(sub.add_parser("reapply", help="re-redact every logged erasure on every landed row"))
    args = parser.parse_args(argv)

    pipeline = build_pipeline(
        args.destination, duckdb_path=args.duckdb_path, dataset=args.dataset, pipelines_dir=args.pipelines_dir
    )
    if args.command == "list":
        with Store(pipeline) as store:
            _print_rows(read_log(store), args.json)
        return 0
    if args.command == "reapply":
        with Store(pipeline) as store:
            known = known_erasures(store)
            rows = reapply(store, None, run_id="cala-erase:reapply")
        write_log(pipeline, rows)
        if not args.json:
            print(f"{len(known)} erasures on record; {sum(r.rows_redacted for r in rows)} rows redacted again")
        _print_rows(rows, args.json)
        return 0

    try:
        rows = erase(
            pipeline,
            KIND_ARGS[args.command],
            args.entity_id,
            reason=args.reason,
            requested_by=args.requested_by,
            with_entries=getattr(args, "with_entries", False),
        )
    except ValueError as exc:
        print(f"cala-erase: {exc}", file=sys.stderr)
        return 2
    _print_rows(rows, args.json)
    if not args.json and all(r.rows_matched == 0 for r in rows):
        print("warning: no landed row carries that id; the request is logged and will apply to rows landed later", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
