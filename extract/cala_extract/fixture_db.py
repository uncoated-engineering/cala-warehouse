"""Turn a Postgres database into "cala after its test suite ran".

Applies fixtures/schema.sql (cala's real DDL, dumped by fixtures/schema.sh)
and COPYs the committed seeds in. That gives the extractor and its tests a
source with cala's exact column types, constraints and partitioned outbox,
without needing cala or Docker at test time.

    python -m cala_extract.fixture_db postgres://user:password@localhost/pg

DESTRUCTIVE: drops and recreates the `public` schema of that database.
`withhold_outbox_after` keeps back everything the outbox announced after a
sequence, so a test can replay "cala kept running" by inserting it later.
"""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import psycopg

from cala_extract.paths import REPO_ROOT
from cala_extract.tables import OUTBOX, STREAMS

SEED_DIR = REPO_ROOT / "fixtures" / "seed"
SCHEMA_SQL = REPO_ROOT / "fixtures" / "schema.sql"

# COPY order respects the foreign keys in schema.sql.
LOAD_ORDER: tuple[str, ...] = (
    "cala_journals",
    "cala_accounts",
    "cala_account_sets",
    "cala_account_set_member_accounts",
    "cala_account_set_member_account_sets",
    "cala_tx_templates",
    "cala_transactions",
    "cala_entries",
    "cala_current_balances",
    "cala_balance_history",
    "cala_journal_events",
    "cala_account_events",
    "cala_account_set_events",
    "cala_tx_template_events",
    "cala_transaction_events",
    "cala_entry_events",
    OUTBOX,
)

# Columns the seeds carry that Postgres computes itself.
GENERATED: dict[str, tuple[str, ...]] = {"cala_entries": ("account_is_account_set",)}

STREAM_TABLES = {OUTBOX, *(s.table for s in STREAMS)}


@dataclass
class Loaded:
    url: str
    row_counts: dict[str, int] = field(default_factory=dict)
    # rows kept back per table, as (header, rows) ready for insert_rows()
    withheld: dict[str, tuple[list[str], list[list[str]]]] = field(default_factory=dict)
    outbox_max_sequence: int = 0

    def withheld_counts(self) -> dict[str, int]:
        return {t: len(rows) for t, (_, rows) in self.withheld.items() if rows}


def read_seed(table: str) -> tuple[list[str], list[list[str]]]:
    with (SEED_DIR / f"{table}.csv").open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        return header, list(reader)


def _copy_csv(conn: psycopg.Connection, table: str, header: list[str], rows: Iterable[list[str]]) -> int:
    """COPY rows in CSV format. An empty field is NULL, which is exactly how
    psql's \\copy wrote the seeds; only the state tables carry empty strings
    ("" names on backing accounts), and those are streamed raw by the caller."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    n = 0
    for row in rows:
        writer.writerow(row)
        n += 1
    cols = ", ".join(f'"{c}"' for c in header)
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)") as copy:
        copy.write(buf.getvalue())
    return n


def _copy_raw(conn: psycopg.Connection, table: str) -> int:
    path = SEED_DIR / f"{table}.csv"
    with path.open("rb") as fh:
        header = fh.readline().decode().strip().split(",")
        cols = ", ".join(f'"{c}"' for c in header)
        with conn.cursor() as cur, cur.copy(
            f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)"
        ) as copy:
            while chunk := fh.read(1 << 20):
                copy.write(chunk)
    return conn.execute(f"select count(*) from {table}").fetchone()[0]


def insert_rows(url: str, table: str, header: list[str], rows: list[list[str]]) -> None:
    """Append seed rows later (the "cala kept running" half of a test)."""
    with psycopg.connect(url) as conn:
        _copy_csv(conn, table, header, rows)
        conn.commit()


def mentions_up_to(outbox_rows: list[list[str]], header: list[str], cutoff: int) -> dict[str, dict[str, int]]:
    """For each stream: entity id -> how many outbox rows with sequence <= cutoff
    announce it. That is how many of the entity's events existed by then."""
    seq_i, payload_i = header.index("sequence"), header.index("payload")
    counts: dict[str, dict[str, int]] = {s.table: {} for s in STREAMS}
    for row in outbox_rows:
        if int(row[seq_i]) > cutoff:
            continue
        payload = json.loads(row[payload_i]) if row[payload_i] else None
        for stream in STREAMS:
            entity_id = stream.ids_in(payload)
            if entity_id:
                counts[stream.table][entity_id] = counts[stream.table].get(entity_id, 0) + 1
    return counts


def load_fixtures(url: str, *, withhold_outbox_after: int | None = None) -> Loaded:
    loaded = Loaded(url=url)
    outbox_header, outbox_rows = read_seed(OUTBOX)
    loaded.outbox_max_sequence = max(int(r[outbox_header.index("sequence")]) for r in outbox_rows)
    mentions = (
        mentions_up_to(outbox_rows, outbox_header, withhold_outbox_after)
        if withhold_outbox_after is not None
        else None
    )

    with psycopg.connect(url) as conn:
        conn.execute("drop schema public cascade")
        conn.execute("create schema public")
        conn.execute(SCHEMA_SQL.read_text())
        for table in LOAD_ORDER:
            if table not in STREAM_TABLES and table not in GENERATED:
                loaded.row_counts[table] = _copy_raw(conn, table)
                continue
            header, rows = read_seed(table)
            if table in GENERATED:
                drop = [header.index(c) for c in GENERATED[table]]
                header = [c for i, c in enumerate(header) if i not in drop]
                rows = [[v for i, v in enumerate(r) if i not in drop] for r in rows]
            keep, hold = rows, []
            if mentions is not None and table == OUTBOX:
                seq_i = header.index("sequence")
                keep = [r for r in rows if int(r[seq_i]) <= withhold_outbox_after]
                hold = [r for r in rows if int(r[seq_i]) > withhold_outbox_after]
            elif mentions is not None and table in mentions:
                id_i, seq_i = header.index("id"), header.index("sequence")
                known = mentions[table]
                keep = [r for r in rows if int(r[seq_i]) <= known.get(r[id_i], 0)]
                hold = [r for r in rows if int(r[seq_i]) > known.get(r[id_i], 0)]
            loaded.row_counts[table] = _copy_csv(conn, table, header, keep)
            loaded.withheld[table] = (header, hold)
        conn.commit()
    return loaded


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    loaded = load_fixtures(args[0])
    for table, n in loaded.row_counts.items():
        print(f"{table:<42} {n:>8} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
