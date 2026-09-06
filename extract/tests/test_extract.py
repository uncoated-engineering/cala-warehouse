"""The extractor against cala's real DDL and the committed seeds.

The controls here are the extraction-layer counterparts of the dbt tests:
what lands equals what the source held, an incremental run loads exactly
what the outbox announced, and the watermark never advances past a sequence
that could still be committed.
"""

from __future__ import annotations

import json
import uuid

import duckdb
import psycopg
import pytest

from cala_extract.fixture_db import insert_rows, load_fixtures
from cala_extract.pipeline import ForeignTables
from cala_extract.source import OUTBOX, STATE, STREAMS, Snapshot, resolve_watermark
from cala_extract.tables import ALL_TABLES

from warehouse_checks import assert_raw_equals_seeds, count, raw_columns

STREAM_TABLES = [OUTBOX, *(s.table for s in STREAMS)]


def test_first_load_lands_the_seeds_exactly(full_fixtures, run, warehouse):
    s = run(verify=True)

    assert (s.watermark_before, s.watermark_after) == (0, full_fixtures.outbox_max_sequence)
    assert s.stalled_at is None and s.abandoned == []
    assert s.verification_failures == {}
    assert s.rows_loaded == full_fixtures.row_counts
    assert_raw_equals_seeds(warehouse["duckdb_path"])


def test_landed_types_match_the_seed_contract(full_fixtures, run, warehouse):
    """UUIDs and JSON as text, timestamps naive UTC, and every column present
    even when the snapshot had only NULLs in it (`context`)."""
    run()
    conn = duckdb.connect(str(warehouse["duckdb_path"]), read_only=True)
    try:
        cols = dict(raw_columns(conn, "cala_entry_events"))
        assert cols == {
            "id": "VARCHAR",
            "sequence": "BIGINT",
            "event_type": "VARCHAR",
            "event": "VARCHAR",
            "context": "VARCHAR",
            "recorded_at": "TIMESTAMP",
        }
        assert conn.execute("select count(*) from raw.cala_entry_events where context is not null").fetchone()[0] == 0
        assert dict(raw_columns(conn, "cala_accounts"))["normal_balance_type"] == "VARCHAR"
        assert dict(raw_columns(conn, "cala_transactions"))["effective"] == "DATE"
        # the backdated fixture entries survive with their 2025 timestamp
        assert conn.execute("select min(recorded_at) from raw.cala_entry_events").fetchone()[0].year == 2025
    finally:
        conn.close()


def test_second_run_loads_nothing_for_the_streams(full_fixtures, run, warehouse):
    run()
    s = run(verify=True)
    assert s.watermark_before == s.watermark_after == full_fixtures.outbox_max_sequence
    assert {t: s.rows_loaded[t] for t in STREAM_TABLES} == {t: 0 for t in STREAM_TABLES}
    assert {t: s.rows_loaded[t] for t in STATE} == {t: full_fixtures.row_counts[t] for t in STATE}
    assert s.verification_failures == {}
    assert_raw_equals_seeds(warehouse["duckdb_path"])


def test_incremental_run_loads_exactly_what_the_outbox_announced(pg_url, run, warehouse):
    """Load half the history, let "cala" write the other half, load again.

    The second run must land precisely the withheld rows: its outbox rows
    are the withheld outbox rows, and (entries have one event each) its
    entry events are the withheld entry events. Afterwards the warehouse
    equals the full seeds, row for row.
    """
    cutoff = 2600
    first = load_fixtures(pg_url, withhold_outbox_after=cutoff)
    withheld = first.withheld_counts()
    assert withheld[OUTBOX] == first.outbox_max_sequence - cutoff
    assert withheld["cala_entry_events"] > 0

    s1 = run()
    assert (s1.watermark_before, s1.watermark_after) == (0, cutoff)
    assert s1.rows_loaded[OUTBOX] == cutoff
    for table in STREAM_TABLES:
        assert count(warehouse["duckdb_path"], table) == first.row_counts[table]

    for table, (header, rows) in first.withheld.items():
        if rows:
            insert_rows(pg_url, table, header, rows)

    s2 = run(verify=True)
    assert (s2.watermark_before, s2.watermark_after) == (cutoff, first.outbox_max_sequence)
    assert s2.stalled_at is None and s2.abandoned == []
    assert s2.rows_loaded[OUTBOX] == withheld[OUTBOX]
    assert s2.rows_loaded["cala_entry_events"] == withheld["cala_entry_events"]
    # an entity announced again (an `updated`) has its earlier events
    # re-fetched and merged, so the other streams may load a few more rows
    # than were withheld, never fewer
    for table in STREAM_TABLES:
        assert s2.rows_loaded[table] >= withheld.get(table, 0), table
    assert s2.verification_failures == {}
    assert_raw_equals_seeds(warehouse["duckdb_path"])


# --- the watermark and the outbox's gaps -----------------------------------


def post_journal(conn: psycopg.Connection, sequence: int, name: str) -> str:
    """What cala writes for a new journal, in one transaction: the state row,
    the `initialized` event and the outbox announcement."""
    journal_id = str(uuid.uuid4())
    values = {"id": journal_id, "name": name, "code": None, "description": None,
              "config": {"enable_effective_balances": False}, "status": "active", "version": 1}
    conn.execute(
        "insert into cala_journals (id, name, code, created_at) values (%s, %s, null, now())",
        [journal_id, name],
    )
    conn.execute(
        "insert into cala_journal_events (id, sequence, event_type, event, recorded_at) "
        "values (%s, 1, 'initialized', %s, now())",
        [journal_id, json.dumps({"type": "initialized", "values": values})],
    )
    conn.execute(
        f"insert into {OUTBOX} (sequence, payload) values (%s, %s)",
        [sequence, json.dumps({"type": "journal_created", "journal": values})],
    )
    return journal_id


def test_in_flight_transaction_stalls_the_watermark(pg_url, full_fixtures, run, warehouse):
    """Sequence N+1 is allocated but its transaction has not committed while
    N+2 has. The run lands N+2 (it is in the snapshot) but the watermark stays
    at N, so the next run re-reads from N+1 and picks it up once it commits."""
    run()
    top = full_fixtures.outbox_max_sequence

    in_flight = psycopg.connect(pg_url)
    post_journal(in_flight, top + 1, "in-flight")  # not committed
    with psycopg.connect(pg_url) as committed:
        post_journal(committed, top + 2, "committed")
        committed.commit()

    s = run()
    assert (s.watermark_before, s.watermark_after, s.highest_seen) == (top, top, top + 2)
    assert s.stalled_at == top + 1 and s.abandoned == []
    assert s.rows_loaded[OUTBOX] == 1
    assert s.rows_loaded["cala_journal_events"] == 1
    assert count(warehouse["duckdb_path"], OUTBOX) == top + 1

    in_flight.commit()
    in_flight.close()
    s = run(verify=True)
    assert (s.watermark_before, s.watermark_after) == (top, top + 2)
    assert s.stalled_at is None
    assert count(warehouse["duckdb_path"], OUTBOX) == top + 2
    assert count(warehouse["duckdb_path"], "cala_journal_events") == full_fixtures.row_counts["cala_journal_events"] + 2
    assert s.verification_failures == {}


def test_abandoned_sequences_are_proven_and_skipped(pg_url, full_fixtures, run, warehouse):
    """Sequences N+1..N+3 were burnt by rolled-back transactions and N+4 is a
    placeholder row (payload NULL) the way obix writes them. Nothing is in
    flight, so the horizon proof passes and the watermark reaches N+4."""
    run()
    top = full_fixtures.outbox_max_sequence
    with psycopg.connect(pg_url) as conn:
        conn.execute(f"insert into {OUTBOX} (sequence, payload) values (%s, null)", [top + 4])
        conn.commit()

    s = run(verify=True)
    assert (s.watermark_before, s.watermark_after) == (top, top + 4)
    assert s.stalled_at is None
    assert s.abandoned == [top + 1, top + 2, top + 3]
    assert s.rows_loaded[OUTBOX] == 1
    assert all(s.rows_loaded[st.table] == 0 for st in STREAMS)
    assert s.verification_failures == {}


def test_resolve_watermark_stops_before_a_late_commit(pg_url, full_fixtures):
    """A gap that filled in between the snapshot and the proof is real data
    the snapshot did not see: stop before it rather than skip it."""
    top = full_fixtures.outbox_max_sequence
    snapshot = Snapshot.open(pg_url)
    marker = snapshot.marker
    snapshot.close()
    with psycopg.connect(pg_url) as conn:
        conn.execute(f"insert into {OUTBOX} (sequence, payload) values (%s, null)", [top + 2])
        conn.commit()

    assert resolve_watermark(pg_url, marker, top + 2, [top + 1]) == (top + 2, None, [top + 1])

    with psycopg.connect(pg_url) as conn:
        conn.execute(f"insert into {OUTBOX} (sequence, payload) values (%s, null)", [top + 1])
        conn.commit()
    assert resolve_watermark(pg_url, marker, top + 2, [top + 1]) == (top, top + 1, [])


def test_resolve_watermark_waits_while_the_horizon_has_not_passed(pg_url, full_fixtures):
    top = full_fixtures.outbox_max_sequence
    older = psycopg.connect(pg_url)
    older.execute("select pg_current_xact_id()")  # holds an xid below any later marker
    snapshot = Snapshot.open(pg_url)
    marker = snapshot.marker
    snapshot.close()
    try:
        assert resolve_watermark(pg_url, marker, top + 5, [top + 1, top + 2]) == (top, top + 1, [])
    finally:
        older.rollback()
        older.close()
    assert resolve_watermark(pg_url, marker, top + 5, [top + 1, top + 2]) == (top + 5, None, [top + 1, top + 2])


# --- state and refresh -------------------------------------------------------


def test_full_refresh_reloads_everything(full_fixtures, run):
    run()
    s = run(full_refresh=True, verify=True)
    assert (s.watermark_before, s.watermark_after) == (0, full_fixtures.outbox_max_sequence)
    assert s.rows_loaded == full_fixtures.row_counts
    assert s.verification_failures == {}


def test_a_wiped_warehouse_starts_over(full_fixtures, run, warehouse):
    """`make clean` deletes the DuckDB file but not dlt's local state. The
    watermark must follow the warehouse, not the local file."""
    run()
    warehouse["duckdb_path"].unlink()
    s = run(verify=True)
    assert (s.watermark_before, s.watermark_after) == (0, full_fixtures.outbox_max_sequence)
    assert s.rows_loaded == full_fixtures.row_counts
    assert s.verification_failures == {}
    assert_raw_equals_seeds(warehouse["duckdb_path"])


def test_every_stream_id_is_announced_on_the_outbox(pg_url, full_fixtures):
    """The premise of the design, checked on the seeds: no id in any keyed
    stream is missing from the outbox payloads that stream listens to."""
    with psycopg.connect(pg_url) as conn:
        rows = conn.execute(f"select payload::text from {OUTBOX} where payload is not null").fetchall()
        announced = {s.table: set() for s in STREAMS}
        for (payload,) in rows:
            p = json.loads(payload)
            for s in STREAMS:
                if entity_id := s.ids_in(p):
                    announced[s.table].add(entity_id)
        for s in STREAMS:
            ids = {r[0] for r in conn.execute(f"select id::text from {s.table}").fetchall()}
            assert ids <= announced[s.table], f"{s.table}: {len(ids - announced[s.table])} ids never announced"


def test_refuses_to_load_over_dbt_seeds_unless_refreshing(full_fixtures, run, warehouse):
    """`make build` and `make extract` both land raw.cala_*. Over seed tables
    the extractor must stop with a clear message, and --full-refresh must
    replace them rather than try to alter them."""
    conn = duckdb.connect(str(warehouse["duckdb_path"]))
    conn.execute("create schema raw")
    conn.execute("create table raw.cala_entries as select 'seed' as id")
    conn.execute("create table raw.cala_entry_events as select 'seed' as id, 1 as sequence")
    conn.close()

    with pytest.raises(ForeignTables, match="cala_entry_events.*dbt seeds"):
        run()

    s = run(full_refresh=True, verify=True)
    assert s.rows_loaded == full_fixtures.row_counts
    assert s.verification_failures == {}
    assert_raw_equals_seeds(warehouse["duckdb_path"])
