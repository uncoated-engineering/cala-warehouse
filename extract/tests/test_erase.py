"""Erasure propagation: every landed copy of an entity's personal fields is
redacted, the accounting rows are untouched, the log says what happened, and
the erasure survives the next extraction run, which re-lands the data from a
source that never forgot it.

The first half runs on a DuckDB file filled straight from the seeds (no
Postgres); the second half needs CALA_PG_URL like the other extractor tests.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import duckdb
import psycopg
import pytest

from cala_extract.erasure import ERASURE_LOG, KINDS, Store, erase, known_erasures, read_log, redact_json
from cala_extract.fixture_db import LOAD_ORDER
from cala_extract.pipeline import build_pipeline
from cala_extract.source import OUTBOX

SEED_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "seed"

PERSONAL_JSON = ["$.values.name", "$.values.description", "$.values.external_id", "$.values.metadata"]


def seed_duckdb(path: Path) -> None:
    """raw.cala_* from the CSVs, the way `dbt seed` would land them (no dlt
    columns), which is also the shape cala-erase meets on a seeded warehouse."""
    conn = duckdb.connect(str(path))
    conn.execute("create schema if not exists raw")
    for table in LOAD_ORDER:
        conn.execute(
            f"create table raw.{table} as select * from read_csv('{SEED_DIR / table}.csv', header=true, "
            "all_varchar=true, allow_quoted_nulls=false)"
        )
        if table.endswith("_events") or table == OUTBOX:
            conn.execute(f"alter table raw.{table} alter sequence type bigint")
    conn.close()


@pytest.fixture
def seeded(tmp_path: Path) -> dict:
    path = tmp_path / "warehouse.duckdb"
    seed_duckdb(path)
    return {"duckdb_path": path, "pipelines_dir": tmp_path / "pipelines"}


def pipeline_for(wh: dict):
    return build_pipeline("duckdb", duckdb_path=wh["duckdb_path"], pipelines_dir=wh["pipelines_dir"])


def q(path: Path, sql: str, *params):
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute(sql, list(params)).fetchall()
    finally:
        conn.close()


def named_account_with_entries(path: Path) -> str:
    """An account that carries a name and has entries with metadata."""
    return q(
        path,
        """
        select a.id from raw.cala_accounts a
        join raw.cala_entries e on e.account_id = a.id
        join raw.cala_entry_events ev on ev.id = e.id
        where a.name <> '' and json_extract_string(ev.event, '$.values.metadata') is not null
        group by a.id order by count(*) desc, a.id limit 1
        """,
    )[0][0]


def personal_values_left(path: Path, account_id: str) -> dict[str, int]:
    """How many non-null personal values still exist for the account, per copy."""
    return {
        "events": q(
            path,
            "select count(*) from raw.cala_account_events where id = ? and ("
            + " or ".join(f"json_extract_string(event, '{p}') is not null" for p in PERSONAL_JSON)
            + ")",
            account_id,
        )[0][0],
        "state": q(path, "select count(*) from raw.cala_accounts where id = ? and (name is not null or external_id is not null)", account_id)[0][0],
        "outbox": q(
            path,
            "select count(*) from raw.cala_persistent_outbox_events where json_extract_string(payload, '$.account.id') = ? and ("
            + " or ".join(f"json_extract_string(payload, '$.account.{f}') is not null" for f in ("name", "description", "external_id", "metadata"))
            + ")",
            account_id,
        )[0][0],
    }


# --- pure -----------------------------------------------------------------


def test_redact_json_nulls_only_what_is_there():
    doc = {"type": "initialized", "values": {"id": "x", "name": "Alice", "metadata": {"k": 1}, "description": None}}
    assert redact_json(doc, ["values.name", "values.metadata", "values.description", "values.missing", "nope.name"]) == 2
    assert doc == {"type": "initialized", "values": {"id": "x", "name": None, "metadata": None, "description": None}}


def test_every_kind_names_real_tables():
    from cala_extract.tables import ALL_TABLES

    for kind in KINDS.values():
        assert kind.stream in ALL_TABLES
        assert kind.state_table is None or kind.state_table in ALL_TABLES
        assert all(f.startswith("values.") for f in kind.event_fields)


# --- on a seeded warehouse (no Postgres) ----------------------------------


def test_erase_account_redacts_every_copy_and_nothing_else(seeded):
    path = seeded["duckdb_path"]
    account_id = named_account_with_entries(path)
    before = personal_values_left(path, account_id)
    assert before["events"] >= 1 and before["state"] == 1 and before["outbox"] >= 1
    entries_before = q(path, "select id, sequence, event from raw.cala_entry_events order by 1, 2")
    balances_before = q(path, "select * from raw.cala_current_balances order by 1, 2, 3")
    others_before = q(path, "select count(*) from raw.cala_accounts where name is not null and id <> ?", account_id)[0][0]

    rows = erase(pipeline_for(seeded), "account", account_id, reason="DSAR-42", requested_by="test")

    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
    # the entity's structure survives: same events, same sequences, same ids
    kept = q(path, "select json_extract_string(event, '$.values.id'), json_extract_string(event, '$.values.normal_balance_type'), json_extract_string(event, '$.values.code') from raw.cala_account_events where id = ?", account_id)
    assert kept and all(r[0] == account_id and r[1] in ("debit", "credit") and r[2] for r in kept)
    # the accounting rows are byte-for-byte what they were
    assert q(path, "select id, sequence, event from raw.cala_entry_events order by 1, 2") == entries_before
    assert q(path, "select * from raw.cala_current_balances order by 1, 2, 3") == balances_before
    assert q(path, "select count(*) from raw.cala_accounts where name is not null and id <> ?", account_id)[0][0] == others_before

    by_table = {r.table_name: r for r in rows}
    assert set(by_table) == {"cala_account_events", "cala_accounts", OUTBOX}
    assert all(r.source == "operator" and r.reason == "DSAR-42" and r.requested_by == "test" for r in rows)
    assert len({r.erasure_id for r in rows}) == 1
    assert by_table["cala_accounts"].rows_matched == 1 and by_table["cala_accounts"].rows_redacted == 1
    assert by_table["cala_account_events"].rows_redacted == before["events"]
    assert by_table[OUTBOX].rows_redacted == before["outbox"]
    assert json.loads(by_table["cala_account_events"].fields) == ["values.name", "values.description", "values.external_id", "values.metadata"]

    logged = q(path, f"select entity_kind, entity_id, source, table_name, rows_redacted from raw.{ERASURE_LOG} order by table_name")
    assert [(r[0], r[1], r[2]) for r in logged] == [("account", account_id, "operator")] * 3


def test_with_entries_cascades_to_the_account_entries(seeded):
    path = seeded["duckdb_path"]
    account_id = named_account_with_entries(path)
    entry_ids = {r[0] for r in q(path, "select id from raw.cala_entries where account_id = ?", account_id)}
    with_meta = q(
        path,
        "select count(*) from raw.cala_entry_events where id in (select id from raw.cala_entries where account_id = ?) "
        "and json_extract_string(event, '$.values.metadata') is not null",
        account_id,
    )[0][0]
    assert with_meta > 0
    units_before = q(path, "select id, json_extract_string(event, '$.values.units'), json_extract_string(event, '$.values.direction') from raw.cala_entry_events order by 1")

    rows = erase(pipeline_for(seeded), "account", account_id, reason="DSAR-42", with_entries=True)

    parent = [r for r in rows if r.entity_kind == "account"]
    cascaded = [r for r in rows if r.entity_kind == "entry"]
    assert {r.entity_id for r in cascaded} == entry_ids
    assert all(r.source == "cascade" and r.cascade_of == parent[0].erasure_id for r in cascaded)
    assert sum(r.rows_redacted for r in cascaded if r.table_name == "cala_entry_events") == with_meta
    assert q(
        path,
        "select count(*) from raw.cala_entry_events where id in (select id from raw.cala_entries where account_id = ?) "
        "and (json_extract_string(event, '$.values.metadata') is not null or json_extract_string(event, '$.values.description') is not null)",
        account_id,
    )[0][0] == 0
    assert q(path, "select count(*) from raw.cala_persistent_outbox_events where json_extract_string(payload, '$.entry.account_id') = ? and json_extract_string(payload, '$.entry.metadata') is not null", account_id)[0][0] == 0
    # amounts and directions untouched, on every entry
    assert q(path, "select id, json_extract_string(event, '$.values.units'), json_extract_string(event, '$.values.direction') from raw.cala_entry_events order by 1") == units_before


def test_repeat_request_is_logged_but_redacts_nothing(seeded):
    path = seeded["duckdb_path"]
    account_id = named_account_with_entries(path)
    p = pipeline_for(seeded)
    first = erase(p, "account", account_id, reason="DSAR-42")
    second = erase(p, "account", account_id, reason="DSAR-42 again")
    assert sum(r.rows_redacted for r in first) > 0
    assert sum(r.rows_redacted for r in second) == 0 and sum(r.rows_matched for r in second) > 0
    with Store(p) as store:
        assert len(known_erasures(store)) == 1  # one entity on record, oldest request wins
        assert len(read_log(store)) == len(first) + len(second)


def test_unknown_id_is_logged_as_received(seeded):
    p = pipeline_for(seeded)
    rows = erase(p, "transaction", str(uuid.uuid4()), reason="DSAR-7")
    assert len(rows) == 1 and (rows[0].rows_matched, rows[0].rows_redacted) == (0, 0)
    with pytest.raises(ValueError, match="UUID"):
        erase(p, "account", "not-an-id", reason="x")
    with pytest.raises(ValueError, match="accounts only"):
        erase(p, "entry", str(uuid.uuid4()), reason="x", with_entries=True)


def test_erase_cli_round_trip(seeded, capsys):
    from cala_extract.erase import main

    path = seeded["duckdb_path"]
    account_id = named_account_with_entries(path)
    common = ["--duckdb-path", str(path), "--pipelines-dir", str(seeded["pipelines_dir"])]
    assert main(["account", account_id, "--reason", "DSAR-1", "--requested-by", "cli", *common]) == 0
    assert personal_values_left(path, account_id)["state"] == 0
    capsys.readouterr()
    assert main(["list", "--json", *common]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert {r["entity_id"] for r in listed} == {account_id} and {r["requested_by"] for r in listed} == {"cli"}
    assert main(["account", "nope", "--reason", "x", *common]) == 2


# --- against Postgres: the erasure outlives the next extraction --------------


def test_erasure_is_reapplied_after_the_next_run(pg_url, full_fixtures, run, warehouse):
    run()
    path = warehouse["duckdb_path"]
    account_id = named_account_with_entries(path)
    p = build_pipeline("duckdb", duckdb_path=path, pipelines_dir=warehouse["pipelines_dir"])
    erase(p, "account", account_id, reason="DSAR-42")
    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}

    # cala still holds the name: the next run replaces cala_accounts from it
    s = run(verify=True)
    assert s.verification_failures == {}
    assert s.erasures_known == 1 and s.forget_events == 0
    assert s.erasures_reapplied == {"cala_accounts": 1}
    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
    log = q(path, f"select source, table_name, rows_redacted from raw.{ERASURE_LOG} where source = 'reapply'")
    assert log == [("reapply", "cala_accounts", 1)]

    # an update to the account re-announces it: its events are re-merged from
    # the source and must be redacted again, including the new event
    top = full_fixtures.outbox_max_sequence
    with psycopg.connect(pg_url) as conn:
        values = json.loads(conn.execute("select event::text from cala_account_events where id = %s and sequence = 1", [account_id]).fetchone()[0])["values"]
        values["version"] = 2
        values["description"] = "renamed by the source after the erasure"
        conn.execute(
            "insert into cala_account_events (id, sequence, event_type, event, recorded_at) values (%s, 2, 'updated', %s, now())",
            [account_id, json.dumps({"type": "updated", "values": values, "fields": ["description"]})],
        )
        conn.execute(
            f"insert into {OUTBOX} (sequence, payload) values (%s, %s)",
            [top + 1, json.dumps({"type": "account_updated", "account": values, "fields": ["description"]})],
        )
        conn.commit()
    s = run(verify=True)
    assert s.verification_failures == {} and s.rows_loaded["cala_account_events"] == 2
    assert s.erasures_reapplied == {"cala_account_events": 2, "cala_accounts": 1, OUTBOX: 1}
    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
    assert q(path, "select count(*) from raw.cala_account_events where id = ?", account_id)[0][0] == 2

    # a full refresh reloads everything from the source and re-applies too
    s = run(full_refresh=True, verify=True)
    assert s.verification_failures == {} and s.erasures_known == 1
    assert s.erasures_reapplied[OUTBOX] >= 1 and s.erasures_reapplied["cala_account_events"] == 2
    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
    assert q(path, f"select count(distinct erasure_id) from raw.{ERASURE_LOG}")[0][0] == 1


def test_a_forget_event_on_the_stream_is_honoured(pg_url, full_fixtures, run, warehouse):
    """es-entity's convention: the application stages an empty `Forgot {}`
    (event_type 'forgot') before forget(), so the erasure is on the stream and
    the outbox announces the write. The extractor lands it and erases."""
    run()
    path = warehouse["duckdb_path"]
    account_id = named_account_with_entries(path)
    top = full_fixtures.outbox_max_sequence
    with psycopg.connect(pg_url) as conn:
        values = json.loads(conn.execute("select event::text from cala_account_events where id = %s and sequence = 1", [account_id]).fetchone()[0])["values"]
        conn.execute(
            "insert into cala_account_events (id, sequence, event_type, event, recorded_at) values (%s, 2, 'forgot', %s, now())",
            [account_id, json.dumps({"type": "forgot"})],
        )
        conn.execute(
            f"insert into {OUTBOX} (sequence, payload) values (%s, %s)",
            [top + 1, json.dumps({"type": "account_updated", "account": values, "fields": []})],
        )
        conn.commit()

    s = run(verify=True)
    assert s.verification_failures == {}
    assert (s.forget_events, s.erasures_known) == (1, 1)
    assert personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
    log = q(path, f"select source, requested_by, reason, table_name from raw.{ERASURE_LOG} order by table_name")
    assert {r[0] for r in log} == {"forget_event"} and {r[1] for r in log} == {"upstream"}
    assert all("forgot event at sequence 2 on cala_account_events" == r[2] for r in log)
    assert {r[3] for r in log} == {"cala_account_events", "cala_accounts", OUTBOX}

    # the next run sees the same forgot event again (no new outbox row for the
    # account, so nothing is re-merged) and files nothing new
    s = run(verify=True)
    assert (s.forget_events, s.erasures_known) == (0, 1)
    assert s.erasures_reapplied == {"cala_accounts": 1}
    assert q(path, f"select count(distinct erasure_id) from raw.{ERASURE_LOG}")[0][0] == 1


def test_forget_event_types_are_configurable(pg_url, full_fixtures, run, warehouse):
    run()
    path = warehouse["duckdb_path"]
    account_id = named_account_with_entries(path)
    top = full_fixtures.outbox_max_sequence
    with psycopg.connect(pg_url) as conn:
        values = json.loads(conn.execute("select event::text from cala_account_events where id = %s and sequence = 1", [account_id]).fetchone()[0])["values"]
        conn.execute(
            "insert into cala_account_events (id, sequence, event_type, event, recorded_at) values (%s, 2, 'erasure_requested', %s, now())",
            [account_id, json.dumps({"type": "erasure_requested"})],
        )
        conn.execute(f"insert into {OUTBOX} (sequence, payload) values (%s, %s)", [top + 1, json.dumps({"type": "account_updated", "account": values, "fields": []})])
        conn.commit()
    s = run()
    assert s.forget_events == 0 and personal_values_left(path, account_id)["state"] == 1
    # the event is already landed; only a run that lands it again sees it, so
    # re-announce the account with the wider type list
    with psycopg.connect(pg_url) as conn:
        conn.execute(f"insert into {OUTBOX} (sequence, payload) values (%s, %s)", [top + 2, json.dumps({"type": "account_updated", "account": values, "fields": []})])
        conn.commit()
    s = run(forget_event_types=("forgot", "erasure_requested"))
    assert s.forget_events == 1 and personal_values_left(path, account_id) == {"events": 0, "state": 0, "outbox": 0}
