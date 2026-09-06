import datetime as dt
import json

import duckdb
import pytest

from cala_mcp.manifest import Manifest
from cala_mcp.tools import reconcile


@pytest.fixture(scope="module")
def conn(duckdb_path):
    c = duckdb.connect(str(duckdb_path), read_only=True)
    yield c
    c.close()


def test_reconcile_is_green_on_the_committed_seeds(paths):
    r = reconcile(**paths)
    assert r["mismatches"] == []
    assert r["is_reconciled"] is True
    assert r["as_of"] is None
    names = [c["name"] for c in r["controls"]]
    assert names == ["assert_balances_reconcile", "assert_account_set_balances_reconcile"]
    leaf, sets = r["controls"]
    assert leaf["rows_by_status"].get("ok", 0) > 0 and leaf["mismatch_count"] == 0
    assert sets["rows_by_status"].get("ok", 0) > 0 and sets["mismatch_count"] == 0
    assert sum(leaf["rows_by_status"].values()) == leaf["rows_compared"]
    json.dumps(r)


def test_reconcile_agrees_with_the_dbt_tests(paths, conn):
    """The tool re-implements the two singular tests; both must give the same verdict."""
    m = Manifest.load(paths["manifest_path"])
    for name in ("assert_balances_reconcile", "assert_account_set_balances_reconcile"):
        node = m.test(name)
        compiled = node.get("compiled_code")
        if not compiled:
            pytest.skip(f"{name} has no compiled_code in the manifest; run `make build`")
        assert conn.sql(compiled).fetchall() == []
    assert reconcile(**paths)["mismatches"] == []


def test_reconcile_as_of_end_is_green(paths, conn):
    end = conn.sql(
        "select greatest((select max(recorded_at) from marts.fct_entries), "
        "(select max(recorded_at) from raw.cala_balance_history))"
    ).fetchone()[0]
    r = reconcile(as_of=end.isoformat(), **paths)
    assert r["as_of"] == end.isoformat()
    assert r["sources"]["cala"].startswith("cala_balance_history")
    assert r["mismatches"] == [] and r["is_reconciled"] is True
    now = reconcile(**paths)
    assert [c["rows_by_status"] for c in r["controls"]] == [c["rows_by_status"] for c in now["controls"]]
    assert any("closure" in c for c in r["caveats"])


def test_reconcile_as_of_before_cala_wrote_anything_reports_backdated_entries(paths, conn):
    """Entries whose recorded_at predates cala's first balance write are a real
    as-of gap: the warehouse has them, cala's history does not yet."""
    first_write = conn.sql("select min(recorded_at) from raw.cala_balance_history").fetchone()[0]
    cutoff = first_write.replace(microsecond=0) - dt.timedelta(seconds=1)
    backdated = conn.sql(
        "select count(*) from (select distinct journal_id, account_id, currency, layer "
        "from marts.fct_entries where recorded_at <= ?)",
        params=[cutoff],
    ).fetchone()[0]
    r = reconcile(as_of=cutoff.isoformat(), **paths)
    leaf = r["controls"][0]
    assert leaf["rows_compared"] == backdated
    assert leaf["rows_by_status"] == ({"missing_in_cala": backdated} if backdated else {})
    assert len(r["mismatches"]) == backdated
    assert all(m["control"] == "assert_balances_reconcile" and m["cala_dr_balance"] is None for m in r["mismatches"])
    assert r["is_reconciled"] is (backdated == 0)


def test_reconcile_limit_flags_truncation(paths, conn):
    first_write = conn.sql("select min(recorded_at) from raw.cala_balance_history").fetchone()[0]
    cutoff = first_write.replace(microsecond=0) - dt.timedelta(seconds=1)
    full = reconcile(as_of=cutoff.isoformat(), **paths)
    if not full["mismatches"]:
        pytest.skip("no backdated entries in these fixtures")
    r = reconcile(as_of=cutoff.isoformat(), limit=1, **paths)
    assert len(r["mismatches"]) == 1
    assert r["mismatches_truncated"] is True and r["is_reconciled"] is False
    assert r["controls"][0]["mismatch_count"] == full["controls"][0]["mismatch_count"]


def test_reconcile_rejects_bad_as_of(paths):
    with pytest.raises(ValueError, match="ISO-8601"):
        reconcile(as_of="last tuesday", **paths)
