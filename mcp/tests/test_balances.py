import json
from decimal import Decimal

import duckdb
import pytest

from cala_mcp.manifest import UnknownColumn
from cala_mcp.tools import explain_account_balance, parse_as_of, trial_balance


@pytest.fixture(scope="module")
def conn(duckdb_path):
    c = duckdb.connect(str(duckdb_path), read_only=True)
    yield c
    c.close()


@pytest.fixture(scope="module")
def busiest_journal(conn):
    return conn.sql(
        "select journal_id from marts.rpt_trial_balance group by 1 order by sum(entry_count) desc, 1 limit 1"
    ).fetchone()[0]


@pytest.fixture(scope="module")
def busiest_balance(conn):
    row = conn.sql(
        "select journal_id, account_id, currency, layer, entry_count from marts.fct_account_balances "
        "order by entry_count desc, account_id limit 1"
    ).fetchone()
    return dict(zip(["journal_id", "account_id", "currency", "layer", "entry_count"], row))


def test_parse_as_of():
    assert parse_as_of(None) is None
    assert parse_as_of("2026-01-31T12:00:00").isoformat() == "2026-01-31T12:00:00"
    assert parse_as_of("2026-01-31T12:00:00+02:00").isoformat() == "2026-01-31T10:00:00"
    with pytest.raises(ValueError, match="ISO-8601"):
        parse_as_of("yesterday")


def test_trial_balance_matches_the_report(paths, conn, busiest_journal):
    tb = trial_balance(busiest_journal, **paths)
    assert tb["source"].startswith("rpt_trial_balance")
    assert tb["is_balanced"] is True
    assert tb["grain"] == ["currency", "layer"]
    expected = conn.sql(
        "select currency, layer, total_debits, total_credits from marts.rpt_trial_balance "
        "where journal_id = ? order by 1, 2",
        params=[busiest_journal],
    ).fetchall()
    got = [(r["currency"], r["layer"], Decimal(r["total_debits"]), Decimal(r["total_credits"])) for r in tb["rows"]]
    assert got == expected
    assert all(isinstance(r["total_debits"], str) for r in tb["rows"])
    assert all(r["difference"] == "0" for r in tb["rows"])
    json.dumps(tb)


def test_trial_balance_as_of_recomputes_from_entries(paths, conn, busiest_journal):
    last = conn.sql(
        "select max(recorded_at) from marts.fct_entries where journal_id = ?", params=[busiest_journal]
    ).fetchone()[0]
    now = trial_balance(busiest_journal, **paths)
    at_end = trial_balance(busiest_journal, as_of=last.isoformat(), **paths)
    assert at_end["source"].startswith("recomputed from fct_entries")
    assert at_end["as_of"] == last.isoformat()
    strip = lambda rows: [{k: r[k] for k in ("currency", "layer", "entry_count", "total_debits", "total_credits", "difference")} for r in rows]
    assert strip(at_end["rows"]) == strip(now["rows"])

    first = conn.sql(
        "select min(recorded_at) from marts.fct_entries where journal_id = ?", params=[busiest_journal]
    ).fetchone()[0]
    early = trial_balance(busiest_journal, as_of=first.isoformat(), **paths)
    assert early["is_balanced"] is True
    assert sum(r["entry_count"] for r in early["rows"]) < sum(r["entry_count"] for r in now["rows"])

    before = trial_balance(busiest_journal, as_of="2000-01-01T00:00:00", **paths)
    assert before["rows"] == [] and before["is_balanced"] is None


def test_trial_balance_unknown_journal(paths):
    tb = trial_balance("not-a-journal", **paths)
    assert tb["rows"] == [] and "no rows" in tb["note"]


def test_explain_account_balance_walks_the_chain(paths, busiest_balance):
    b = busiest_balance
    out = explain_account_balance(b["account_id"], b["currency"], b["layer"], journal_id=b["journal_id"], **paths)
    assert out["account"]["account_id"] == b["account_id"]
    assert len(out["balances"]) == 1
    bal = out["balances"][0]
    assert bal["entry_count"] == b["entry_count"] == bal["entries_shown"]
    assert bal["entries_truncated"] is False
    assert bal["chain_reconciles"] is True
    assert bal["cala"]["matches"] is True
    chain = bal["entries"]
    assert [e["position"] for e in chain] == list(range(1, len(chain) + 1))
    assert chain[-1]["entry_id"] == bal["latest_entry_id"]
    # running totals are consistent step by step and land on the balance
    dr = cr = Decimal(0)
    for e in chain:
        if e["direction"] == "debit":
            dr += Decimal(e["units"])
        else:
            cr += Decimal(e["units"])
        assert Decimal(e["running_dr"]) == dr and Decimal(e["running_cr"]) == cr
    assert Decimal(bal["dr_balance"]) == dr and Decimal(bal["cr_balance"]) == cr
    signed = dr - cr if out["account"]["normal_balance_type"] == "debit" else cr - dr
    assert Decimal(bal["balance"]) == signed == Decimal(chain[-1]["running_balance"])
    json.dumps(out)


def test_explain_account_balance_limit_keeps_totals(paths, busiest_balance):
    b = busiest_balance
    out = explain_account_balance(b["account_id"], b["currency"], b["layer"], journal_id=b["journal_id"], limit=3, **paths)
    bal = out["balances"][0]
    assert bal["entries_shown"] == 3 and bal["entries_truncated"] is True
    assert bal["chain_reconciles"] is True
    assert bal["entries"][-1]["running_dr"] == bal["dr_balance"]


def test_explain_account_balance_across_journals(paths, conn):
    row = conn.sql(
        "select account_id, currency, layer, count(*) from marts.fct_account_balances "
        "group by 1, 2, 3 having count(*) > 1 order by 4 desc, 1 limit 1"
    ).fetchone()
    if row is None:
        pytest.skip("no account holds the same (currency, layer) in two journals")
    out = explain_account_balance(row[0], row[1], row[2], **paths)
    assert len(out["balances"]) == row[3]
    assert len({b["journal_id"] for b in out["balances"]}) == row[3]


def test_explain_account_balance_rejects_unknown_layer(paths, busiest_balance):
    with pytest.raises(UnknownColumn, match="accepts"):
        explain_account_balance(busiest_balance["account_id"], "USD", "settled_or_not", **paths)


def test_explain_account_balance_on_account_set(paths, conn):
    set_id = conn.sql("select account_set_id from marts.dim_account_sets limit 1").fetchone()[0]
    out = explain_account_balance(set_id, "USD", "settled", **paths)
    assert out["balances"] == []
    assert "rollup" in out["note"]


def test_explain_unknown_account(paths):
    out = explain_account_balance("nope", "USD", "settled", **paths)
    assert out["account"] is None and "not in dim_accounts" in out["note"]
