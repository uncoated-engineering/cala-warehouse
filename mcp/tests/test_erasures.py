"""The erasures tool over the built warehouse, grounded in fct_erasures like
everything else. On the seeds nothing is erased; on the CI extract job an
account and its entries are, so the test reads the log first."""

import duckdb

from cala_mcp.tools import erasures, describe_model


def test_fct_erasures_is_documented_column_for_column(paths):
    body = describe_model("fct_erasures", **paths)
    assert body["grain"] == ["erasure_id"]
    assert body["undocumented_columns"] == []
    names = {c["name"] for c in body["columns"]}
    assert {"entity_kind", "entity_id", "erasure_source", "reapply_count", "rows_redacted_total"} <= names


def test_erasures_reports_the_log_or_says_it_is_empty(paths, duckdb_path):
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    logged = conn.execute(
        "select erasure_id, entity_id, erasure_source from marts.fct_erasures order by first_applied_at, erasure_id"
    ).fetchall()
    conn.close()

    body = erasures(**paths)
    assert body["requests_total"] == len(logged)
    if not logged:
        assert body["erasures"] == [] and "nothing has been erased" in body["note"]
        one = erasures("00000000-0000-0000-0000-000000000001", **paths)
        assert one["erasures"] == [] and "no erasure on record" in one["note"]
        return

    # the request rows come back newest first, each with what it touched
    assert {e["erasure_id"] for e in body["erasures"]} <= {r[0] for r in logged}
    assert all(e["tables_touched"] >= 1 and isinstance(e["rows_redacted_total"], int) for e in body["erasures"])
    root = next(r for r in logged if r[2] != "cascade")
    one = erasures(root[1], **paths)
    assert [e["erasure_id"] for e in one["erasures"]] == [root[0]]
    assert one["erasures"][0]["erasure_source"] in ("operator", "forget_event")
    assert "note" not in one
