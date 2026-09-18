"""The quality KPIs over the built warehouse.

The hooks record an invocation after it ends, so the KPI marts describe
previous builds. CI runs `make build` twice before this suite; locally a
single build leaves the marts empty and the row-level assertions skip.
"""

import duckdb
import pytest

from cala_mcp.tools import describe_model, quality_kpis


def test_quality_marts_document_every_physical_column(paths):
    for name in ("rpt_quality_kpis", "fct_test_results", "fct_row_count_drift"):
        described = describe_model(name, **paths)
        assert described["undocumented_columns"] == [], name
        assert described["grain"], name


def test_quality_kpis_shape(paths):
    result = quality_kpis(3, **paths)
    assert result["grain"] == ["invocation_id"]
    assert isinstance(result["runs"], list) and len(result["runs"]) <= 3
    assert isinstance(result["latest_failing_tests"], list)
    assert isinstance(result["latest_drift_outliers"], list)
    if not result["runs"]:
        assert "no runs recorded yet" in result["note"]
        assert result["latest_invocation_id"] is None


def test_latest_recorded_build_passed_every_control(paths, duckdb_path):
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    recorded = conn.execute("select count(*) from marts.rpt_quality_kpis").fetchone()[0]
    conn.close()
    if recorded == 0:
        pytest.skip("no invocation recorded in the marts yet; build twice")

    result = quality_kpis(1, **paths)
    latest = result["runs"][0]
    assert latest["invocation_id"] == result["latest_invocation_id"]
    # pass_rate is a ratio, not an amount: a float on DuckDB, in [0, 1]
    assert 0 <= float(latest["pass_rate"]) <= 1
    assert latest["controls_run"] >= 5
    assert latest["all_controls_passed"] is True
    assert result["latest_failing_tests"] == []
    assert latest["outbox_max_sequence"] == 5207
    # the fixtures' newest outbox row is dated 2026-09-06; the lag is days, not negative
    assert float(latest["outbox_freshness_lag_s"]) > 0
