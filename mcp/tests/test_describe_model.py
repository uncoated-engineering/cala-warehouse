import json

import pytest

from cala_mcp.manifest import Manifest, ManifestMissing, UnknownColumn, UnknownRelation
from cala_mcp.tools import describe_model, list_models


def test_manifest_missing_is_reported_not_guessed(tmp_path):
    with pytest.raises(ManifestMissing, match="make build"):
        Manifest.load(tmp_path / "manifest.json")


def test_manifest_indexes_models_sources_and_grain(manifest_path):
    m = Manifest.load(manifest_path)
    assert "fct_account_balances" in m.names("model")
    fab = m.model("fct_account_balances")
    assert fab.grain == ("journal_id", "account_id", "currency", "layer")
    assert fab.relation_name.endswith('"marts"."fct_account_balances"')
    assert "fct_entries" in fab.upstream
    assert "rpt_trial_balance" in fab.downstream
    assert "assert_balances_reconcile" in fab.tests
    src = m.source("cala_balance_history")
    assert src.resource_type == "source"
    assert "values" in src.columns


def test_require_refuses_undeclared_columns(manifest_path):
    m = Manifest.load(manifest_path)
    with pytest.raises(UnknownColumn, match="not_a_column"):
        m.model("fct_entries").require("entry_id", "not_a_column")
    with pytest.raises(UnknownRelation):
        m.model("fct_nothing")


def test_describe_model_reads_columns_types_and_lineage(paths):
    d = describe_model("fct_account_balances", **paths)
    assert d["grain"] == ["journal_id", "account_id", "currency", "layer"]
    cols = {c["name"]: c for c in d["columns"]}
    assert cols["dr_balance"]["data_type"] == "DECIMAL(38,18)"
    assert cols["dr_balance"]["description"]
    assert "not_null" in cols["dr_balance"]["tests"]
    assert d["undocumented_columns"] == []
    assert d["lineage"]["upstream"] == ["fct_entries", "stg_cala_accounts"]
    assert "assert_balances_reconcile" in d["lineage"]["tests"]
    assert d["manifest"]["dbt_version"]
    json.dumps(d)  # JSON-native throughout


def test_describe_model_works_from_manifest_alone(manifest_path, tmp_path):
    d = describe_model("rpt_trial_balance", manifest_path=manifest_path, duckdb_path=tmp_path / "none.duckdb")
    assert d["columns"] and all(c["data_type"] is None for c in d["columns"])
    assert "not found" in d["warehouse_note"]


def test_every_mart_documents_every_physical_column(paths):
    """The tools only see declared columns, so a physical column without a yml entry is a gap."""
    m = Manifest.load(paths["manifest_path"])
    gaps = {}
    for name in m.names("model"):
        if m.model(name).schema != "marts":
            continue
        d = describe_model(name, **paths)
        if d["undocumented_columns"]:
            gaps[name] = d["undocumented_columns"]
    assert gaps == {}


def test_list_models(manifest_path):
    models = list_models(manifest_path=manifest_path)
    names = {m["name"] for m in models}
    assert {"fct_entries", "fct_account_balances", "rpt_trial_balance", "stg_cala_balances"} <= names
