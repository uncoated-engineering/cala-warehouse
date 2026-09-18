"""End to end through the MCP protocol with an in-process client."""

import asyncio
import json

import duckdb
import pytest

from cala_mcp.server import build_server

TOOLS = {"describe_model", "list_models", "trial_balance", "explain_account_balance", "reconcile", "erasures"}


def call(tool, **arguments):
    from mcp.client import Client

    async def go():
        async with Client(build_server()) as client:
            return await client.call_tool(tool, arguments)

    return asyncio.run(go())


def test_server_exposes_exactly_the_read_only_tools(paths):
    from mcp.client import Client

    async def go():
        async with Client(build_server()) as client:
            return await client.list_tools()

    listed = asyncio.run(go()).tools
    assert {t.name for t in listed} == TOOLS
    for t in listed:
        assert t.description, t.name
        assert "sql" not in t.input_schema["properties"], t.name


def test_describe_model_over_the_wire(paths):
    result = call("describe_model", name="rpt_trial_balance")
    assert not result.is_error
    body = result.structured_content
    assert body["grain"] == ["journal_id", "currency", "layer"]
    names = {c["name"] for c in body["columns"]}
    assert {"total_debits", "total_credits", "difference"} <= names


def test_reconcile_over_the_wire_is_green(paths):
    result = call("reconcile")
    assert not result.is_error
    assert result.structured_content["mismatches"] == []


def test_trial_balance_decimals_arrive_as_strings(paths, duckdb_path):
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    journal = conn.sql("select journal_id from marts.rpt_trial_balance limit 1").fetchone()[0]
    conn.close()
    result = call("trial_balance", journal_id=journal)
    assert not result.is_error
    text = json.loads(result.content[0].text)
    row = text["rows"][0]
    assert isinstance(row["total_debits"], str) and isinstance(row["difference"], str)


def test_unknown_model_is_an_error_not_a_guess(paths):
    result = call("describe_model", name="fct_made_up")
    assert result.is_error
    assert "not a model in the manifest" in result.content[0].text
