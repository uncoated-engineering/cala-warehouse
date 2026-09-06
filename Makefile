# cala-warehouse — dbt package + DuckDB harness for cala-ledger event data.
#
# Everything runs locally against committed seeds; no cloud credentials needed.
#   make seed    load fixtures/seed/*.csv into DuckDB
#   make build   dbt build (models + tests)
#   make test    dbt test only
#   make build FULL_REFRESH=1   rebuild incremental models from scratch
#   make mcp     start the read-only MCP server (stdio) over the built DuckDB file
#   make mcp-test  pytest for the MCP tools; needs `make build` first
#
# `make fixtures` regenerates the seeds by running cala's own integration
# suite in Docker and dumping the resulting tables. Requires Docker and a
# checkout of cala at ../cala (override with CALA_DIR).

DBT      := uv run dbt
DBT_DIR  := dbt
TARGET   ?= duckdb
CALA_DIR ?= ../cala

# stg_cala_entries is incremental. After the seeds are REPLACED (make
# fixtures), the incremental table still holds the previous batch and must be
# rebuilt: `make build FULL_REFRESH=1`. CI always starts from an empty
# database, so it never needs this.
FULL_REFRESH ?=
DBT_FLAGS    := --target $(TARGET) $(if $(FULL_REFRESH),--full-refresh,)

.PHONY: install seed build test run clean fixtures docs mcp mcp-test

install:
	uv sync

seed:
	cd $(DBT_DIR) && $(DBT) seed $(DBT_FLAGS)

run:
	cd $(DBT_DIR) && $(DBT) run $(DBT_FLAGS)

test:
	cd $(DBT_DIR) && $(DBT) test --target $(TARGET)

# Seeds stand in for the extractor's tables, and staging reads them through
# source(), which dbt does not link to seeds in the DAG. So load seeds first,
# then build everything else; a single `dbt build` can race on a fresh database.
build:
	cd $(DBT_DIR) && $(DBT) seed $(DBT_FLAGS)
	cd $(DBT_DIR) && $(DBT) build $(DBT_FLAGS) --exclude resource_type:seed

docs:
	cd $(DBT_DIR) && $(DBT) docs generate --target $(TARGET)

clean:
	cd $(DBT_DIR) && $(DBT) clean
	rm -f $(DBT_DIR)/*.duckdb $(DBT_DIR)/*.duckdb.wal

fixtures:
	CALA_DIR=$(CALA_DIR) ./fixtures/generate.sh

# The MCP server reads dbt/target/manifest.json and dbt/cala_warehouse.duckdb
# (read-only), both written by `make build`. Talks MCP over stdio; point an
# MCP client at `uv run cala-mcp` with this directory as cwd.
mcp:
	uv run cala-mcp

mcp-test:
	uv run pytest
