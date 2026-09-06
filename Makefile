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
# Extraction (from a live cala Postgres instead of seeds; needs CALA_PG_URL):
#   make extract           land cala's tables in dbt/cala_warehouse.duckdb (raw schema)
#   make build-extracted   models + tests on what `make extract` landed, no seeds
#   make extract-test      pytest for the extractor; needs a throwaway Postgres
#   make fixture-pg        start a Docker Postgres loaded with the seeds, for the two above
#
# `make fixtures` regenerates the seeds by running cala's own integration
# suite in Docker and dumping the resulting tables. Requires Docker and a
# checkout of cala at ../cala (override with CALA_DIR).

DBT      := uv run dbt
DBT_DIR  := dbt
TARGET   ?= duckdb
CALA_DIR ?= ../cala

# A throwaway Postgres for the extractor: `make fixture-pg` starts it and
# loads fixtures/schema.sql + fixtures/seed/ into it. Point CALA_PG_URL at a
# real cala database instead to extract from it.
FIXTURE_PG_PORT ?= 5434
FIXTURE_PG_URL  := postgres://user:password@127.0.0.1:$(FIXTURE_PG_PORT)/pg
CALA_PG_URL     ?= $(FIXTURE_PG_URL)
export CALA_PG_URL

# stg_cala_entries is incremental. After the seeds are REPLACED (make
# fixtures), the incremental table still holds the previous batch and must be
# rebuilt: `make build FULL_REFRESH=1`. CI always starts from an empty
# database, so it never needs this.
FULL_REFRESH ?=
DBT_FLAGS    := --target $(TARGET) $(if $(FULL_REFRESH),--full-refresh,)

.PHONY: install seed build test run clean fixtures docs mcp mcp-test \
        extract build-extracted extract-test fixture-pg fixture-pg-stop

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

# Also drops dlt's local pipeline state (.dlt/). The extractor would recover
# on its own (the watermark follows the warehouse, not the local file), but a
# clean slate should look like one.
clean:
	cd $(DBT_DIR) && $(DBT) clean
	rm -f $(DBT_DIR)/*.duckdb $(DBT_DIR)/*.duckdb.wal
	rm -rf .dlt

fixtures:
	CALA_DIR=$(CALA_DIR) ./fixtures/generate.sh

# The MCP server reads dbt/target/manifest.json and dbt/cala_warehouse.duckdb
# (read-only), both written by `make build`. Talks MCP over stdio; point an
# MCP client at `uv run cala-mcp` with this directory as cwd.
mcp:
	uv run cala-mcp

mcp-test:
	uv run pytest mcp/tests

# --- extraction -------------------------------------------------------------
# `make extract` lands the raw tables where dbt seeds would have put them, so
# do not mix the two in one DuckDB file: `make clean` first when switching.
# Every run is idempotent; the streams are loaded incrementally from the
# outbox watermark, the state tables are replaced. See extract/cala_extract/.
extract:
	uv run cala-extract --verify $(EXTRACT_FLAGS)

build-extracted:
	cd $(DBT_DIR) && $(DBT) build $(DBT_FLAGS) --exclude resource_type:seed

extract-test:
	uv run pytest extract/tests

fixture-pg:
	docker rm -f cala-fixture-pg >/dev/null 2>&1 || true
	docker run -d --name cala-fixture-pg \
	  -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=pg \
	  -p 127.0.0.1:$(FIXTURE_PG_PORT):5432 postgres:18 >/dev/null
	@until docker exec cala-fixture-pg pg_isready -U user -d pg >/dev/null 2>&1; do sleep 1; done
	uv run python -m cala_extract.fixture_db $(FIXTURE_PG_URL)

fixture-pg-stop:
	docker rm -f cala-fixture-pg >/dev/null 2>&1 || true
