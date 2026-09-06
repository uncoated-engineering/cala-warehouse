# cala-warehouse — dbt package + DuckDB harness for cala-ledger event data.
#
# Everything runs locally against committed seeds; no cloud credentials needed.
#   make seed    load fixtures/seed/*.csv into DuckDB
#   make build   dbt build (models + tests)
#   make test    dbt test only
#
# `make fixtures` regenerates the seeds by running cala's own integration
# suite in Docker and dumping the resulting tables. Requires Docker and a
# checkout of cala at ../cala (override with CALA_DIR).

DBT      := uv run dbt
DBT_DIR  := dbt
TARGET   ?= duckdb
CALA_DIR ?= ../cala

.PHONY: install seed build test run clean fixtures docs

install:
	uv sync

seed:
	cd $(DBT_DIR) && $(DBT) seed --target $(TARGET)

run:
	cd $(DBT_DIR) && $(DBT) run --target $(TARGET)

test:
	cd $(DBT_DIR) && $(DBT) test --target $(TARGET)

build:
	cd $(DBT_DIR) && $(DBT) build --target $(TARGET)

docs:
	cd $(DBT_DIR) && $(DBT) docs generate --target $(TARGET)

clean:
	cd $(DBT_DIR) && $(DBT) clean
	rm -f $(DBT_DIR)/*.duckdb $(DBT_DIR)/*.duckdb.wal

fixtures:
	CALA_DIR=$(CALA_DIR) ./fixtures/generate.sh
