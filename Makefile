# cala-warehouse — dbt package + DuckDB harness for cala-ledger event data.
#
# Everything runs locally against committed seeds; no cloud credentials needed.
#   make seed    load fixtures/seed/*.csv into DuckDB
#   make build   dbt build (models + tests)
#   make test    dbt test only
#   make build FULL_REFRESH=1   rebuild incremental models from scratch
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

.PHONY: install seed build test run clean fixtures docs

install:
	uv sync

seed:
	cd $(DBT_DIR) && $(DBT) seed $(DBT_FLAGS)

run:
	cd $(DBT_DIR) && $(DBT) run $(DBT_FLAGS)

test:
	cd $(DBT_DIR) && $(DBT) test --target $(TARGET)

build:
	cd $(DBT_DIR) && $(DBT) build $(DBT_FLAGS)

docs:
	cd $(DBT_DIR) && $(DBT) docs generate --target $(TARGET)

clean:
	cd $(DBT_DIR) && $(DBT) clean
	rm -f $(DBT_DIR)/*.duckdb $(DBT_DIR)/*.duckdb.wal

fixtures:
	CALA_DIR=$(CALA_DIR) ./fixtures/generate.sh
