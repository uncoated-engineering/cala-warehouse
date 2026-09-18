#!/usr/bin/env bash
# Write fixtures/schema.sql: the DDL of the tables we extract, exactly as
# cala-ledger's migrations create them, dumped with pg_dump from a throwaway
# Postgres. The extraction tests (extract/tests/) and CI apply this file to a
# fresh database and COPY the seeds in, so the extractor is exercised against
# cala's real column types, constraints and the partitioned outbox rather than
# a hand-written imitation.
#
# Called by generate.sh; can also be run on its own.
# Requirements: docker, psql, pg_dump, a cala checkout (default ../cala).
# Usage:        CALA_DIR=../cala ./fixtures/schema.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALA_DIR="$(cd "${CALA_DIR:-$HERE/../../cala}" && pwd)"
OUT="$HERE/schema.sql"
PG_PORT="${PG_PORT:-5435}"
PG_NAME="cala-schema-pg"
PG_IMAGE="${PG_IMAGE:-postgres:18}"
PG_CON="postgres://user:password@127.0.0.1:${PG_PORT}/pg?sslmode=disable"

# Same list as generate.sh. The outbox is RANGE-partitioned, so its partitions
# come along via the wildcard.
TABLES=(
  cala_journals            cala_journal_events
  cala_accounts            cala_account_events
  cala_account_sets        cala_account_set_events
  cala_account_set_member_accounts
  cala_account_set_member_account_sets
  cala_tx_templates        cala_tx_template_events
  cala_transactions        cala_transaction_events
  cala_entries             cala_entry_events
  cala_current_balances    cala_balance_history
  'cala_persistent_outbox_events*'
)

cleanup() { docker rm -f "$PG_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> starting $PG_IMAGE on 127.0.0.1:$PG_PORT"
cleanup
docker run -d --name "$PG_NAME" \
  -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=pg \
  -p "127.0.0.1:${PG_PORT}:5432" "$PG_IMAGE" >/dev/null
for _ in $(seq 1 60); do
  docker exec "$PG_NAME" pg_isready -U user -d pg >/dev/null 2>&1 && break
  sleep 1
done

CALA_REV="$(git -C "$CALA_DIR" rev-parse --short HEAD)"
echo "==> applying cala-ledger migrations (cala @ $CALA_REV)"
for f in "$CALA_DIR"/cala-ledger/migrations/*.sql; do
  psql "$PG_CON" -q -v ON_ERROR_STOP=1 -f "$f"
done

echo "==> dumping DDL to $OUT"
args=()
for t in "${TABLES[@]}"; do args+=(-t "$t"); done
{
  echo "-- DDL for the tables cala-warehouse extracts, dumped by fixtures/schema.sh"
  echo "-- from cala-ledger's migrations (cala @ $CALA_REV, pg_dump $(pg_dump --version | awk '{print $3}'))."
  echo "-- Do not edit: regenerate with ./fixtures/schema.sh."
  # Drop pg_dump's session settings and psql meta-commands (\restrict /
  # \unrestrict) so the file is plain SQL that any client can execute.
  # `-t` selects tables only; the enum types cala_accounts uses must be
  # dumped separately from the whole schema. TYPE blocks end at ");".
  pg_dump "$PG_CON" --schema-only --no-owner --no-privileges --no-comments \
    | awk '/^CREATE TYPE/{p=1} p{print} p&&/\);/{p=0; print ""}'
  pg_dump "$PG_CON" --schema-only --no-owner --no-privileges --no-comments "${args[@]}" \
    | grep -vE '^(SET |SELECT pg_catalog\.set_config|\\(un)?restrict|--)' \
    | cat -s
} > "$OUT"
echo "==> wrote $OUT ($(grep -c 'CREATE TABLE' "$OUT") tables)"
