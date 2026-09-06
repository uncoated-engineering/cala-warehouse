#!/usr/bin/env bash
# Regenerate fixtures/seed/*.csv by running cala-ledger's own integration
# test suite against a throwaway Postgres and dumping what it leaves behind.
#
# The point: the seeds are produced by cala's code, not invented. Every
# account, journal, transaction, entry and balance row in fixtures/seed/ was
# written by cala-ledger's repositories during its test run, so the schemas
# and event sequences are exactly what a real deployment produces.
#
# Requirements: docker, psql, a cala checkout (default ../cala).
# Usage:        CALA_DIR=../cala ./fixtures/generate.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CALA_DIR="$(cd "${CALA_DIR:-$HERE/../../cala}" && pwd)"
SEED_DIR="$HERE/seed"
PG_PORT="${PG_PORT:-5433}"
PG_NAME="cala-fixture-pg"
RUST_IMAGE="${RUST_IMAGE:-rust:1-bookworm}"
PG_IMAGE="${PG_IMAGE:-postgres:18}"
PG_CON="postgres://user:password@127.0.0.1:${PG_PORT}/pg?sslmode=disable"

# Tables we ship as seeds. Two shapes, per es-entity's convention:
#   cala_<entity>          mutable current state
#   cala_<entity>_events   append-only event stream, UNIQUE(id, sequence)
# plus cala's own balance projections (what we reconcile against) and the
# outbox (the only place account-set membership changes are recorded as events).
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
  cala_persistent_outbox_events
)

cleanup() {
  docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> starting $PG_IMAGE on 127.0.0.1:$PG_PORT"
cleanup
docker run -d --name "$PG_NAME" \
  -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=pg \
  -p "127.0.0.1:${PG_PORT}:5432" "$PG_IMAGE" -c max_connections=300 >/dev/null
for _ in $(seq 1 60); do
  docker exec "$PG_NAME" pg_isready -U user -d pg >/dev/null 2>&1 && break
  sleep 1
done

CALA_REV="$(git -C "$CALA_DIR" rev-parse --short HEAD)"
CALA_VERSION="$(grep -m1 -E '^version' "$CALA_DIR/cala-ledger/Cargo.toml" | cut -d'"' -f2 || echo unknown)"
echo "==> running cala integration suite (cala $CALA_VERSION @ $CALA_REV) in $RUST_IMAGE"
echo "    first run compiles the workspace; expect several minutes"

# --security-opt label=disable: SELinux hosts (Fedora) otherwise deny the
# container access to the bind mount. It does not relabel the checkout.
# SQLX_OFFLINE: compile-time query checks use cala's committed .sqlx/ cache;
# the tests still run their migrations against the live database.
# Pin to the toolchain the image ships (rust-toolchain.toml says "stable",
# which rustup would otherwise try to download a second copy of).
RUST_TOOLCHAIN="$(docker run --rm "$RUST_IMAGE" rustup show active-toolchain | cut -d' ' -f1)"
TEST_LOG="$HERE/raw/cargo-test.log"
mkdir -p "$HERE/raw"
set +e
docker run --rm --network host --security-opt label=disable \
  -v "$CALA_DIR:/work" \
  -v cala-cargo-registry:/usr/local/cargo/registry \
  -v cala-cargo-target:/target \
  -w /work \
  -e RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN" -e CARGO_TARGET_DIR=/target -e SQLX_OFFLINE=true \
  -e PG_CON="$PG_CON" -e DATABASE_URL="$PG_CON" \
  "$RUST_IMAGE" cargo test --workspace --locked --no-fail-fast 2>&1 | tee "$TEST_LOG"
TEST_STATUS=${PIPESTATUS[0]}
set -e
echo "==> cargo test exit status: $TEST_STATUS (non-zero is tolerated; see manifest)"

echo "==> dumping ${#TABLES[@]} tables to $SEED_DIR"
mkdir -p "$SEED_DIR"
for t in "${TABLES[@]}"; do
  # Sort so re-generation produces stable diffs where the data allows it.
  case "$t" in
    *_events)                order="ORDER BY id, sequence" ;;
    cala_balance_history)    order="ORDER BY journal_id, account_id, currency, version" ;;
    cala_persistent_outbox_events) order="ORDER BY sequence" ;;
    cala_account_set_member_*) order="ORDER BY 1, 2" ;;
    *)                       order="ORDER BY id" ;;
  esac
  [[ "$t" == cala_current_balances ]] && order="ORDER BY journal_id, account_id, currency"
  psql "$PG_CON" -qAt -c "\\copy (SELECT * FROM $t $order) TO '$SEED_DIR/$t.csv' WITH (FORMAT csv, HEADER true)"
  printf '    %-42s %8s rows\n' "$t" "$(($(wc -l < "$SEED_DIR/$t.csv") - 1))"
done

# Provenance: which cala produced these rows and what its own suite reported.
CALA_VERSION="$CALA_VERSION" CALA_REV="$CALA_REV" RUST_IMAGE="$RUST_IMAGE" \
  PG_IMAGE="$PG_IMAGE" TEST_STATUS="$TEST_STATUS" "$HERE/manifest.sh"

# The DDL the extractor tests recreate these tables from, kept in step with
# the seeds (same cala checkout).
CALA_DIR="$CALA_DIR" PG_IMAGE="$PG_IMAGE" "$HERE/schema.sh"
