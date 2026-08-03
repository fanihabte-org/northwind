#!/usr/bin/env bash
# Exercise the production-shaped source bootstrap on an isolated CI Compose
# project. This must never target the developer's persistent `northwind` stack.
set -euo pipefail

readonly PROJECT_NAME="northwind-source-integration"
readonly BASELINE_DATE="2026-07-24"
readonly THROUGH_DATE="2026-08-24"
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly EXPORTS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/northwind-fakeforce-exports.XXXXXX")"

compose() {
  docker compose --project-directory "$ROOT_DIR" --project-name "$PROJECT_NAME" "$@"
}

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    compose ps || true
    compose logs --no-color || true
  fi
  compose down --volumes --remove-orphans || true
  rm -rf "$EXPORTS_DIR"
  exit "$status"
}
trap cleanup EXIT

export FAKEFORCE_EXPORTS_DIR="$EXPORTS_DIR"
export SIMULATION_BASELINE_DATE="$BASELINE_DATE"
export SIMULATION_SEED="20260728"

cd "$ROOT_DIR"

# The migration container uses the same images and dependencies as deployment.
compose up -d ops erp migrations
compose wait migrations

# The loader is intentionally one-time and runs after migrations have completed.
python generator/load.py

# Run an actual containerized catch-up, including the durable simulation state
# and cross-system reconciliation. The end date leaves enough days for CRM
# opportunities to become Ops orders, invoices, and ERP postings.
compose run --rm --no-deps simulator \
  python -m simulator.scheduler \
    --baseline "$BASELINE_DATE" \
    --through "$THROUGH_DATE" \
    --seed "$SIMULATION_SEED"

ops_events="$(compose exec -T ops psql -U ops -d ops -tAc \
  "SELECT count(*) FROM simulation.applied_events")"
erp_events="$(compose exec -T erp psql -U erp -d erp -tAc \
  "SELECT count(*) FROM simulation.applied_events")"

test "$ops_events" -gt 0
test "$erp_events" -gt 0
