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

# Verify the live database guard, not merely its migration text. The probe is
# wrapped in a transaction: the allowed corrective credit note is rolled back,
# while an UPDATE and DELETE must fail with the trigger's object-state code.
erp_posting_id="$(compose exec -T erp psql -U erp -d erp -tAc \
  "SELECT posting_id FROM erp.revenue_postings ORDER BY posting_id LIMIT 1")"
test -n "$erp_posting_id"

compose exec -T erp psql -v ON_ERROR_STOP=1 -U erp -d erp \
  -v posting_id="$erp_posting_id" <<'SQL'
SELECT set_config('northwind.ci_posting_id', :'posting_id', false);
DO $$
DECLARE
    target_posting_id BIGINT := current_setting('northwind.ci_posting_id')::BIGINT;
BEGIN
    BEGIN
        UPDATE erp.revenue_postings
        SET amount_doc = amount_doc
        WHERE posting_id = target_posting_id;
        RAISE EXCEPTION 'append-only UPDATE was unexpectedly accepted';
    EXCEPTION WHEN SQLSTATE '55000' THEN
        NULL;
    END;

    BEGIN
        DELETE FROM erp.revenue_postings WHERE posting_id = target_posting_id;
        RAISE EXCEPTION 'append-only DELETE was unexpectedly accepted';
    EXCEPTION WHEN SQLSTATE '55000' THEN
        NULL;
    END;
END;
$$;

BEGIN;
INSERT INTO erp.revenue_postings (
    posting_id, document_number, document_type, company_code, order_ref, gl_account,
    cost_center_code, posting_date, fiscal_period, document_currency, amount_doc,
    company_currency, amount_company, reverses_posting_id, posted_at, created_at
)
SELECT
    posting_id + 9000000000000, 'CI-CRN-' || posting_id, 'CRN', company_code,
    order_ref, gl_account, cost_center_code, posting_date, fiscal_period,
    document_currency, -amount_doc, company_currency, -amount_company, posting_id,
    posted_at, created_at
FROM erp.revenue_postings
WHERE posting_id = :'posting_id'::BIGINT;
ROLLBACK;
SQL
