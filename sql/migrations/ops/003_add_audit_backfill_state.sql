-- Northwind Systems :: Operations source database, migration 003
-- Durable checkpoints for the explicit, bounded historical audit-field backfill.

CREATE TABLE IF NOT EXISTS simulation.audit_backfill_progress (
    backfill_name VARCHAR(80) PRIMARY KEY,
    last_key VARCHAR(80),
    rows_scanned BIGINT NOT NULL DEFAULT 0,
    rows_updated BIGINT NOT NULL DEFAULT 0,
    completed BOOLEAN NOT NULL DEFAULT false,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ
);
