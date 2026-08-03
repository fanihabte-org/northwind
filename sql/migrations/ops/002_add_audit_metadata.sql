-- Northwind Systems :: Operations source database, migration 002
--
-- Introduce record-history fields without rewriting existing large tables.
-- A separate, checkpointed backfill will populate historical rows before this
-- project enforces NOT NULL constraints. New seed loads and simulator writes
-- populate both fields from this release onward.

ALTER TABLE ops.products
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;

ALTER TABLE ops.warehouses
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE ops.carriers
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE ops.order_lines
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;

ALTER TABLE ops.shipments
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE ops.invoices
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE ops.support_cases
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS ix_products_updated ON ops.products (updated_at);
CREATE INDEX IF NOT EXISTS ix_warehouses_updated ON ops.warehouses (updated_at);
CREATE INDEX IF NOT EXISTS ix_carriers_updated ON ops.carriers (updated_at);
CREATE INDEX IF NOT EXISTS ix_shipments_updated ON ops.shipments (updated_at);
CREATE INDEX IF NOT EXISTS ix_invoices_updated ON ops.invoices (updated_at);
CREATE INDEX IF NOT EXISTS ix_cases_updated ON ops.support_cases (updated_at);
