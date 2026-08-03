-- Northwind Systems :: Operations source database, migration 004
-- Apply only after audit_backfill_progress reports every target completed and
-- validation confirms no null or reverse-ordered timestamps.

ALTER TABLE ops.products
    ADD CONSTRAINT ck_products_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.warehouses
    ADD CONSTRAINT ck_warehouses_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.carriers
    ADD CONSTRAINT ck_carriers_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.order_lines
    ADD CONSTRAINT ck_order_lines_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.shipments
    ADD CONSTRAINT ck_shipments_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.invoices
    ADD CONSTRAINT ck_invoices_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE ops.support_cases
    ADD CONSTRAINT ck_support_cases_audit_dates CHECK (updated_at >= created_at) NOT VALID;

ALTER TABLE ops.products VALIDATE CONSTRAINT ck_products_audit_dates;
ALTER TABLE ops.warehouses VALIDATE CONSTRAINT ck_warehouses_audit_dates;
ALTER TABLE ops.carriers VALIDATE CONSTRAINT ck_carriers_audit_dates;
ALTER TABLE ops.order_lines VALIDATE CONSTRAINT ck_order_lines_audit_dates;
ALTER TABLE ops.shipments VALIDATE CONSTRAINT ck_shipments_audit_dates;
ALTER TABLE ops.invoices VALIDATE CONSTRAINT ck_invoices_audit_dates;
ALTER TABLE ops.support_cases VALIDATE CONSTRAINT ck_support_cases_audit_dates;

ALTER TABLE ops.products
    ALTER COLUMN created_at SET NOT NULL;
ALTER TABLE ops.warehouses
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE ops.carriers
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE ops.order_lines
    ALTER COLUMN created_at SET NOT NULL;
ALTER TABLE ops.shipments
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE ops.invoices
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE ops.support_cases
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
