-- Northwind Systems :: ERP source database, migration 004
-- Apply only after the ERP audit backfill reports every target completed and
-- validation confirms no null or reverse-ordered timestamps.

ALTER TABLE erp.companies
    ADD CONSTRAINT ck_companies_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE erp.cost_centers
    ADD CONSTRAINT ck_cost_centers_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE erp.gl_accounts
    ADD CONSTRAINT ck_gl_accounts_audit_dates CHECK (updated_at >= created_at) NOT VALID;
ALTER TABLE erp.fx_rates
    ADD CONSTRAINT ck_fx_rates_audit_dates CHECK (updated_at >= created_at) NOT VALID;

-- The ledger is immutable in this model.  A journal row is recorded when it is
-- posted; later financial corrections are new CRN or ADJ rows, not UPDATEs.
ALTER TABLE erp.revenue_postings
    ADD CONSTRAINT ck_revenue_postings_created_at CHECK (created_at = posted_at) NOT VALID;

ALTER TABLE erp.companies VALIDATE CONSTRAINT ck_companies_audit_dates;
ALTER TABLE erp.cost_centers VALIDATE CONSTRAINT ck_cost_centers_audit_dates;
ALTER TABLE erp.gl_accounts VALIDATE CONSTRAINT ck_gl_accounts_audit_dates;
ALTER TABLE erp.fx_rates VALIDATE CONSTRAINT ck_fx_rates_audit_dates;
ALTER TABLE erp.revenue_postings VALIDATE CONSTRAINT ck_revenue_postings_created_at;

ALTER TABLE erp.companies
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE erp.cost_centers
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE erp.gl_accounts
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE erp.fx_rates
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE erp.revenue_postings
    ALTER COLUMN created_at SET NOT NULL;
