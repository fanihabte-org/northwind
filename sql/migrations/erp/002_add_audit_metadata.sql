-- Northwind Systems :: ERP source database, migration 002
-- Add audit fields without rewriting historical ERP data. A later checkpointed
-- backfill will populate pre-existing rows before constraints are enforced.

ALTER TABLE erp.companies
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE erp.cost_centers
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE erp.gl_accounts
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

ALTER TABLE erp.fx_rates
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

-- Posted journal entries are immutable. Corrections are separate CRN or ADJ
-- postings, so they have a creation timestamp but no mutable updated_at field.
ALTER TABLE erp.revenue_postings
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS ix_companies_updated ON erp.companies (updated_at);
CREATE INDEX IF NOT EXISTS ix_cost_centers_updated ON erp.cost_centers (updated_at);
CREATE INDEX IF NOT EXISTS ix_gl_accounts_updated ON erp.gl_accounts (updated_at);
CREATE INDEX IF NOT EXISTS ix_fx_rates_updated ON erp.fx_rates (updated_at);
CREATE INDEX IF NOT EXISTS ix_postings_created ON erp.revenue_postings (created_at);
