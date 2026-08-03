-- Northwind Systems :: ERP source database
-- Finance owns its entity master data and revenue ledger locally.
-- Deliberately non-destructive: use an explicit reset procedure for test data.

CREATE SCHEMA IF NOT EXISTS erp;
CREATE SCHEMA IF NOT EXISTS simulation;

CREATE TABLE IF NOT EXISTS simulation.applied_events (
    event_id VARCHAR(64) PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    business_date DATE NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    CONSTRAINT ck_applied_events_system CHECK (source_system IN ('crm', 'ops', 'erp'))
);

CREATE TABLE IF NOT EXISTS erp.companies (
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(120) NOT NULL,
    functional_currency CHAR(3) NOT NULL,
    country VARCHAR(60) NOT NULL,
    CONSTRAINT pk_companies PRIMARY KEY (company_code)
);

CREATE TABLE IF NOT EXISTS erp.cost_centers (
    cost_center_code VARCHAR(20) NOT NULL,
    cost_center_name VARCHAR(120) NOT NULL,
    company_code VARCHAR(10) NOT NULL,
    region VARCHAR(20) NOT NULL,
    function VARCHAR(40) NOT NULL,
    owner_email VARCHAR(160) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_active BOOLEAN NOT NULL,
    CONSTRAINT pk_cost_centers PRIMARY KEY (cost_center_code),
    CONSTRAINT fk_cc_company FOREIGN KEY (company_code) REFERENCES erp.companies (company_code),
    CONSTRAINT ck_cc_validity CHECK (valid_to IS NULL OR valid_to > valid_from),
    CONSTRAINT ck_cc_active CHECK ((is_active AND valid_to IS NULL) OR NOT is_active)
);

CREATE TABLE IF NOT EXISTS erp.gl_accounts (
    gl_account VARCHAR(10) NOT NULL,
    gl_name VARCHAR(120) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    is_postable BOOLEAN NOT NULL,
    CONSTRAINT pk_gl_accounts PRIMARY KEY (gl_account),
    CONSTRAINT ck_gl_type CHECK (account_type IN ('REVENUE','ASSET','LIABILITY','EXPENSE','CLEARING'))
);

CREATE TABLE IF NOT EXISTS erp.fx_rates (
    from_currency CHAR(3) NOT NULL,
    to_currency CHAR(3) NOT NULL,
    rate_date DATE NOT NULL,
    rate_type VARCHAR(10) NOT NULL,
    rate NUMERIC(18,8) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_fx_rates PRIMARY KEY (from_currency, to_currency, rate_date, rate_type),
    CONSTRAINT ck_fx_rate CHECK (rate > 0),
    CONSTRAINT ck_fx_diff CHECK (from_currency <> to_currency),
    CONSTRAINT ck_fx_type CHECK (rate_type IN ('SPOT','AVG','CLOSE'))
);

CREATE TABLE IF NOT EXISTS erp.revenue_postings (
    posting_id BIGINT NOT NULL,
    document_number VARCHAR(24) NOT NULL,
    document_type VARCHAR(10) NOT NULL,
    company_code VARCHAR(10) NOT NULL,
    order_ref BIGINT,
    gl_account VARCHAR(10) NOT NULL,
    cost_center_code VARCHAR(20),
    posting_date DATE NOT NULL,
    fiscal_period VARCHAR(7) NOT NULL,
    document_currency CHAR(3) NOT NULL,
    amount_doc NUMERIC(18,2) NOT NULL,
    company_currency CHAR(3) NOT NULL,
    amount_company NUMERIC(18,2) NOT NULL,
    reverses_posting_id BIGINT,
    posted_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_postings PRIMARY KEY (posting_id),
    CONSTRAINT uq_postings_doc UNIQUE (company_code, document_number),
    CONSTRAINT fk_postings_co FOREIGN KEY (company_code) REFERENCES erp.companies (company_code),
    CONSTRAINT fk_postings_gl FOREIGN KEY (gl_account) REFERENCES erp.gl_accounts (gl_account),
    CONSTRAINT fk_postings_cc FOREIGN KEY (cost_center_code) REFERENCES erp.cost_centers (cost_center_code),
    CONSTRAINT fk_postings_rev FOREIGN KEY (reverses_posting_id) REFERENCES erp.revenue_postings (posting_id),
    CONSTRAINT ck_postings_type CHECK (document_type IN ('INV','CRN','ADJ')),
    CONSTRAINT ck_postings_sign CHECK ((document_type = 'INV' AND amount_doc > 0) OR (document_type = 'CRN' AND amount_doc < 0) OR (document_type = 'ADJ')),
    CONSTRAINT ck_postings_crn CHECK ((document_type = 'CRN' AND reverses_posting_id IS NOT NULL) OR (document_type <> 'CRN' AND reverses_posting_id IS NULL)),
    CONSTRAINT ck_postings_signs CHECK (SIGN(amount_doc) = SIGN(amount_company)),
    CONSTRAINT ck_postings_posted CHECK (posted_at >= posting_date::TIMESTAMP),
    CONSTRAINT ck_postings_period CHECK (fiscal_period = TO_CHAR(posting_date, 'YYYY-MM'))
);

CREATE INDEX IF NOT EXISTS ix_post_posted ON erp.revenue_postings (posted_at);
CREATE INDEX IF NOT EXISTS ix_post_date ON erp.revenue_postings (posting_date);
CREATE INDEX IF NOT EXISTS ix_post_order ON erp.revenue_postings (order_ref);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'erp_reader') THEN
        CREATE ROLE erp_reader LOGIN PASSWORD 'erp_reader';
    END IF;
END $$;
GRANT USAGE ON SCHEMA erp TO erp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA erp TO erp_reader;
