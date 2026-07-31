-- =====================================================================
-- Northwind Systems :: SOURCE SYSTEMS
--
--   ops  -- order management and fulfilment   (SQL Server stand-in)
--   erp  -- finance and multi-entity ledger   (ERP stand-in)
--   crm  -- served over REST by FakeForce, and loadable here for convenience
--
-- Every constraint a competent DBA would place on these systems is present.
-- There are deliberately no foreign keys BETWEEN ops, erp and crm: they are
-- separate databases owned by separate teams, and no engine can enforce
-- integrity across that boundary.
-- =====================================================================

DROP SCHEMA IF EXISTS ops CASCADE;
DROP SCHEMA IF EXISTS erp CASCADE;
DROP SCHEMA IF EXISTS crm CASCADE;
CREATE SCHEMA ops;
CREATE SCHEMA erp;
CREATE SCHEMA crm;

-- =====================================================================
-- erp : entities, master data, rates
-- =====================================================================

CREATE TABLE erp.companies (
    company_code         VARCHAR(10)  NOT NULL,
    company_name         VARCHAR(120) NOT NULL,
    functional_currency  CHAR(3)      NOT NULL,
    country              VARCHAR(60)  NOT NULL,
    CONSTRAINT pk_companies PRIMARY KEY (company_code)
);

CREATE TABLE erp.cost_centers (
    cost_center_code  VARCHAR(20)  NOT NULL,
    cost_center_name  VARCHAR(120) NOT NULL,
    company_code      VARCHAR(10)  NOT NULL,
    region            VARCHAR(20)  NOT NULL,
    function          VARCHAR(40)  NOT NULL,
    owner_email       VARCHAR(160) NOT NULL,
    valid_from        DATE         NOT NULL,
    valid_to          DATE,
    is_active         BOOLEAN      NOT NULL,
    CONSTRAINT pk_cost_centers PRIMARY KEY (cost_center_code),
    CONSTRAINT fk_cc_company   FOREIGN KEY (company_code) REFERENCES erp.companies (company_code),
    CONSTRAINT ck_cc_validity  CHECK (valid_to IS NULL OR valid_to > valid_from),
    CONSTRAINT ck_cc_active    CHECK ((is_active AND valid_to IS NULL) OR NOT is_active)
);

CREATE TABLE erp.gl_accounts (
    gl_account    VARCHAR(10)  NOT NULL,
    gl_name       VARCHAR(120) NOT NULL,
    account_type  VARCHAR(20)  NOT NULL,
    is_postable   BOOLEAN      NOT NULL,
    CONSTRAINT pk_gl_accounts PRIMARY KEY (gl_account),
    CONSTRAINT ck_gl_type CHECK (account_type IN
        ('REVENUE','ASSET','LIABILITY','EXPENSE','CLEARING'))
);

CREATE TABLE erp.fx_rates (
    from_currency  CHAR(3)       NOT NULL,
    to_currency    CHAR(3)       NOT NULL,
    rate_date      DATE          NOT NULL,
    rate_type      VARCHAR(10)   NOT NULL,
    rate           NUMERIC(18,8) NOT NULL,
    source_system  VARCHAR(30)   NOT NULL,
    loaded_at      TIMESTAMP     NOT NULL,
    CONSTRAINT pk_fx_rates PRIMARY KEY (from_currency, to_currency, rate_date, rate_type),
    CONSTRAINT ck_fx_rate  CHECK (rate > 0),
    CONSTRAINT ck_fx_diff  CHECK (from_currency <> to_currency),
    CONSTRAINT ck_fx_type  CHECK (rate_type IN ('SPOT','AVG','CLOSE'))
);

-- =====================================================================
-- ops : customers, catalogue, orders, fulfilment, service
-- =====================================================================

CREATE TABLE ops.customers (
    customer_id         INTEGER       NOT NULL,
    account_number      VARCHAR(20)   NOT NULL,
    customer_name       VARCHAR(200)  NOT NULL,
    segment             VARCHAR(40)   NOT NULL,
    industry            VARCHAR(60)   NOT NULL,
    region              VARCHAR(20)   NOT NULL,
    company_code        VARCHAR(10)   NOT NULL,
    country             VARCHAR(60)   NOT NULL,
    employee_count      INTEGER       NOT NULL,
    credit_limit_usd    NUMERIC(14,2) NOT NULL,
    payment_terms_days  SMALLINT      NOT NULL,
    is_active           BOOLEAN       NOT NULL,
    created_at          TIMESTAMP     NOT NULL,
    updated_at          TIMESTAMP     NOT NULL,
    CONSTRAINT pk_customers       PRIMARY KEY (customer_id),
    CONSTRAINT uq_customers_acct  UNIQUE (account_number),
    CONSTRAINT fk_customers_co    FOREIGN KEY (company_code) REFERENCES erp.companies (company_code),
    CONSTRAINT ck_customers_seg   CHECK (segment IN ('Enterprise','Mid-Market','SMB','Public Sector')),
    CONSTRAINT ck_customers_reg   CHECK (region IN ('NA-WEST','NA-EAST','CANADA','EMEA','UKI','APAC')),
    CONSTRAINT ck_customers_terms CHECK (payment_terms_days IN (15,30,45,60,90)),
    CONSTRAINT ck_customers_emp   CHECK (employee_count > 0),
    CONSTRAINT ck_customers_dates CHECK (updated_at >= created_at)
);

CREATE TABLE ops.products (
    product_id         INTEGER       NOT NULL,
    sku                VARCHAR(40)   NOT NULL,
    product_name       VARCHAR(200)  NOT NULL,
    category           VARCHAR(60)   NOT NULL,
    product_family     VARCHAR(40)   NOT NULL,
    list_price_usd     NUMERIC(12,2) NOT NULL,
    standard_cost_usd  NUMERIC(12,2) NOT NULL,
    launch_date        DATE          NOT NULL,
    is_discontinued    BOOLEAN       NOT NULL,
    discontinued_on    DATE,
    updated_at         TIMESTAMP     NOT NULL,
    CONSTRAINT pk_products      PRIMARY KEY (product_id),
    CONSTRAINT uq_products_sku  UNIQUE (sku),
    CONSTRAINT ck_products_cat  CHECK (category IN
        ('Hardware','Software License','Support Plan','Professional Services','Consumables')),
    CONSTRAINT ck_products_px   CHECK (list_price_usd > 0 AND standard_cost_usd >= 0),
    CONSTRAINT ck_products_disc CHECK (
        (is_discontinued AND discontinued_on IS NOT NULL AND discontinued_on >= launch_date)
        OR (NOT is_discontinued AND discontinued_on IS NULL))
);

CREATE TABLE ops.warehouses (
    warehouse_code  VARCHAR(10)   NOT NULL,
    warehouse_name  VARCHAR(120)  NOT NULL,
    region          VARCHAR(20)   NOT NULL,
    latitude        NUMERIC(9,5)  NOT NULL,
    longitude       NUMERIC(9,5)  NOT NULL,
    CONSTRAINT pk_warehouses PRIMARY KEY (warehouse_code),
    CONSTRAINT ck_wh_lat CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_wh_lon CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE ops.carriers (
    carrier_code        VARCHAR(15)  NOT NULL,
    carrier_name        VARCHAR(120) NOT NULL,
    mode                VARCHAR(10)  NOT NULL,
    cost_index          NUMERIC(6,3) NOT NULL,
    published_otd_rate  NUMERIC(5,3) NOT NULL,
    CONSTRAINT pk_carriers  PRIMARY KEY (carrier_code),
    CONSTRAINT ck_car_mode  CHECK (mode IN ('GROUND','AIR','OCEAN','RAIL')),
    CONSTRAINT ck_car_otd   CHECK (published_otd_rate BETWEEN 0 AND 1)
);

CREATE TABLE ops.orders (
    order_id                 BIGINT        NOT NULL,
    customer_id              INTEGER       NOT NULL,
    opportunity_ref          VARCHAR(18),
    rep_id                   VARCHAR(12),
    order_date               DATE          NOT NULL,
    requested_delivery_date  DATE          NOT NULL,
    currency_code            CHAR(3)       NOT NULL,
    status                   VARCHAR(20)   NOT NULL,
    sales_channel            VARCHAR(20)   NOT NULL,
    order_discount_pct       NUMERIC(5,2)  NOT NULL,
    po_number                VARCHAR(20),
    created_at               TIMESTAMP     NOT NULL,
    updated_at               TIMESTAMP     NOT NULL,
    CONSTRAINT pk_orders        PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_cust   FOREIGN KEY (customer_id) REFERENCES ops.customers (customer_id),
    CONSTRAINT ck_orders_status CHECK (status IN ('PENDING','SHIPPED','INVOICED','CANCELLED')),
    CONSTRAINT ck_orders_chan   CHECK (sales_channel IN ('DIRECT','PARTNER','ECOMM')),
    CONSTRAINT ck_orders_ccy    CHECK (currency_code IN ('USD','EUR','GBP','CAD','JPY')),
    CONSTRAINT ck_orders_disc   CHECK (order_discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_orders_rdd    CHECK (requested_delivery_date >= order_date),
    CONSTRAINT ck_orders_dates  CHECK (updated_at >= created_at)
);

CREATE TABLE ops.order_lines (
    order_line_id  BIGINT        NOT NULL,
    order_id       BIGINT        NOT NULL,
    product_id     INTEGER       NOT NULL,
    quantity       INTEGER       NOT NULL,
    unit_price     NUMERIC(16,2) NOT NULL,
    discount_pct   NUMERIC(5,2)  NOT NULL,
    line_amount    NUMERIC(18,2) NOT NULL,
    unit_cost_usd  NUMERIC(12,2) NOT NULL,
    updated_at     TIMESTAMP     NOT NULL,
    CONSTRAINT pk_order_lines       PRIMARY KEY (order_line_id),
    CONSTRAINT uq_order_lines_grain UNIQUE (order_id, product_id),
    CONSTRAINT fk_lines_order       FOREIGN KEY (order_id)   REFERENCES ops.orders (order_id),
    CONSTRAINT fk_lines_product     FOREIGN KEY (product_id) REFERENCES ops.products (product_id),
    CONSTRAINT ck_lines_qty         CHECK (quantity > 0),
    CONSTRAINT ck_lines_price       CHECK (unit_price >= 0),
    CONSTRAINT ck_lines_disc        CHECK (discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_lines_amount      CHECK (line_amount >= 0)
);

CREATE TABLE ops.shipments (
    shipment_id             BIGINT        NOT NULL,
    order_id                BIGINT        NOT NULL,
    warehouse_code          VARCHAR(10)   NOT NULL,
    carrier_code            VARCHAR(15)   NOT NULL,
    service_level           VARCHAR(15)   NOT NULL,
    ship_date               DATE          NOT NULL,
    promised_delivery_date  DATE          NOT NULL,
    delivered_date          DATE,
    package_count           SMALLINT      NOT NULL,
    gross_weight_kg         NUMERIC(12,2) NOT NULL,
    distance_km             NUMERIC(12,2) NOT NULL,
    freight_cost_usd        NUMERIC(12,2) NOT NULL,
    tracking_number         VARCHAR(30)   NOT NULL,
    CONSTRAINT pk_shipments      PRIMARY KEY (shipment_id),
    CONSTRAINT uq_shipments_ord  UNIQUE (order_id),
    CONSTRAINT fk_ship_order     FOREIGN KEY (order_id)       REFERENCES ops.orders (order_id),
    CONSTRAINT fk_ship_wh        FOREIGN KEY (warehouse_code) REFERENCES ops.warehouses (warehouse_code),
    CONSTRAINT fk_ship_carrier   FOREIGN KEY (carrier_code)   REFERENCES ops.carriers (carrier_code),
    CONSTRAINT ck_ship_service   CHECK (service_level IN ('STANDARD','EXPEDITED','ECONOMY')),
    CONSTRAINT ck_ship_promise   CHECK (promised_delivery_date >= ship_date),
    CONSTRAINT ck_ship_delivered CHECK (delivered_date IS NULL OR delivered_date >= ship_date),
    CONSTRAINT ck_ship_pkg       CHECK (package_count > 0),
    CONSTRAINT ck_ship_metrics   CHECK (gross_weight_kg > 0 AND distance_km > 0 AND freight_cost_usd > 0)
);

CREATE TABLE ops.support_cases (
    case_id           BIGINT        NOT NULL,
    customer_id       INTEGER       NOT NULL,
    case_type         VARCHAR(40)   NOT NULL,
    priority          CHAR(2)       NOT NULL,
    channel           VARCHAR(20)   NOT NULL,
    opened_at         TIMESTAMP     NOT NULL,
    resolution_hours  NUMERIC(10,2),
    status            VARCHAR(20)   NOT NULL,
    csat_score        SMALLINT,
    assigned_region   VARCHAR(20)   NOT NULL,
    CONSTRAINT pk_cases       PRIMARY KEY (case_id),
    CONSTRAINT fk_cases_cust  FOREIGN KEY (customer_id) REFERENCES ops.customers (customer_id),
    CONSTRAINT ck_cases_pri   CHECK (priority IN ('P1','P2','P3','P4')),
    CONSTRAINT ck_cases_stat  CHECK (status IN ('Open','Resolved','Closed')),
    CONSTRAINT ck_cases_csat  CHECK (csat_score IS NULL OR csat_score BETWEEN 1 AND 5),
    CONSTRAINT ck_cases_res   CHECK (resolution_hours IS NULL OR resolution_hours >= 0),
    CONSTRAINT ck_cases_open  CHECK (status <> 'Open' OR resolution_hours IS NULL)
);

-- =====================================================================
-- erp : the ledger
-- =====================================================================

CREATE TABLE erp.revenue_postings (
    posting_id           BIGINT        NOT NULL,
    document_number      VARCHAR(24)   NOT NULL,
    document_type        VARCHAR(10)   NOT NULL,
    company_code         VARCHAR(10)   NOT NULL,
    order_ref            BIGINT,
    gl_account           VARCHAR(10)   NOT NULL,
    cost_center_code     VARCHAR(20),
    posting_date         DATE          NOT NULL,
    fiscal_period        VARCHAR(7)    NOT NULL,
    document_currency    CHAR(3)       NOT NULL,
    amount_doc           NUMERIC(18,2) NOT NULL,
    company_currency     CHAR(3)       NOT NULL,
    amount_company       NUMERIC(18,2) NOT NULL,
    reverses_posting_id  BIGINT,
    posted_at            TIMESTAMP     NOT NULL,
    CONSTRAINT pk_postings     PRIMARY KEY (posting_id),
    CONSTRAINT uq_postings_doc UNIQUE (company_code, document_number),
    CONSTRAINT fk_postings_co  FOREIGN KEY (company_code)        REFERENCES erp.companies (company_code),
    CONSTRAINT fk_postings_gl  FOREIGN KEY (gl_account)          REFERENCES erp.gl_accounts (gl_account),
    CONSTRAINT fk_postings_cc  FOREIGN KEY (cost_center_code)    REFERENCES erp.cost_centers (cost_center_code),
    CONSTRAINT fk_postings_rev FOREIGN KEY (reverses_posting_id) REFERENCES erp.revenue_postings (posting_id),
    CONSTRAINT ck_postings_type CHECK (document_type IN ('INV','CRN','ADJ')),
    CONSTRAINT ck_postings_sign CHECK (
        (document_type = 'INV' AND amount_doc > 0) OR
        (document_type = 'CRN' AND amount_doc < 0) OR
        (document_type = 'ADJ')),
    CONSTRAINT ck_postings_crn CHECK (
        (document_type =  'CRN' AND reverses_posting_id IS NOT NULL) OR
        (document_type <> 'CRN' AND reverses_posting_id IS NULL)),
    CONSTRAINT ck_postings_signs   CHECK (SIGN(amount_doc) = SIGN(amount_company)),
    CONSTRAINT ck_postings_posted  CHECK (posted_at >= posting_date::TIMESTAMP),
    CONSTRAINT ck_postings_period  CHECK (fiscal_period = TO_CHAR(posting_date, 'YYYY-MM'))
);

-- =====================================================================
-- crm : the same objects FakeForce serves over REST, for people who would
--       rather join in SQL than write an API client. Loading this is optional.
-- =====================================================================

CREATE TABLE crm.sales_reps (
    rep_id            VARCHAR(12)   NOT NULL,
    rep_name          VARCHAR(120)  NOT NULL,
    region            VARCHAR(20)   NOT NULL,
    hire_date         DATE          NOT NULL,
    departure_date    DATE,
    quota_usd_annual  NUMERIC(14,2) NOT NULL,
    manager_id        VARCHAR(12)   NOT NULL,
    CONSTRAINT pk_reps      PRIMARY KEY (rep_id),
    CONSTRAINT ck_reps_dep  CHECK (departure_date IS NULL OR departure_date > hire_date)
);

CREATE TABLE crm.campaigns (
    campaign_id      VARCHAR(12)   NOT NULL,
    campaign_name    VARCHAR(160)  NOT NULL,
    campaign_type    VARCHAR(40)   NOT NULL,
    target_region    VARCHAR(20)   NOT NULL,
    target_segment   VARCHAR(40)   NOT NULL,
    start_date       DATE          NOT NULL,
    end_date         DATE          NOT NULL,
    budget_usd       NUMERIC(14,2) NOT NULL,
    channel_owner    VARCHAR(120)  NOT NULL,
    CONSTRAINT pk_campaigns    PRIMARY KEY (campaign_id),
    CONSTRAINT ck_camp_window  CHECK (end_date >= start_date),
    CONSTRAINT ck_camp_budget  CHECK (budget_usd > 0)
);

CREATE TABLE crm.accounts (
    "Id"                VARCHAR(18)   NOT NULL,
    "Name"              VARCHAR(200)  NOT NULL,
    "AccountNumber"     VARCHAR(20)   NOT NULL,
    "Industry"          VARCHAR(60),
    "Type"              VARCHAR(40),
    "BillingCountry"    VARCHAR(60),
    "AnnualRevenue"     NUMERIC(18,2),
    "NumberOfEmployees" INTEGER,
    "OwnerId"           VARCHAR(12),
    "CreatedDate"       VARCHAR(30)   NOT NULL,
    "LastModifiedDate"  VARCHAR(30)   NOT NULL,
    "IsDeleted"         BOOLEAN       NOT NULL,
    CONSTRAINT pk_crm_accounts PRIMARY KEY ("Id"),
    CONSTRAINT uq_crm_acctnum  UNIQUE ("AccountNumber"),
    CONSTRAINT fk_crm_owner    FOREIGN KEY ("OwnerId") REFERENCES crm.sales_reps (rep_id)
);

CREATE TABLE crm.opportunities (
    "Id"                VARCHAR(18)   NOT NULL,
    "AccountId"         VARCHAR(18)   NOT NULL,
    "Name"              VARCHAR(260)  NOT NULL,
    "StageName"         VARCHAR(40)   NOT NULL,
    "Amount"            NUMERIC(18,2),
    "CurrencyIsoCode"   CHAR(3)       NOT NULL,
    "CloseDate"         DATE          NOT NULL,
    "Probability"       SMALLINT      NOT NULL,
    "LeadSource"        VARCHAR(40),
    "CampaignId"        VARCHAR(12),
    "DiscountPercent"   NUMERIC(5,2),
    "LossReason"        VARCHAR(40),
    "SalesCycleDays"    INTEGER,
    "OwnerId"           VARCHAR(12),
    "CreatedDate"       VARCHAR(30)   NOT NULL,
    "LastModifiedDate"  VARCHAR(30)   NOT NULL,
    "IsDeleted"         BOOLEAN       NOT NULL,
    CONSTRAINT pk_crm_opps    PRIMARY KEY ("Id"),
    CONSTRAINT fk_crm_opp_acc FOREIGN KEY ("AccountId")  REFERENCES crm.accounts ("Id"),
    CONSTRAINT fk_crm_opp_cmp FOREIGN KEY ("CampaignId") REFERENCES crm.campaigns (campaign_id),
    CONSTRAINT fk_crm_opp_own FOREIGN KEY ("OwnerId")    REFERENCES crm.sales_reps (rep_id),
    CONSTRAINT ck_crm_stage   CHECK ("StageName" IN
        ('Prospecting','Qualification','Proposal','Negotiation','Closed Won','Closed Lost')),
    CONSTRAINT ck_crm_prob    CHECK ("Probability" BETWEEN 0 AND 100),
    CONSTRAINT ck_crm_loss    CHECK (("StageName" = 'Closed Lost') = ("LossReason" IS NOT NULL))
);

-- =====================================================================
-- Indexes on the columns you will actually filter and join on
-- =====================================================================
CREATE INDEX ix_orders_date     ON ops.orders (order_date);
CREATE INDEX ix_orders_updated  ON ops.orders (updated_at);
CREATE INDEX ix_orders_cust     ON ops.orders (customer_id);
CREATE INDEX ix_orders_opp      ON ops.orders (opportunity_ref);
CREATE INDEX ix_lines_order     ON ops.order_lines (order_id);
CREATE INDEX ix_lines_updated   ON ops.order_lines (updated_at);
CREATE INDEX ix_lines_product   ON ops.order_lines (product_id);
CREATE INDEX ix_ship_dates      ON ops.shipments (ship_date, delivered_date);
CREATE INDEX ix_ship_carrier    ON ops.shipments (carrier_code);
CREATE INDEX ix_cases_cust      ON ops.support_cases (customer_id, opened_at);
CREATE INDEX ix_post_posted     ON erp.revenue_postings (posted_at);
CREATE INDEX ix_post_date       ON erp.revenue_postings (posting_date);
CREATE INDEX ix_post_order      ON erp.revenue_postings (order_ref);
CREATE INDEX ix_crm_opp_close   ON crm.opportunities ("CloseDate");
CREATE INDEX ix_crm_opp_stage   ON crm.opportunities ("StageName");
CREATE INDEX ix_crm_opp_owner   ON crm.opportunities ("OwnerId");

-- =====================================================================
-- Read-only extraction role
-- =====================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'northwind_reader') THEN
        CREATE ROLE northwind_reader LOGIN PASSWORD 'northwind_reader';
    END IF;
END $$;

GRANT USAGE ON SCHEMA ops, erp, crm TO northwind_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops, erp, crm TO northwind_reader;
