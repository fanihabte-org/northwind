-- Northwind Systems :: Operations source database
-- CRM and ERP references remain business keys because those systems are separate databases.
-- Deliberately non-destructive: use an explicit reset procedure for test data.

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS simulation;

CREATE TABLE IF NOT EXISTS simulation.applied_events (
    event_id VARCHAR(64) PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    business_date DATE NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    CONSTRAINT ck_applied_events_system CHECK (source_system IN ('crm', 'ops', 'erp'))
);

CREATE TABLE IF NOT EXISTS ops.customers (
    customer_id INTEGER NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    segment VARCHAR(40) NOT NULL,
    industry VARCHAR(60) NOT NULL,
    region VARCHAR(20) NOT NULL,
    company_code VARCHAR(10) NOT NULL,
    country VARCHAR(60) NOT NULL,
    employee_count INTEGER NOT NULL,
    credit_limit_usd NUMERIC(14,2) NOT NULL,
    payment_terms_days SMALLINT NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT uq_customers_acct UNIQUE (account_number),
    CONSTRAINT ck_customers_company CHECK (company_code IN ('US01','CA01','GB01','DE01','JP01')),
    CONSTRAINT ck_customers_seg CHECK (segment IN ('Enterprise','Mid-Market','SMB','Public Sector')),
    CONSTRAINT ck_customers_reg CHECK (region IN ('NA-WEST','NA-EAST','CANADA','EMEA','UKI','APAC')),
    CONSTRAINT ck_customers_terms CHECK (payment_terms_days IN (15,30,45,60,90)),
    CONSTRAINT ck_customers_emp CHECK (employee_count > 0),
    CONSTRAINT ck_customers_dates CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS ops.products (
    product_id INTEGER NOT NULL,
    sku VARCHAR(40) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(60) NOT NULL,
    product_family VARCHAR(40) NOT NULL,
    list_price_usd NUMERIC(12,2) NOT NULL,
    standard_cost_usd NUMERIC(12,2) NOT NULL,
    launch_date DATE NOT NULL,
    is_discontinued BOOLEAN NOT NULL,
    discontinued_on DATE,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT uq_products_sku UNIQUE (sku),
    CONSTRAINT ck_products_cat CHECK (category IN ('Hardware','Software License','Support Plan','Professional Services','Consumables')),
    CONSTRAINT ck_products_px CHECK (list_price_usd > 0 AND standard_cost_usd >= 0),
    CONSTRAINT ck_products_disc CHECK ((is_discontinued AND discontinued_on IS NOT NULL AND discontinued_on >= launch_date) OR (NOT is_discontinued AND discontinued_on IS NULL))
);

CREATE TABLE IF NOT EXISTS ops.warehouses (
    warehouse_code VARCHAR(10) NOT NULL,
    warehouse_name VARCHAR(120) NOT NULL,
    region VARCHAR(20) NOT NULL,
    latitude NUMERIC(9,5) NOT NULL,
    longitude NUMERIC(9,5) NOT NULL,
    CONSTRAINT pk_warehouses PRIMARY KEY (warehouse_code),
    CONSTRAINT ck_wh_lat CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_wh_lon CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS ops.carriers (
    carrier_code VARCHAR(15) NOT NULL,
    carrier_name VARCHAR(120) NOT NULL,
    mode VARCHAR(10) NOT NULL,
    cost_index NUMERIC(6,3) NOT NULL,
    published_otd_rate NUMERIC(5,3) NOT NULL,
    CONSTRAINT pk_carriers PRIMARY KEY (carrier_code),
    CONSTRAINT ck_car_mode CHECK (mode IN ('GROUND','AIR','OCEAN','RAIL')),
    CONSTRAINT ck_car_otd CHECK (published_otd_rate BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS ops.orders (
    order_id BIGINT NOT NULL,
    customer_id INTEGER NOT NULL,
    opportunity_ref VARCHAR(18),
    rep_id VARCHAR(12),
    order_date DATE NOT NULL,
    requested_delivery_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    sales_channel VARCHAR(20) NOT NULL,
    order_discount_pct NUMERIC(5,2) NOT NULL,
    po_number VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_cust FOREIGN KEY (customer_id) REFERENCES ops.customers (customer_id),
    CONSTRAINT ck_orders_status CHECK (status IN ('PENDING','SHIPPED','INVOICED','CANCELLED')),
    CONSTRAINT ck_orders_chan CHECK (sales_channel IN ('DIRECT','PARTNER','ECOMM')),
    CONSTRAINT ck_orders_ccy CHECK (currency_code IN ('USD','EUR','GBP','CAD','JPY')),
    CONSTRAINT ck_orders_disc CHECK (order_discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_orders_rdd CHECK (requested_delivery_date >= order_date),
    CONSTRAINT ck_orders_dates CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS ops.order_lines (
    order_line_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(16,2) NOT NULL,
    discount_pct NUMERIC(5,2) NOT NULL,
    line_amount NUMERIC(18,2) NOT NULL,
    unit_cost_usd NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_order_lines PRIMARY KEY (order_line_id),
    CONSTRAINT uq_order_lines_grain UNIQUE (order_id, product_id),
    CONSTRAINT fk_lines_order FOREIGN KEY (order_id) REFERENCES ops.orders (order_id),
    CONSTRAINT fk_lines_product FOREIGN KEY (product_id) REFERENCES ops.products (product_id),
    CONSTRAINT ck_lines_qty CHECK (quantity > 0),
    CONSTRAINT ck_lines_price CHECK (unit_price >= 0),
    CONSTRAINT ck_lines_disc CHECK (discount_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_lines_amount CHECK (line_amount >= 0)
);

CREATE TABLE IF NOT EXISTS ops.shipments (
    shipment_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    warehouse_code VARCHAR(10) NOT NULL,
    carrier_code VARCHAR(15) NOT NULL,
    service_level VARCHAR(15) NOT NULL,
    ship_date DATE NOT NULL,
    promised_delivery_date DATE NOT NULL,
    delivered_date DATE,
    package_count SMALLINT NOT NULL,
    gross_weight_kg NUMERIC(12,2) NOT NULL,
    distance_km NUMERIC(12,2) NOT NULL,
    freight_cost_usd NUMERIC(12,2) NOT NULL,
    tracking_number VARCHAR(30) NOT NULL,
    CONSTRAINT pk_shipments PRIMARY KEY (shipment_id),
    CONSTRAINT uq_shipments_ord UNIQUE (order_id),
    CONSTRAINT fk_ship_order FOREIGN KEY (order_id) REFERENCES ops.orders (order_id),
    CONSTRAINT fk_ship_wh FOREIGN KEY (warehouse_code) REFERENCES ops.warehouses (warehouse_code),
    CONSTRAINT fk_ship_carrier FOREIGN KEY (carrier_code) REFERENCES ops.carriers (carrier_code),
    CONSTRAINT ck_ship_service CHECK (service_level IN ('STANDARD','EXPEDITED','ECONOMY')),
    CONSTRAINT ck_ship_promise CHECK (promised_delivery_date >= ship_date),
    CONSTRAINT ck_ship_delivered CHECK (delivered_date IS NULL OR delivered_date >= ship_date),
    CONSTRAINT ck_ship_pkg CHECK (package_count > 0),
    CONSTRAINT ck_ship_metrics CHECK (gross_weight_kg > 0 AND distance_km > 0 AND freight_cost_usd > 0)
);

CREATE TABLE IF NOT EXISTS ops.invoices (
    invoice_id BIGINT NOT NULL,
    invoice_number VARCHAR(24) NOT NULL,
    order_id BIGINT NOT NULL,
    invoice_date DATE NOT NULL,
    currency_code CHAR(3) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_invoices PRIMARY KEY (invoice_id),
    CONSTRAINT uq_invoices_number UNIQUE (invoice_number),
    CONSTRAINT uq_invoices_order UNIQUE (order_id),
    CONSTRAINT fk_invoices_order FOREIGN KEY (order_id) REFERENCES ops.orders (order_id),
    CONSTRAINT ck_invoices_currency CHECK (currency_code IN ('USD','EUR','GBP','CAD','JPY')),
    CONSTRAINT ck_invoices_amount CHECK (amount > 0),
    CONSTRAINT ck_invoices_status CHECK (status IN ('ISSUED','VOID'))
);

CREATE TABLE IF NOT EXISTS ops.support_cases (
    case_id BIGINT NOT NULL,
    customer_id INTEGER NOT NULL,
    case_type VARCHAR(40) NOT NULL,
    priority CHAR(2) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    resolution_hours NUMERIC(10,2),
    status VARCHAR(20) NOT NULL,
    csat_score SMALLINT,
    assigned_region VARCHAR(20) NOT NULL,
    CONSTRAINT pk_cases PRIMARY KEY (case_id),
    CONSTRAINT fk_cases_cust FOREIGN KEY (customer_id) REFERENCES ops.customers (customer_id),
    CONSTRAINT ck_cases_pri CHECK (priority IN ('P1','P2','P3','P4')),
    CONSTRAINT ck_cases_stat CHECK (status IN ('Open','Resolved','Closed')),
    CONSTRAINT ck_cases_csat CHECK (csat_score IS NULL OR csat_score BETWEEN 1 AND 5),
    CONSTRAINT ck_cases_res CHECK (resolution_hours IS NULL OR resolution_hours >= 0),
    CONSTRAINT ck_cases_open CHECK (status <> 'Open' OR resolution_hours IS NULL)
);

CREATE INDEX IF NOT EXISTS ix_orders_date ON ops.orders (order_date);
CREATE INDEX IF NOT EXISTS ix_orders_updated ON ops.orders (updated_at);
CREATE INDEX IF NOT EXISTS ix_orders_cust ON ops.orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_opp ON ops.orders (opportunity_ref);
CREATE INDEX IF NOT EXISTS ix_lines_order ON ops.order_lines (order_id);
CREATE INDEX IF NOT EXISTS ix_lines_updated ON ops.order_lines (updated_at);
CREATE INDEX IF NOT EXISTS ix_lines_product ON ops.order_lines (product_id);
CREATE INDEX IF NOT EXISTS ix_ship_dates ON ops.shipments (ship_date, delivered_date);
CREATE INDEX IF NOT EXISTS ix_ship_carrier ON ops.shipments (carrier_code);
CREATE INDEX IF NOT EXISTS ix_invoices_date ON ops.invoices (invoice_date);
CREATE INDEX IF NOT EXISTS ix_cases_cust ON ops.support_cases (customer_id, opened_at);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ops_reader') THEN
        CREATE ROLE ops_reader LOGIN PASSWORD 'ops_reader';
    END IF;
END $$;
GRANT USAGE ON SCHEMA ops TO ops_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO ops_reader;
