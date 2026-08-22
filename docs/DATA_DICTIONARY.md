# Northwind Systems — Data Dictionary

Documentation for the three operational systems that make up the Northwind Systems
dataset. This describes **what the data is**, not what to do with it.

---

## 1. The business

Northwind Systems sells IT infrastructure to other businesses: server and network
hardware, software licences, support plans, professional services, and consumables.
It operates through **five legal entities** and sells into **six sales regions**.

The company runs three systems, owned by three different teams:

| System | Owner | Contents | How you read it |
|---|---|---|---|
| **CRM** | Sales Operations | Accounts, opportunities, reps, campaigns | REST API (`FakeForce`, Salesforce-shaped) |
| **`ops`** | IT Operations | Customers, catalogue, orders, fulfilment, support | Dedicated Postgres database on `localhost:5433` |
| **`erp`** | Finance | Legal entities, cost centres, GL, FX, the revenue ledger | Dedicated Postgres database on `localhost:5434` |

The dataset covers **2023-01-01 through 2026-07-24** — three and a half years, which
is enough for year-over-year comparison, cohort maturation, and seasonal models that
need more than one cycle.

**The business has a history.** It grew, launched products, retired others, changed
pricing, renegotiated a carrier contract, reorganised a support team, and met a new
competitor in one of its regions. None of those events are labelled anywhere in the
data. They are visible only as changes in the numbers, which is the point.

### Legal entities

| Code | Entity | Functional currency | Country |
|---|---|---|---|
| `US01` | Northwind Systems Inc. | USD | United States |
| `CA01` | Northwind Systems Canada ULC | CAD | Canada |
| `GB01` | Northwind Systems UK Ltd. | GBP | United Kingdom |
| `DE01` | Northwind Systems GmbH | EUR | Germany |
| `JP01` | Northwind Systems KK | JPY | Japan |

### Sales regions and their booking entity

`NA-WEST` → US01 · `NA-EAST` → US01 · `CANADA` → CA01 · `EMEA` → DE01 ·
`UKI` → GB01 · `APAC` → JP01

### Database ownership and constraints

Ops and ERP are separate PostgreSQL databases in the same Compose stack. Their DDL
lives in `sql/migrations/ops/` and `sql/migrations/erp/`, respectively. Each database enforces
its own primary keys, unique constraints, check constraints, foreign keys, and
indexes. A constraint never crosses a source-system boundary: CRM, Ops, and ERP
references across systems are deliberately business keys for the data warehouse to
reconcile.

Ops and ERP each own a `simulation.applied_events` retry ledger. Its primary key
is the deterministic simulation event ID, so a restarted daily run can safely
retry an already-applied source event without creating a duplicate order or
Finance posting. The ledgers are intentionally local to their respective source
databases and never create a cross-system constraint.

Before a simulated business date is marked complete, a bounded reconciliation
checks that each newly planned Ops and ERP event has both its local retry marker
and its expected order, shipment, invoice, or revenue-posting record. A mismatch
leaves the date incomplete so the normal replay path can recover it.

---

## 2. Entity map

```
                    ┌─────────────────────────────────────────────┐
   CRM              │  crm.sales_reps          crm.campaigns      │
   (FakeForce)      │      rep_id                  campaign_id     │
                    │        │                         │           │
                    │        ├──────────┬──────────────┘           │
                    │        ▼          ▼                          │
                    │  crm.accounts ─< crm.opportunities           │
                    │      Id              Id                       │
                    └───────┬──────────────┬──────────────────────┘
                            │              │
            AccountNumber   │              │  opportunity_ref
         (business key,     │              │  (no FK possible)
          formats vary)     │              │
                            ▼              ▼
   ops              ┌─────────────────────────────────────────────┐
                    │  ops.customers ──< ops.orders ──< ops.order_lines
                    │   customer_id        order_id  │      product_id
                    │        │                       │          │
                    │        │                       │          ▼
                    │        │                       │    ops.products
                    │        ▼                       ▼
                    │  ops.support_cases       ops.shipments
                    │                            │        │
                    │             ops.warehouses ┘        └ ops.carriers
                    └───────────────────────┬─────────────────────┘
                                            │  order_ref
                                            │  (no FK possible)
                                            ▼
   erp              ┌─────────────────────────────────────────────┐
                    │  erp.revenue_postings                        │
                    │     ├─ company_code     → erp.companies      │
                    │     ├─ gl_account       → erp.gl_accounts    │
                    │     ├─ cost_center_code → erp.cost_centers   │
                    │     └─ reverses_posting_id → itself          │
                    │                                              │
                    │  erp.fx_rates    (from_currency, rate_date)  │
                    └─────────────────────────────────────────────┘
```

### Cross-system joins

There are **no foreign keys between CRM, `ops` and `erp`**. They are separate
databases owned by separate teams; no engine can enforce integrity across that
boundary. Within each system, every relationship *is* enforced.

| From | To | Key | Cardinality | Notes |
|---|---|---|---|---|
| `ops.customers.account_number` | `crm.accounts."AccountNumber"` | business key | 1:1 | The canonical form is `ACC-` plus six digits. Accounts opened before the mid-2024 CRM migration were keyed manually and the `ops` side reflects however the operator typed it. |
| `ops.orders.opportunity_ref` | `crm.opportunities."Id"` | Salesforce id | N:1 | `NULL` for `ECOMM` orders, which have no opportunity. |
| `ops.orders.rep_id` | `crm.sales_reps.rep_id` | rep id | N:1 | `NULL` for `ECOMM`. |
| `erp.revenue_postings.order_ref` | `ops.orders.order_id` | order id | N:1 | `NULL` for `ADJ` postings. |
| `ops.customers.company_code` | `erp.companies.company_code` | entity code | N:1 | External ERP business key; Ops validates the known legal-entity code set locally but cannot hold an ERP foreign key. Determines the currency the order is booked in. |

### Daily lifecycle, source lag, and controlled exceptions

After the baseline period, the simulator advances the business one calendar date at
a time at midnight in `America/Los_Angeles`. It always resumes from the most recent
successfully completed simulation date. If the host was down for two days, it
generates and applies both missing dates in order; it never jumps directly to the
current date or fills the gap with a single oversized dump.

The normal event flow is deliberately causal:

| Upstream event | Downstream event | Normal SLA |
|---|---|---|
| CRM opportunity becomes `Closed Won` | Ops order is created | 1–3 calendar days |
| Ops order is created | Shipment is created | 1–5 calendar days |
| Shipment is eligible for billing | Order becomes invoiced | 0–3 calendar days |
| Ops invoice event | ERP revenue posting | 1–2 calendar days |

Daily additions are proportional to the current population, using an 8% annualised
growth rate divided across the year. The exact count varies deterministically from
the simulation seed while preserving keys, cardinalities, and all local source
constraints.

The dataset also includes a small, auditable anomaly budget. These exceptions are
intentional and must be distinguishable from normal business flow:

| Exception | Default rate | Effect |
|---|---:|---|
| Long-open opportunity | 0.6% | Opportunity stays in an open sales stage longer than its normal cycle. |
| Delayed CRM-to-Ops conversion | 0.4% | A won opportunity waits beyond the normal conversion window. |
| Stalled shipment | 0.8% | An order remains pending or unshipped for multiple additional days. |
| Late ERP posting | 0.4% | An eligible invoice reaches Finance later than its normal posting window. |

Once per ISO week, one deterministic random business date also receives a one-day
source-delivery SLA breach. The delayed source data is published on the following
run and the simulation ledger records the cause. This creates a realistic,
recoverable data-quality incident without making delayed data the norm.

### Lifecycle-history contract

Operational tables retain only the latest usable state of a record. Lifecycle
history is recorded separately as immutable, append-only event rows; it does not
replace the operational row or prevent its `updated_at` value from changing.

History is owned by the source system that owns the lifecycle. Every history row
uses the following common contract:

| Field | Meaning |
|---|---|
| `occurred_at` | Local business timestamp at which the lifecycle transition took effect. |
| `recorded_at` | Local source-system timestamp at which the transition was persisted. |
| `source_event_id` | Deterministic simulator event identifier; unique within its history table. |
| `previous_state` | State before the transition; `NULL` only for an initial creation event. |
| `new_state` | State after the transition. |
| `sla_due_at` | Latest permitted business timestamp calculated from the causal upstream event. |
| `sla_status` | `ON_TIME` or `BREACHED`, determined from `occurred_at` and `sla_due_at`. |
| `anomaly_type` | Controlled exception that explains a non-normal outcome, when applicable. |

`occurred_at` is derived from the planned causal event and its business date, never
from the wall-clock time at which a container happens to run. `recorded_at` is
written in the same source-database transaction as the current-state update.

Existing seed records are a baseline current snapshot. An explicit, opt-in
inferred-baseline backfill may derive the complete valid predecessor chain from
the current operational rows and their business dates. It writes only to the
append-only history tables, never changes a current-state row, uses deterministic
event identifiers for idempotency, and marks every inserted row
`anomaly_type = 'inferred_baseline'`. Rows with simulator-recorded history are
excluded. Inferred timestamps are estimates rather than source-recorded facts.

Dimension/master tables remain current-state only; no SCD or dimensional-history
tables are created for this dataset.

---

## 3. Table reference

Row counts below are for `--scale 1`. All timestamps are naive local time with
second granularity. All dates are `YYYY-MM-DD`.

---

### `ops.customers`
**Grain:** one row per customer account. ≈ 75,000 rows.

The operational view of a customer. Corresponds 1:1 to a CRM Account, matched on
account number.

| Column | Type | Null | Description |
|---|---|---|---|
| `customer_id` | INTEGER | no | Primary key. Surrogate, assigned by the OMS. |
| `account_number` | VARCHAR(20) | no | Business key shared with the CRM. Unique. Canonical form `ACC-000123`. Populated by hand before the 2024 CRM migration and by integration after it, so the stored format varies with `created_at`. |
| `customer_name` | VARCHAR(200) | no | Registered company name. |
| `segment` | VARCHAR(40) | no | `Enterprise` · `Mid-Market` · `SMB` · `Public Sector`. Drives order frequency, basket size, sales-cycle length and retention. |
| `industry` | VARCHAR(60) | no | Nine values. Descriptive only; does not drive behaviour. |
| `region` | VARCHAR(20) | no | Sales region. Determines booking entity and default currency. |
| `company_code` | VARCHAR(10) | no | Booking legal entity. Locally constrained to a valid Northwind entity code; externally reconciled to `erp.companies.company_code`, not a foreign key because ERP is a separate database. |
| `country` | VARCHAR(60) | no | Billing country, implied by region. |
| `employee_count` | INTEGER | no | Company size. Correlates with segment. |
| `credit_limit_usd` | NUMERIC(14,2) | no | Approved credit ceiling. Log-normally distributed. |
| `payment_terms_days` | SMALLINT | no | `15` · `30` · `45` · `60` · `90`. |
| `is_active` | BOOLEAN | no | Whether the account is still trading **as of the end of the window**. This is a current-state flag, not a history. |
| `created_at` | TIMESTAMP | no | Account opened. This is the acquisition date for cohort work. |
| `updated_at` | TIMESTAMP | no | Last record change. |

---

### `ops.products`
**Grain:** one row per sellable item. ≈ 1,200 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `product_id` | INTEGER | no | Primary key. |
| `sku` | VARCHAR(40) | no | Unique stock keeping unit, `NW-<CAT>-<n>`. |
| `product_name` | VARCHAR(200) | no | Marketing name. |
| `category` | VARCHAR(60) | no | `Hardware` · `Software License` · `Support Plan` · `Professional Services` · `Consumables`. Gross margin differs sharply by category. |
| `product_family` | VARCHAR(40) | no | `Core` · `Extended` · `Aurora`. Families group products for mix analysis. |
| `list_price_usd` | NUMERIC(12,2) | no | Catalogue price in USD. Order lines are priced from this, converted at the order-date rate. |
| `standard_cost_usd` | NUMERIC(12,2) | no | Standard unit cost. `list_price_usd - standard_cost_usd` is the list gross margin. |
| `launch_date` | DATE | no | First sellable date. Roughly half the catalogue predates the window; the rest launches during it. |
| `is_discontinued` | BOOLEAN | no | Current status flag. |
| `discontinued_on` | DATE | yes | Set when and only when `is_discontinued`. Support renewals and open orders continue past this date. |
| `created_at` | TIMESTAMP | no | Catalogue-record creation time. For generated products this is the initial catalogue publication timestamp on `launch_date`. |
| `updated_at` | TIMESTAMP | no | Last catalogue change. |

---

### `ops.orders`
**Grain:** one row per customer order (the header). ≈ 1.5 M rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `order_id` | BIGINT | no | Primary key. |
| `customer_id` | INTEGER | no | FK to `ops.customers`. |
| `opportunity_ref` | VARCHAR(18) | yes | CRM Opportunity id. `NULL` where the order did not come from an opportunity — always for `ECOMM`, and for a minority of `DIRECT` and `PARTNER` orders that were booked without one. No foreign key: the CRM is a different system, and the referenced record may have changed there since. |
| `rep_id` | VARCHAR(12) | yes | Owning sales rep. `NULL` wherever `opportunity_ref` is. |
| `order_date` | DATE | no | Business date of the order. Use this for revenue timing. |
| `requested_delivery_date` | DATE | no | Date the customer asked for. Always ≥ `order_date`. |
| `currency_code` | CHAR(3) | no | **Transaction (document) currency.** Usually the region default; a minority of orders are transacted in USD instead. |
| `status` | VARCHAR(20) | no | `PENDING` · `SHIPPED` · `INVOICED` · `CANCELLED`. Only `SHIPPED` and `INVOICED` reach the ledger and produce a shipment. Cancelled orders keep their lines. |
| `sales_channel` | VARCHAR(20) | no | `DIRECT` · `PARTNER` · `ECOMM`. |
| `order_discount_pct` | NUMERIC(5,2) | no | Header-level discount negotiated on the opportunity. `0` for `ECOMM`. |
| `po_number` | VARCHAR(20) | yes | Customer purchase order reference where one was supplied. |
| `created_at` | TIMESTAMP | no | When the row was **written**. Normally the same day as `order_date`; diverges when orders are entered retrospectively. |
| `updated_at` | TIMESTAMP | no | Last change. Second granularity. Bulk maintenance jobs stamp many rows with an identical value. |

---

### `ops.order_lines`
**Grain:** one row per product per order. ≈ 4.6 M rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `order_line_id` | BIGINT | no | Primary key. |
| `order_id` | BIGINT | no | FK to `ops.orders`. |
| `product_id` | INTEGER | no | FK to `ops.products`. Unique together with `order_id` — a product appears at most once per order. |
| `quantity` | INTEGER | no | Units. Always positive. Consumables order in much larger quantities. |
| `unit_price` | NUMERIC(16,2) | no | Price per unit **in the order's document currency**, converted from `list_price_usd` at the order-date rate. Rounded to the currency's ISO 4217 minor unit, so JPY values carry no decimals. |
| `discount_pct` | NUMERIC(5,2) | no | Line discount. Derived from the header discount with line-level variation. |
| `line_amount` | NUMERIC(18,2) | no | `quantity × unit_price × (1 - discount_pct/100)`, in document currency. |
| `unit_cost_usd` | NUMERIC(12,2) | no | Standard cost snapshot in USD. Margin needs both sides on the same currency basis. |
| `created_at` | TIMESTAMP | no | When the OMS created the line. Usually inherited from the order creation time. |
| `updated_at` | TIMESTAMP | no | Last change. Second granularity. |

---

### `ops.warehouses`
**Grain:** one row per distribution centre. 8 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `warehouse_code` | VARCHAR(10) | no | Primary key, e.g. `WH-SEA`. |
| `warehouse_name` | VARCHAR(120) | no | Facility name. |
| `region` | VARCHAR(20) | no | Region served. Orders normally ship from a DC in their own region. |
| `latitude` / `longitude` | NUMERIC(9,5) | no | Facility coordinates, for lane and distance work. |
| `created_at` / `updated_at` | TIMESTAMP | no | Source-record audit timestamps, not physical facility construction or renovation dates. |

---

### `ops.carriers`
**Grain:** one row per contracted carrier. 5 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `carrier_code` | VARCHAR(15) | no | Primary key. |
| `carrier_name` | VARCHAR(120) | no | Carrier name. |
| `mode` | VARCHAR(10) | no | `GROUND` · `AIR` · `OCEAN` · `RAIL`. |
| `cost_index` | NUMERIC(6,3) | no | Relative rate card, indexed to 1.00. |
| `published_otd_rate` | NUMERIC(5,3) | no | The on-time rate the carrier **claims** in its contract. Actual performance is in `ops.shipments` and is not obliged to agree. |
| `created_at` / `updated_at` | TIMESTAMP | no | Source-record audit timestamps, not the carrier's corporate start date or contract effective dates. |

---

### `ops.shipments`
**Grain:** one row per shipped order. One-to-one with orders in `SHIPPED` or `INVOICED` status. ≈ 1.3 M rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `shipment_id` | BIGINT | no | Primary key. |
| `order_id` | BIGINT | no | FK to `ops.orders`. Unique — one shipment per order. |
| `warehouse_code` | VARCHAR(10) | no | FK to `ops.warehouses`. Origin. |
| `carrier_code` | VARCHAR(15) | no | FK to `ops.carriers`. |
| `service_level` | VARCHAR(15) | no | `STANDARD` (5 day base) · `EXPEDITED` (2 day) · `ECONOMY` (9 day). Multiplies freight cost. |
| `ship_date` | DATE | no | Left the warehouse. |
| `promised_delivery_date` | DATE | no | Commitment made to the customer. Derived from service level and distance. |
| `delivered_date` | DATE | yes | Actual delivery. `NULL` while the shipment remains in transit; it is populated only by the later delivery lifecycle event. |
| `package_count` | SMALLINT | no | Pieces in the shipment. |
| `gross_weight_kg` | NUMERIC(12,2) | no | Billable weight. |
| `distance_km` | NUMERIC(12,2) | no | Origin to destination. |
| `freight_cost_usd` | NUMERIC(12,2) | no | Freight paid, USD. A function of distance, weight, carrier rate card and service level. |
| `tracking_number` | VARCHAR(30) | no | Carrier tracking reference. |
| `created_at` | TIMESTAMP | no | When Ops created the shipment record. This is distinct from `ship_date`, the physical hand-off date. |
| `updated_at` | TIMESTAMP | no | Last tracking or delivery-status update. |

**On-time delivery** is `delivered_date <= promised_delivery_date`. Actual OTD varies
by carrier, lane, service level and season, and the carrier mix is not constant across
the window.

### `ops.shipment_status_history`
**Grain:** one immutable lifecycle transition per shipment, for shipments created
after lifecycle history is enabled. The current `ops.shipments` row remains the
latest operational view; this table preserves its state changes.

| Column | Type | Null | Description |
|---|---|---|---|
| `shipment_status_event_id` | BIGINT | no | Identity primary key. |
| `shipment_id` | BIGINT | no | FK to `ops.shipments`. |
| `previous_status` | VARCHAR(20) | yes | `NULL` for initial `SHIPPED`; otherwise `SHIPPED`. |
| `new_status` | VARCHAR(20) | no | `SHIPPED` or `DELIVERED`. Only `SHIPPED → DELIVERED` is permitted after creation. |
| `occurred_at` | TIMESTAMP | no | Causal business timestamp of the hand-off or delivery. |
| `recorded_at` | TIMESTAMP | no | Source transaction timestamp when the event was persisted. |
| `source_event_id` | VARCHAR(64) | no | Unique deterministic simulator event ID. |
| `sla_due_at` | TIMESTAMP | yes | Delivery commitment for a `DELIVERED` transition; `NULL` at shipment creation. |
| `sla_status` | VARCHAR(10) | no | `ON_TIME` or `BREACHED`. |
| `anomaly_type` | VARCHAR(80) | yes | Controlled exception associated with the transition. |

---

### `ops.invoices`
**Grain:** one issued invoice per order. Created after shipment is eligible for
billing; this is the durable Ops hand-off to ERP, not an ERP ledger entry.

| Column | Type | Null | Description |
|---|---|---|---|
| `invoice_id` | BIGINT | no | Primary key. |
| `invoice_number` | VARCHAR(24) | no | Unique operational invoice reference. |
| `order_id` | BIGINT | no | FK to `ops.orders`; unique, so an order has at most one invoice. |
| `invoice_date` | DATE | no | Date billing issued the invoice. |
| `currency_code` | CHAR(3) | no | Document currency; restricted to supported order currencies. |
| `amount` | NUMERIC(18,2) | no | Positive invoiced order-line total in document currency. |
| `status` | VARCHAR(20) | no | `ISSUED` or `VOID`. Daily simulation creates `ISSUED` invoices only. |
| `created_at` | TIMESTAMP | no | Write timestamp for the operational record. |
| `updated_at` | TIMESTAMP | no | Last operational invoice-record change. Initially equal to `created_at`; it is distinct from downstream ERP posting time. |

### `ops.invoice_status_history`
**Grain:** one immutable lifecycle transition per invoice, for invoices created
after lifecycle history is enabled. The current `ops.invoices` row remains the
latest operational view; this table retains `ISSUED` and any later `VOID` event.

| Column | Type | Null | Description |
|---|---|---|---|
| `invoice_status_event_id` | BIGINT | no | Identity primary key. |
| `invoice_id` | BIGINT | no | FK to `ops.invoices`. |
| `previous_status` | VARCHAR(20) | yes | `NULL` for initial `ISSUED`; otherwise `ISSUED`. |
| `new_status` | VARCHAR(20) | no | `ISSUED` or `VOID`. Only `ISSUED → VOID` is permitted after creation. |
| `occurred_at` | TIMESTAMP | no | Causal business timestamp of issuance or voiding. |
| `recorded_at` | TIMESTAMP | no | Source transaction timestamp when the event was persisted. |
| `source_event_id` | VARCHAR(64) | no | Unique deterministic simulator event ID. |
| `sla_due_at` | TIMESTAMP | yes | Latest permitted timestamp under the causal billing SLA. |
| `sla_status` | VARCHAR(10) | no | `ON_TIME` or `BREACHED`. |
| `anomaly_type` | VARCHAR(80) | yes | Controlled exception associated with the transition. |

---

### `ops.support_cases`
**Grain:** one row per support case. ≈ 400 K rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `case_id` | BIGINT | no | Primary key. |
| `customer_id` | INTEGER | no | FK to `ops.customers`. |
| `case_type` | VARCHAR(40) | no | Seven values, from `Hardware Fault` to `Onboarding`. |
| `priority` | CHAR(2) | no | `P1` (most severe) through `P4`. Sets the expected resolution time. |
| `channel` | VARCHAR(20) | no | `Email` · `Phone` · `Portal` · `Chat`. |
| `opened_at` | TIMESTAMP | no | Case creation. |
| `resolution_hours` | NUMERIC(10,2) | yes | Wall-clock hours to resolution. `NULL` while `status = 'Open'`. |
| `status` | VARCHAR(20) | no | `Open` · `Resolved` · `Closed`. |
| `csat_score` | SMALLINT | yes | 1–5 survey response. `NULL` where the customer did not respond, which is most of the time. Response is not random — it correlates with resolution time. |
| `assigned_region` | VARCHAR(20) | no | Support team that handled the case. |
| `created_at` | TIMESTAMP | no | When the support record was created. For generated history it equals `opened_at`. |
| `updated_at` | TIMESTAMP | no | Last case-state update; resolved and closed historical cases advance it through their resolution time. |

### `ops.support_case_status_history`
**Grain:** one immutable lifecycle transition per support case, beginning with
newly simulated cases after lifecycle history is enabled. The `ops.support_cases`
row remains the current operational view.

| Column | Type | Null | Description |
|---|---|---|---|
| `support_case_status_event_id` | BIGINT | no | Identity primary key. |
| `case_id` | BIGINT | no | FK to `ops.support_cases`. |
| `previous_status` | VARCHAR(20) | yes | `NULL` for initial `Open`; otherwise `Open` or `Resolved`. |
| `new_status` | VARCHAR(20) | no | `Open`, `Resolved`, or `Closed`. Allowed path is `Open → Resolved → Closed`. |
| `occurred_at` | TIMESTAMP | no | Causal business timestamp of creation, resolution, or closure. |
| `recorded_at` | TIMESTAMP | no | Source transaction timestamp when the event was persisted. |
| `source_event_id` | VARCHAR(64) | no | Unique deterministic simulator event ID. |
| `sla_due_at` | TIMESTAMP | yes | Expected response or resolution timestamp based on case priority. |
| `sla_status` | VARCHAR(10) | no | `ON_TIME` or `BREACHED`. |
| `anomaly_type` | VARCHAR(80) | yes | Controlled exception associated with the transition. |

---

### `erp.companies`
**Grain:** one row per legal entity. 5 rows. See the table in §1.

| Column | Type | Null | Description |
|---|---|---|---|
| `company_code` | VARCHAR(10) | no | Primary key. |
| `company_name` | VARCHAR(120) | no | Registered name. |
| `functional_currency` | CHAR(3) | no | The currency this entity keeps its books in. |
| `country` | VARCHAR(60) | no | Country of incorporation. |
| `created_at` / `updated_at` | TIMESTAMP | no | ERP master-record audit timestamps, not incorporation or legal-entity effective dates. |

---

### `erp.cost_centers`
**Grain:** one row per cost centre. ≈ 27 rows. Slowly changing.

| Column | Type | Null | Description |
|---|---|---|---|
| `cost_center_code` | VARCHAR(20) | no | Primary key. |
| `cost_center_name` | VARCHAR(120) | no | Descriptive name. |
| `company_code` | VARCHAR(10) | no | FK to `erp.companies`. A cost centre belongs to exactly one entity. |
| `region` | VARCHAR(20) | no | Region it serves. |
| `function` | VARCHAR(40) | no | `Sales` · `Services` · `Support` · `Renewals`, and others added later. |
| `owner_email` | VARCHAR(160) | no | Accountable owner. |
| `valid_from` | DATE | no | Date the centre opened. Some open partway through the window. |
| `valid_to` | DATE | yes | Date it closed. `NULL` while open. |
| `is_active` | BOOLEAN | no | Current status. A retired centre keeps all of its historical postings. |
| `created_at` | TIMESTAMP | no | When the ERP master record was created; generated history aligns this with `valid_from`. |
| `updated_at` | TIMESTAMP | no | Last master-record update. A retirement advances this timestamp but does not change earlier postings. |

Validity is a range, not a flag: a centre is the correct attribution for a posting when
`valid_from <= posting_date < COALESCE(valid_to, 'infinity')`.

---

### `erp.gl_accounts`
**Grain:** one row per general ledger account. 8 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `gl_account` | VARCHAR(10) | no | Primary key. |
| `gl_name` | VARCHAR(120) | no | Account description. |
| `account_type` | VARCHAR(20) | no | `REVENUE` · `ASSET` · `LIABILITY` · `EXPENSE` · `CLEARING`. |
| `is_postable` | BOOLEAN | no | Whether postings are permitted. |
| `created_at` / `updated_at` | TIMESTAMP | no | Chart-of-accounts master-record audit timestamps. |

Revenue sits in the 4000 series: `4000` product, `4010` licence, `4020` services,
`4030` support, `4090` adjustments.

---

### `erp.fx_rates`
**Grain:** one row per currency pair per date per rate type. ≈ 3,600 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `from_currency` | CHAR(3) | no | Source currency. |
| `to_currency` | CHAR(3) | no | Always `USD` in this feed. |
| `rate_date` | DATE | no | Date the rate applies to. |
| `rate_type` | VARCHAR(10) | no | `SPOT` in this feed. |
| `rate` | NUMERIC(18,8) | no | Multiply an amount in `from_currency` by this to get USD. |
| `source_system` | VARCHAR(30) | no | Feed identifier. |
| `loaded_at` | TIMESTAMP | no | When the rate arrived. |
| `created_at` / `updated_at` | TIMESTAMP | no | Source-record audit timestamps. For an immutable feed observation both initially equal `loaded_at`. |

Two conventions of this feed, both standard in the industry and both consequential:

- **Rates are published on banking days only.** No row exists for a Saturday, a
  Sunday, or a market holiday.
- **Identity pairs are never published.** There is no `USD → USD` row, because the
  rate is 1 by definition and no provider transmits it.

---

### `erp.revenue_postings`
**Grain:** one row per accounting document line. ≈ 1.3 M rows.

The revenue ledger. This is Finance's number, and it is the one that gets reported.

| Column | Type | Null | Description |
|---|---|---|---|
| `posting_id` | BIGINT | no | Primary key. |
| `document_number` | VARCHAR(24) | no | Document reference. Unique within an entity. |
| `document_type` | VARCHAR(10) | no | `INV` invoice (positive) · `CRN` credit note (negative) · `ADJ` period adjustment (either sign). |
| `company_code` | VARCHAR(10) | no | FK to `erp.companies`. The entity that booked it. |
| `order_ref` | BIGINT | yes | `ops.orders.order_id`. `NULL` for `ADJ`, which are booked at entity level and do not trace to an order. No foreign key — different system. |
| `gl_account` | VARCHAR(10) | no | FK to `erp.gl_accounts`. |
| `cost_center_code` | VARCHAR(20) | yes | FK to `erp.cost_centers`. `NULL` for `ADJ`. |
| `posting_date` | DATE | no | The **business date** of the document. This is the date the revenue belongs to. |
| `fiscal_period` | VARCHAR(7) | no | `YYYY-MM`, always consistent with `posting_date`. |
| `document_currency` | CHAR(3) | no | The currency the customer transacted in. |
| `amount_doc` | NUMERIC(18,2) | no | Amount in document currency. |
| `company_currency` | CHAR(3) | no | The booking entity's functional currency. |
| `amount_company` | NUMERIC(18,2) | no | The same amount restated into the entity's functional currency. |
| `reverses_posting_id` | BIGINT | yes | Self-referencing FK. Set on `CRN` rows and only on `CRN` rows; points at the invoice being reversed, which is frequently in an earlier period. |
| `posted_at` | TIMESTAMP | no | When the document **hit the ledger**. Always ≥ `posting_date`, and the gap is not constant — month-end close and backdated corrections both widen it. |
| `created_at` | TIMESTAMP | no | When this immutable journal entry was recorded. For generated and simulated entries it equals `posted_at`. There is intentionally no `updated_at`: corrections create new `CRN` or `ADJ` rows rather than changing a posted entry. |

**Two currencies per row is deliberate.** A German entity books EUR for a customer who
transacted in USD. `amount_doc` and `amount_company` describe the same money on two
different bases; they are not a discrepancy.

---

### `crm.accounts` (`Account` over the API)
**Grain:** one row per CRM account. ≈ 75,000 rows.

Salesforce naming and types are preserved — `PascalCase` fields, `attributes`
envelopes on API responses, 18-character ids.

| Column | Type | Null | Description |
|---|---|---|---|
| `Id` | VARCHAR(18) | no | Salesforce record id, `001…`. Primary key. All digits after the prefix — keep it a string. |
| `Name` | VARCHAR(200) | no | Account name. |
| `AccountNumber` | VARCHAR(20) | no | Business key. The CRM holds the canonical form. |
| `Industry` | VARCHAR(60) | yes | Industry classification. |
| `Type` | VARCHAR(40) | yes | `Customer - Direct` · `Customer - Channel` · `Partner`. |
| `BillingCountry` | VARCHAR(60) | yes | Billing country. |
| `AnnualRevenue` | NUMERIC(18,2) | yes | Estimated customer revenue. Sales-entered, not audited. |
| `NumberOfEmployees` | INTEGER | yes | Estimated headcount. |
| `OwnerId` | VARCHAR(12) | yes | Owning rep. FK to `crm.sales_reps`. |
| `CreatedById` | VARCHAR(12) | no | Rep that created the CRM record. Initially the owning rep for generated records. |
| `LastModifiedById` | VARCHAR(12) | no | Rep responsible for the most recent user-visible change. |
| `CreatedDate` | VARCHAR(30) | no | Salesforce ISO-8601 with offset, e.g. `2024-03-11T09:14:22.000+0000`. |
| `LastModifiedDate` | VARCHAR(30) | no | Same format. Last user-visible modification time. |
| `SystemModstamp` | VARCHAR(30) | no | System-maintained change timestamp. Use this, then `Id` as a tie-breaker, for incremental CRM extraction. It is always ≥ `LastModifiedDate`. |
| `IsDeleted` | BOOLEAN | no | Recycle-bin flag. See the note under `crm.opportunities`. |

---

### `crm.opportunities` (`Opportunity` over the API)
**Grain:** one row per sales opportunity. ≈ 2.4 M rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `Id` | VARCHAR(18) | no | Salesforce id, `006…`. Primary key. |
| `AccountId` | VARCHAR(18) | no | FK to `crm.accounts."Id"`. |
| `Name` | VARCHAR(260) | no | Opportunity name. |
| `StageName` | VARCHAR(40) | no | `Prospecting` · `Qualification` · `Proposal` · `Negotiation` · `Closed Won` · `Closed Lost`. Only `Closed Won` produced an order. |
| `Amount` | NUMERIC(18,2) | yes | Forecast value in `CurrencyIsoCode`. This is the **rep's forecast**, not what was invoiced. The order is the truth. |
| `CurrencyIsoCode` | CHAR(3) | no | Opportunity currency. |
| `CloseDate` | DATE | no | Actual close for closed stages, forecast close for open ones. |
| `Probability` | SMALLINT | no | Stage-implied win probability, 0–100. |
| `LeadSource` | VARCHAR(40) | yes | Six values, including `Marketing Campaign`. |
| `CampaignId` | VARCHAR(12) | yes | FK to `crm.campaigns`. Populated only when `LeadSource = 'Marketing Campaign'`. |
| `DiscountPercent` | NUMERIC(5,2) | yes | Discount approved on the deal. Subject to an approval ceiling that has not been constant over the window. |
| `LossReason` | VARCHAR(40) | yes | Set when and only when `StageName = 'Closed Lost'`. `Price` · `Competitor` · `No Decision` · `Timing` · `Feature Gap` · `Budget Cut`. |
| `SalesCycleDays` | INTEGER | yes | `CloseDate - CreatedDate`. Varies by segment. |
| `OwnerId` | VARCHAR(12) | yes | Owning rep. FK to `crm.sales_reps`. |
| `CreatedById` | VARCHAR(12) | no | Rep that created the opportunity. Initially the owning rep for generated records. |
| `LastModifiedById` | VARCHAR(12) | no | Rep responsible for the most recent user-visible change. |
| `CreatedDate` | VARCHAR(30) | no | ISO-8601 with offset. |
| `LastModifiedDate` | VARCHAR(30) | no | Last user-visible modification time. |
| `SystemModstamp` | VARCHAR(30) | no | System-maintained extraction watermark. Use `SystemModstamp`, then `Id`, for incremental extraction; it is always ≥ `LastModifiedDate`. |
| `IsDeleted` | BOOLEAN | no | Recycle-bin flag. |

**On `IsDeleted`.** The API mirrors Salesforce exactly: `/query` does not return
soft-deleted records, `/queryAll` does, and `/sobjects/Opportunity/deleted` lists the
ids that were removed. Which endpoint you extract with determines whether these rows
exist in your copy of the data. Orders in `ops` were placed before any of this
happened and still reference the ids.

---

### `crm.opportunity_history` (`OpportunityHistory` over the API)
**Grain:** one immutable opportunity-stage transition. The generated seed base is
schema-bearing and empty; daily simulator rows are published in one immutable Parquet
partition per `business_date` and are exposed through FakeForce as `OpportunityHistory`.

| Column | Type | Null | Description |
|---|---|---|---|
| `Id` | VARCHAR(64) | no | Deterministic CRM simulator event id. Primary key and retry de-duplication key. |
| `OpportunityId` | VARCHAR(18) | no | Salesforce opportunity id (`006…`) whose stage changed. |
| `PreviousStageName` | VARCHAR(40) | yes | Stage before the event. `NULL` only for the initial `opportunity_created` history row. |
| `StageName` | VARCHAR(40) | no | Stage after the event. A stage-change row must differ from `PreviousStageName`. |
| `CreatedDate` | VARCHAR(30) | no | Business-effective CRM event time, in Salesforce ISO-8601 format. |
| `CreatedById` | VARCHAR(12) | no | Rep responsible for the transition. |
| `SystemModstamp` | VARCHAR(30) | no | CRM extraction watermark for the event; use this with `Id` as a tie-breaker. |

This is append-only history, not an SCD and not a replacement for
`crm.opportunities`: the latter remains the latest current state. There is no
`IsDeleted`, update, or overwrite operation for history rows. Before a simulation date
is completed, reconciliation reads (but never edits) that date's manifest and
partition. It verifies the immutable path, Parquet schema, deterministic event IDs,
CRM event fields, business-date placement, and valid initial/stage-change shape. A
failure leaves the simulation date incomplete for safe retry.

---

### `crm.sales_reps`
**Grain:** one row per sales representative. ≈ 220 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `rep_id` | VARCHAR(12) | no | Primary key, `REP-nnnn`. |
| `rep_name` | VARCHAR(120) | no | Full name. |
| `region` | VARCHAR(20) | no | Assigned territory. |
| `hire_date` | DATE | no | Start date. Reps ramp over roughly two quarters. |
| `departure_date` | DATE | yes | Leaving date. `NULL` for current staff. |
| `quota_usd_annual` | NUMERIC(14,2) | no | Annual quota, USD. Regionally differentiated. |
| `manager_id` | VARCHAR(12) | no | Reporting line. Not a foreign key — managers are not in this table. |

Reps differ from one another in win rate, average deal size and willingness to
discount. Those differences are stable per rep and are not recorded anywhere.

---

### `crm.campaigns`
**Grain:** one row per marketing campaign. ≈ 150 rows.

| Column | Type | Null | Description |
|---|---|---|---|
| `campaign_id` | VARCHAR(12) | no | Primary key, `CMP-nnnn`. |
| `campaign_name` | VARCHAR(160) | no | Campaign name. |
| `campaign_type` | VARCHAR(40) | no | `Email Nurture` · `Paid Search` · `Field Event` · `Webinar` · `Content Syndication` · `Partner Co-Marketing`. |
| `target_region` | VARCHAR(20) | no | Intended region, or `GLOBAL`. |
| `target_segment` | VARCHAR(40) | no | Intended segment, or `ALL`. |
| `start_date` / `end_date` | DATE | no | Campaign window. |
| `budget_usd` | NUMERIC(14,2) | no | Committed spend. |
| `channel_owner` | VARCHAR(120) | no | Responsible marketer. |

Attribution runs `campaigns → opportunities.CampaignId → orders → revenue`, which is
enough for a cost-per-opportunity and a pipeline-return calculation.

---

## 4. Business processes

**Lead to order.** A campaign or an outbound motion creates an opportunity owned by a
rep. It moves through stages and closes won or lost, with a discount negotiated along
the way. Won opportunities become orders in `ops`. `ECOMM` orders skip this entirely
and have no opportunity.

**Order to cash.** An order is placed with a currency, a channel and a discount, and
carries one line per product. When it is shipped or invoiced, Finance books a revenue
document in the entity that owns the customer's region — in that entity's functional
currency, on the ledger's own clock. Returns and billing corrections are booked later
as credit notes pointing back at the original invoice.

**Order to delivery.** A shipped order leaves a regional warehouse on a carrier at a
service level, with a promise date derived from both. What actually happened is in
`delivered_date`. Carrier mix, rate cards and seasonal conditions all move over the
window.

**Service.** Customers open cases. Priority sets the expected resolution time; actual
resolution time varies by team and period. Some customers respond to the CSAT survey.

---

## 5. Metric definitions

| Metric | Definition |
|---|---|
| **Bookings** | Sum of `line_amount` on non-cancelled orders, by `order_date`. The sales view. |
| **Billed revenue** | Sum of `amount_doc` on `erp.revenue_postings`, by `posting_date`, net of credit notes. The Finance view. Will not equal bookings, and the gap is informative. |
| **Gross margin** | `line_amount` less `quantity × unit_cost_usd`, once both are on the same currency basis. |
| **ASP** | Average selling price: `line_amount / quantity`. |
| **Realised discount** | `1 - (unit_price / list_price_usd)` on a common currency basis. Differs from `discount_pct`, which is the approved figure. |
| **Win rate** | Closed Won ÷ (Closed Won + Closed Lost), over a close-date window. |
| **Sales cycle** | `CloseDate - CreatedDate` on closed opportunities. |
| **Quota attainment** | Rep bookings ÷ pro-rated `quota_usd_annual`. |
| **On-time delivery (OTD)** | `delivered_date <= promised_delivery_date`, over delivered shipments. |
| **Freight cost ratio** | `freight_cost_usd` ÷ order revenue, USD basis. |
| **Cost per kg / per km** | Freight efficiency by carrier, lane and service level. |
| **Logo churn** | Customers with no order in a trailing window who had one before it. |
| **Net revenue retention** | Revenue from a cohort this period ÷ revenue from the same cohort last period. |
| **Time to resolution** | `resolution_hours` on resolved cases, usually reported as a median or p90. |
| **Campaign cost per opportunity** | `budget_usd` ÷ opportunities attributed to the campaign. |

---

## 6. What the data will support

**Descriptive.** Revenue by period, region, entity, segment, category and channel.
Order and basket trends. Rep leaderboards and quota attainment. Funnel conversion by
stage. Carrier and lane scorecards. Case volume and resolution distributions. Cohort
retention curves.

**Diagnostic.** Margin is not flat across the window and neither is win rate, freight
cost per kilogram, on-time delivery, or churn by region. Each movement has a cause
that is present in the data — a mix shift, a policy change, a supplier constraint, a
carrier decision, a competitor, a reorganisation. Decomposition, changepoint detection
and cohort slicing will find them. Nothing is labelled.

**Predictive.** Daily and weekly order volume by region is a well-behaved forecasting
target with trend, weekly seasonality, quarter-end effects and holidays. Opportunity
win/loss has real signal in amount, discount, rep, source and cycle length. Customer
churn has signal in order recency, case history and resolution time. Shipment lateness
has signal in carrier, lane, service level and season. Freight cost is close to a
regression on distance, weight, service level and carrier.

---

## 7. Conventions and cautions

- **Central contract registry.** `analytics.metadata.table_versions` and
  `analytics.metadata.column_versions` hold versioned Ops and ERP table
  contracts, separate from source-owned data. Run
  `python -m generator.metadata_registry` in the `migrations` container after
  a source schema change; unchanged contracts retain their existing version.
  CRM contract registration remains API-driven and is intentionally not inferred
  from local Parquet files.

- **Timestamps are second-granularity and naive.** No timezone, no sub-second component.
- **Salesforce ids are all-digit strings.** `pandas.read_csv` will read
  `001000000000000001` as an integer and drop the leading zeros unless you pin the
  dtype. The same applies to `AccountId`, `OwnerId` and `CampaignId`.
- **CRM audit watermark.** Extract changed Accounts and Opportunities using the
  ordered pair `(SystemModstamp, Id)`, rather than `LastModifiedDate` alone.
  Older locally generated seed files remain readable: FakeForce presents the
  historical `LastModifiedDate` and `OwnerId` values as compatibility fallbacks
  until the seed is regenerated with the physical audit columns.
- **Ops audit rollout.** Migration `002_add_audit_metadata.sql` adds missing
  audit fields as nullable columns so an existing large database is not rewritten
  during deployment. New seed loads and simulated events populate them from this
  release onward. Migration `003_add_audit_backfill_state.sql` supplies durable
  checkpoints for `python -m generator.audit_backfill`; it updates a bounded
  primary-key batch and can resume after interruption. Until that command has
  completed, do not treat a `NULL` audit field in a pre-migration row as an
  absent business event or a deletion. After the completed backfill has been
  validated, migration `004_enforce_audit_metadata.sql` makes these fields
  mandatory and rejects any `updated_at` earlier than `created_at`.
- **ERP audit rollout.** ERP migration `002_add_audit_metadata.sql` follows the
  same non-rewriting introduction for ERP masters and FX rates. Migration
  `003_add_audit_backfill_state.sql` creates a separate ERP-local durable
  checkpoint ledger; run `python -m generator.audit_backfill --system erp`
  in bounded batches (or `--until-complete`) to populate historical records.
  The large `revenue_postings` ledger resumes by `posting_id`, so an interrupted
  run never restarts its completed range. After a zero-null validation, migration
  `004_enforce_audit_metadata.sql` makes the fields mandatory and rejects reverse
  timestamp order. Revenue postings are append-only accounting entries:
  `created_at` equals `posted_at`, while corrections must be separate `CRN` or
  `ADJ` rows, not in-place updates. Migration
  `005_enforce_revenue_posting_immutability.sql` enforces that rule in the ERP
  database: `UPDATE` and `DELETE` are rejected for posted journal records.
- **Money is in the currency named beside it.** `ops` amounts are in
  `orders.currency_code`; `erp` carries both bases explicitly. Nothing is pre-converted
  to USD except `unit_cost_usd`, `freight_cost_usd`, `list_price_usd` and
  `credit_limit_usd`, which say so in their names.
- **Amounts are rounded to their ISO 4217 minor unit.** JPY has no minor unit, so JPY
  amounts are whole numbers and are numerically much larger than their USD equivalents.
- **`is_active` and `is_discontinued` are current-state flags**, not history. Date
  ranges (`valid_from` / `valid_to`, `launch_date` / `discontinued_on`) carry the
  history.
- **`NULL` frequently means something specific.** A `NULL` `opportunity_ref` means the
  order came through self-serve. A `NULL` `delivered_date` means in transit. A `NULL`
  `csat_score` means no survey response. A `NULL` `order_ref` on a posting means an
  entity-level adjustment. None of these are missing data.
- **The generator is deterministic.** The same `--seed` and `--scale` reproduce the
  same dataset exactly, so your tests and models stay comparable across runs.
