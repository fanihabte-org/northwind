-- Northwind Systems :: Operations source database, migration 007
-- Immutable lifecycle transitions for the invoice operational record.

CREATE TABLE IF NOT EXISTS ops.invoice_status_history (
    invoice_status_event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    invoice_id BIGINT NOT NULL,
    previous_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_event_id VARCHAR(64) NOT NULL,
    sla_due_at TIMESTAMP,
    sla_status VARCHAR(10) NOT NULL,
    anomaly_type VARCHAR(80),
    CONSTRAINT pk_invoice_status_history PRIMARY KEY (invoice_status_event_id),
    CONSTRAINT fk_invoice_status_history_invoice
        FOREIGN KEY (invoice_id) REFERENCES ops.invoices (invoice_id),
    CONSTRAINT uq_invoice_status_history_event UNIQUE (source_event_id),
    CONSTRAINT ck_invoice_history_previous
        CHECK (previous_status IS NULL OR previous_status IN ('ISSUED', 'VOID')),
    CONSTRAINT ck_invoice_history_new
        CHECK (new_status IN ('ISSUED', 'VOID')),
    CONSTRAINT ck_invoice_history_transition CHECK (
        (previous_status IS NULL AND new_status = 'ISSUED')
        OR (previous_status = 'ISSUED' AND new_status = 'VOID')
    ),
    CONSTRAINT ck_invoice_history_recorded CHECK (recorded_at >= occurred_at),
    CONSTRAINT ck_invoice_history_sla CHECK (sla_status IN ('ON_TIME', 'BREACHED'))
);

CREATE INDEX IF NOT EXISTS ix_invoice_status_history_invoice_occurred
    ON ops.invoice_status_history (invoice_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_invoice_status_history_sla_occurred
    ON ops.invoice_status_history (sla_status, occurred_at);
