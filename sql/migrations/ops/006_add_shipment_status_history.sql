-- Northwind Systems :: Operations source database, migration 006
-- Immutable lifecycle transitions for the shipment operational record.

CREATE TABLE IF NOT EXISTS ops.shipment_status_history (
    shipment_status_event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    shipment_id BIGINT NOT NULL,
    previous_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_event_id VARCHAR(64) NOT NULL,
    sla_due_at TIMESTAMP,
    sla_status VARCHAR(10) NOT NULL,
    anomaly_type VARCHAR(80),
    CONSTRAINT pk_shipment_status_history PRIMARY KEY (shipment_status_event_id),
    CONSTRAINT fk_shipment_status_history_shipment
        FOREIGN KEY (shipment_id) REFERENCES ops.shipments (shipment_id),
    CONSTRAINT uq_shipment_status_history_event UNIQUE (source_event_id),
    CONSTRAINT ck_shipment_history_previous
        CHECK (previous_status IS NULL OR previous_status IN ('SHIPPED', 'DELIVERED')),
    CONSTRAINT ck_shipment_history_new
        CHECK (new_status IN ('SHIPPED', 'DELIVERED')),
    CONSTRAINT ck_shipment_history_transition CHECK (
        (previous_status IS NULL AND new_status = 'SHIPPED')
        OR (previous_status = 'SHIPPED' AND new_status = 'DELIVERED')
    ),
    CONSTRAINT ck_shipment_history_recorded CHECK (recorded_at >= occurred_at),
    CONSTRAINT ck_shipment_history_sla CHECK (sla_status IN ('ON_TIME', 'BREACHED'))
);

CREATE INDEX IF NOT EXISTS ix_shipment_status_history_shipment_occurred
    ON ops.shipment_status_history (shipment_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_shipment_status_history_sla_occurred
    ON ops.shipment_status_history (sla_status, occurred_at);
