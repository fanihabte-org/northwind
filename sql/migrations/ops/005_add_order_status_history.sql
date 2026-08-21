-- Northwind Systems :: Operations source database, migration 005
-- Immutable lifecycle transitions for the current-state order header.

CREATE TABLE IF NOT EXISTS ops.order_status_history (
    order_status_event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    order_id BIGINT NOT NULL,
    previous_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_event_id VARCHAR(64) NOT NULL,
    sla_due_at TIMESTAMP,
    sla_status VARCHAR(10) NOT NULL,
    anomaly_type VARCHAR(80),
    CONSTRAINT pk_order_status_history PRIMARY KEY (order_status_event_id),
    CONSTRAINT fk_order_status_history_order FOREIGN KEY (order_id) REFERENCES ops.orders (order_id),
    CONSTRAINT uq_order_status_history_event UNIQUE (source_event_id),
    CONSTRAINT ck_order_history_previous CHECK (previous_status IS NULL OR previous_status IN ('PENDING', 'SHIPPED', 'INVOICED', 'CANCELLED')),
    CONSTRAINT ck_order_history_new CHECK (new_status IN ('PENDING', 'SHIPPED', 'INVOICED', 'CANCELLED')),
    CONSTRAINT ck_order_history_transition CHECK (
        (previous_status IS NULL AND new_status = 'PENDING')
        OR (previous_status = 'PENDING' AND new_status IN ('SHIPPED', 'CANCELLED'))
        OR (previous_status = 'SHIPPED' AND new_status IN ('INVOICED', 'CANCELLED'))
    ),
    CONSTRAINT ck_order_history_recorded CHECK (recorded_at >= occurred_at),
    CONSTRAINT ck_order_history_sla CHECK (sla_status IN ('ON_TIME', 'BREACHED'))
);

CREATE INDEX IF NOT EXISTS ix_order_status_history_order_occurred
    ON ops.order_status_history (order_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_order_status_history_sla_occurred
