-- Northwind Systems :: Operations source database, migration 008
-- Immutable lifecycle transitions for the support-case operational record.

CREATE TABLE IF NOT EXISTS ops.support_case_status_history (
    support_case_status_event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    case_id BIGINT NOT NULL,
    previous_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    source_event_id VARCHAR(64) NOT NULL,
    sla_due_at TIMESTAMP,
    sla_status VARCHAR(10) NOT NULL,
    anomaly_type VARCHAR(80),
    CONSTRAINT pk_support_case_status_history PRIMARY KEY (support_case_status_event_id),
    CONSTRAINT fk_support_case_status_history_case
        FOREIGN KEY (case_id) REFERENCES ops.support_cases (case_id),
    CONSTRAINT uq_support_case_status_history_event UNIQUE (source_event_id),
    CONSTRAINT ck_support_case_history_previous
        CHECK (previous_status IS NULL OR previous_status IN ('Open', 'Resolved', 'Closed')),
    CONSTRAINT ck_support_case_history_new
        CHECK (new_status IN ('Open', 'Resolved', 'Closed')),
    CONSTRAINT ck_support_case_history_transition CHECK (
        (previous_status IS NULL AND new_status = 'Open')
        OR (previous_status = 'Open' AND new_status = 'Resolved')
        OR (previous_status = 'Resolved' AND new_status = 'Closed')
    ),
    CONSTRAINT ck_support_case_history_recorded CHECK (recorded_at >= occurred_at),
    CONSTRAINT ck_support_case_history_sla CHECK (sla_status IN ('ON_TIME', 'BREACHED'))
);

CREATE INDEX IF NOT EXISTS ix_support_case_status_history_case_occurred
    ON ops.support_case_status_history (case_id, occurred_at);

CREATE INDEX IF NOT EXISTS ix_support_case_status_history_sla_occurred
    ON ops.support_case_status_history (sla_status, occurred_at);
