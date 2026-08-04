-- Northwind Systems :: analytics metadata registry, migration 001
-- This warehouse-local registry describes source contracts; it never copies
-- operational or financial business rows into analytics.

CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.table_versions (
    table_version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    source_schema VARCHAR(63) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    version INTEGER NOT NULL,
    schema_fingerprint CHAR(64) NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    retired_at TIMESTAMPTZ,
    CONSTRAINT uq_metadata_table_version UNIQUE (source_system, source_schema, table_name, version),
    CONSTRAINT uq_metadata_table_fingerprint UNIQUE (source_system, source_schema, table_name, schema_fingerprint),
    CONSTRAINT ck_metadata_table_version CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS metadata.column_versions (
    table_version_id BIGINT NOT NULL REFERENCES metadata.table_versions (table_version_id),
    ordinal_position INTEGER NOT NULL,
    column_name VARCHAR(63) NOT NULL,
    data_type TEXT NOT NULL,
    is_nullable BOOLEAN NOT NULL,
    character_maximum_length INTEGER,
    numeric_precision INTEGER,
    numeric_scale INTEGER,
    PRIMARY KEY (table_version_id, column_name),
    CONSTRAINT uq_metadata_column_ordinal UNIQUE (table_version_id, ordinal_position),
    CONSTRAINT ck_metadata_column_ordinal CHECK (ordinal_position > 0)
);

CREATE INDEX IF NOT EXISTS ix_metadata_table_current
    ON metadata.table_versions (source_system, source_schema, table_name, version DESC)
    WHERE retired_at IS NULL;

CREATE OR REPLACE VIEW metadata.current_columns AS
SELECT
    table_version.source_system,
    table_version.source_schema,
    table_version.table_name,
    table_version.version,
    table_version.schema_fingerprint,
    column_version.ordinal_position,
    column_version.column_name,
    column_version.data_type,
    column_version.is_nullable,
    column_version.character_maximum_length,
    column_version.numeric_precision,
    column_version.numeric_scale,
    table_version.captured_at
FROM metadata.table_versions AS table_version
JOIN metadata.column_versions AS column_version
  ON column_version.table_version_id = table_version.table_version_id
WHERE table_version.retired_at IS NULL;
