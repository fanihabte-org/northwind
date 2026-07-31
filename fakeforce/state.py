"""Durable FakeForce metadata backed by DuckDB.

This module intentionally owns only operational metadata.  Query execution and
mutable sObject tables will use the same database in later subtasks, but no
request handler should hold a global DuckDB connection.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import json
import csv
import io
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import duckdb

from fakeforce.config import Settings
from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType, validate_transition


SCHEMA_VERSION = "0001_initial_operational_state"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS dataset_snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    catalog_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS cursors (
    locator_id VARCHAR PRIMARY KEY,
    api_version VARCHAR NOT NULL,
    normalized_query VARCHAR NOT NULL,
    query_hash VARCHAR NOT NULL,
    object_name VARCHAR NOT NULL,
    dataset_snapshot_id VARCHAR NOT NULL,
    batch_size INTEGER NOT NULL,
    current_offset BIGINT NOT NULL DEFAULT 0,
    total_size BIGINT NOT NULL,
    result_artifact VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    last_accessed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    expires_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR PRIMARY KEY,
    job_type VARCHAR NOT NULL,
    api_version VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    object_name VARCHAR,
    operation VARCHAR,
    query_text VARCHAR,
    dataset_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    expires_at TIMESTAMP,
    error_code VARCHAR,
    error_message VARCHAR,
    options_json VARCHAR NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_id VARCHAR NOT NULL,
    checkpoint_number BIGINT NOT NULL,
    source_file VARCHAR,
    source_row_group BIGINT,
    source_row BIGINT,
    input_byte_offset BIGINT,
    csv_row_number BIGINT,
    internal_batch_number BIGINT,
    records_processed BIGINT NOT NULL DEFAULT 0,
    records_failed BIGINT NOT NULL DEFAULT 0,
    bytes_written BIGINT NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    artifact_path VARCHAR,
    committed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (job_id, checkpoint_number)
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id VARCHAR PRIMARY KEY,
    owner_type VARCHAR NOT NULL,
    owner_id VARCHAR NOT NULL,
    artifact_type VARCHAR NOT NULL,
    path VARCHAR NOT NULL UNIQUE,
    byte_size BIGINT NOT NULL,
    checksum VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    expires_at TIMESTAMP,
    status VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS api_usage_events (
    event_id VARCHAR PRIMARY KEY,
    occurred_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    route VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS job_row_results (
    job_id VARCHAR NOT NULL,
    csv_row_number BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    record_id VARCHAR,
    error_message VARCHAR,
    source_values_json VARCHAR NOT NULL,
    PRIMARY KEY (job_id, csv_row_number)
);
"""


class StateStore:
    """Creates and opens the single persistent FakeForce DuckDB database."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    @classmethod
    def from_settings(cls, settings: Settings) -> "StateStore":
        return cls(settings.state_directory / "fakeforce.duckdb")

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(self.database_path))
        try:
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connection() as conn:
            conn.execute(_SCHEMA_SQL)
            # DuckDB cannot add a column with a DEFAULT/NOT NULL constraint
            # in-place, so old state files are migrated in two safe steps.
            conn.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS options_json VARCHAR")
            conn.execute("UPDATE jobs SET options_json = '{}' WHERE options_json IS NULL")
            conn.execute(
                "UPDATE jobs SET expires_at = created_at + INTERVAL 7 DAY "
                "WHERE expires_at IS NULL"
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)",
                [SCHEMA_VERSION],
            )

    def register_snapshot(self, snapshot_id: str, catalog_hash: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO dataset_snapshots (snapshot_id, catalog_hash) VALUES (?, ?)",
                [snapshot_id, catalog_hash],
            )

    def create_cursor(
        self,
        *,
        locator_id: str,
        api_version: str,
        normalized_query: str,
        query_hash: str,
        object_name: str,
        dataset_snapshot_id: str,
        batch_size: int,
        page_offset: int,
        total_size: int,
        result_artifact: str,
        expires_at: datetime,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO cursors (
                    locator_id, api_version, normalized_query, query_hash, object_name,
                    dataset_snapshot_id, batch_size, current_offset, total_size,
                    result_artifact, status, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                [
                    locator_id,
                    api_version,
                    normalized_query,
                    query_hash,
                    object_name,
                    dataset_snapshot_id,
                    batch_size,
                    page_offset,
                    total_size,
                    result_artifact,
                    expires_at,
                ],
            )

    def get_cursor(self, locator_id: str) -> "StoredCursor | None":
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT normalized_query, api_version, current_offset, batch_size, total_size,
                       result_artifact, dataset_snapshot_id
                FROM cursors
                WHERE locator_id = ? AND status = 'open' AND expires_at > current_timestamp
                """,
                [locator_id],
            ).fetchone()
        if row is None:
            return None
        return StoredCursor(
            normalized_query=row[0],
            api_version=row[1],
            page_offset=row[2],
            batch_size=row[3],
            total_size=row[4],
            result_artifact=row[5],
            dataset_snapshot_id=row[6],
        )

    def count_open_cursors(self) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT count(*) FROM cursors WHERE status = 'open' AND expires_at > current_timestamp"
            ).fetchone()[0]

    def expire_cursors(self) -> list[str]:
        """Remove expired locator metadata and return its artifact paths."""
        with self.connection() as conn:
            paths = [
                row[0]
                for row in conn.execute(
                    "SELECT result_artifact FROM cursors WHERE expires_at <= current_timestamp"
                ).fetchall()
            ]
            conn.execute("DELETE FROM cursors WHERE expires_at <= current_timestamp")
        return paths

    def active_cursor_artifacts(self) -> set[str]:
        with self.connection() as conn:
            return {
                row[0]
                for row in conn.execute(
                    "SELECT result_artifact FROM cursors WHERE status = 'open' AND expires_at > current_timestamp"
                ).fetchall()
            }

    def record_api_usage(self, route: str) -> int:
        """Record one REST API call and return use during the rolling 24 hours."""
        with self.connection() as conn:
            conn.execute(
                "DELETE FROM api_usage_events WHERE occurred_at <= current_timestamp - INTERVAL 24 HOUR"
            )
            conn.execute(
                "INSERT INTO api_usage_events (event_id, route) VALUES (?, ?)",
                [uuid4().hex, route],
            )
            return conn.execute("SELECT count(*) FROM api_usage_events").fetchone()[0]

    def api_usage_last_24_hours(self) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT count(*) FROM api_usage_events "
                "WHERE occurred_at > current_timestamp - INTERVAL 24 HOUR"
            ).fetchone()[0]

    def create_job(self, job: BulkJob) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, job_type, api_version, state, object_name, operation,
                    query_text, dataset_snapshot_id, error_code, error_message, expires_at, options_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    job.job_id,
                    job.job_type.value,
                    job.api_version,
                    job.state.value,
                    job.object_name,
                    job.operation,
                    job.query_text,
                    job.dataset_snapshot_id,
                    job.error_code,
                    job.error_message,
                    job.expires_at,
                    json.dumps(job.options, sort_keys=True),
                ],
            )

    def get_job(self, job_id: str) -> BulkJob | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT job_id, job_type, api_version, state, object_name, operation,
                       query_text, dataset_snapshot_id, error_code, error_message, options_json
                FROM jobs
                WHERE job_id = ? AND (expires_at IS NULL OR expires_at > current_timestamp)
                """,
                [job_id],
            ).fetchone()
        if row is None:
            return None
        return BulkJob(
            row[0], BulkJobType(row[1]), row[2], BulkJobState(row[3]), row[4], row[5],
            row[6], row[7], row[8], row[9], options=json.loads(row[10] or "{}")
        )

    def transition_job(
        self,
        job_id: str,
        next_state: BulkJobState,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> BulkJob:
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                row = conn.execute(
                    "SELECT state FROM jobs WHERE job_id = ?", [job_id]
                ).fetchone()
                if row is None:
                    raise KeyError(job_id)
                validate_transition(BulkJobState(row[0]), next_state)
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?, error_code = ?, error_message = ?, updated_at = current_timestamp
                    WHERE job_id = ?
                    """,
                    [next_state.value, error_code, error_message, job_id],
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        job = self.get_job(job_id)
        assert job is not None
        return job

    def add_job_checkpoint(
        self,
        job_id: str,
        records_processed: int,
        bytes_written: int,
        artifact_path: str,
        *,
        records_failed: int = 0,
        csv_row_number: int | None = None,
    ) -> None:
        with self.connection() as conn:
            checkpoint_number = conn.execute(
                "SELECT coalesce(max(checkpoint_number), -1) + 1 FROM job_checkpoints WHERE job_id = ?",
                [job_id],
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO job_checkpoints (
                    job_id, checkpoint_number, records_processed, records_failed,
                    bytes_written, csv_row_number, artifact_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    job_id,
                    checkpoint_number,
                    records_processed,
                    records_failed,
                    bytes_written,
                    csv_row_number,
                    artifact_path,
                ],
            )

    def latest_job_checkpoint_records(self, job_id: str) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT coalesce(max(records_processed), 0) FROM job_checkpoints WHERE job_id = ?",
                [job_id],
            ).fetchone()[0]

    def ingest_progress(self, job_id: str) -> "IngestProgress":
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT records_processed, records_failed, coalesce(csv_row_number, 0)
                FROM job_checkpoints WHERE job_id = ?
                ORDER BY checkpoint_number DESC LIMIT 1
                """,
                [job_id],
            ).fetchone()
        return IngestProgress(*row) if row is not None else IngestProgress(0, 0, 0)

    def ingest_result_count(self, job_id: str, status: str) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT count(*) FROM job_row_results WHERE job_id = ? AND status = ?",
                [job_id, status],
            ).fetchone()[0]

    def stream_ingest_results(
        self, job_id: str, status: str, source_fields: list[str]
    ) -> Iterator[bytes]:
        """Yield a bounded CSV response directly from durable row outcomes."""
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        if status == "success":
            writer.writerow(["sf__Id", "sf__Created", "sf__Error"])
        else:
            writer.writerow(["sf__Id", "sf__Error", *source_fields])
        yield output.getvalue().encode()
        output.seek(0)
        output.truncate(0)
        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT record_id, error_message, source_values_json
                FROM job_row_results WHERE job_id = ? AND status = ?
                ORDER BY csv_row_number
                """,
                [job_id, status],
            )
            while rows := cursor.fetchmany(1000):
                for record_id, error_message, source_values_json in rows:
                    if status == "success":
                        writer.writerow([record_id, "true", ""])
                    else:
                        source_values = json.loads(source_values_json)
                        writer.writerow(
                            [record_id or "", error_message or "", *[
                                source_values.get(field, "") for field in source_fields
                            ]]
                        )
                    yield output.getvalue().encode()
                    output.seek(0)
                    output.truncate(0)

    def resumable_query_jobs(self) -> list[str]:
        with self.connection() as conn:
            return [
                row[0]
                for row in conn.execute(
                    "SELECT job_id FROM jobs WHERE job_type = 'query' "
                    "AND state IN ('UploadComplete', 'InProgress') "
                    "AND (expires_at IS NULL OR expires_at > current_timestamp)"
                ).fetchall()
            ]

    def resumable_ingest_jobs(self) -> list[str]:
        with self.connection() as conn:
            return [
                row[0]
                for row in conn.execute(
                    "SELECT job_id FROM jobs WHERE job_type = 'ingest' "
                    "AND state IN ('UploadComplete', 'InProgress') "
                    "AND (expires_at IS NULL OR expires_at > current_timestamp)"
                ).fetchall()
            ]

    def expire_jobs(self) -> list[str]:
        """Delete expired durable jobs and return their owned job IDs."""
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                job_ids = [
                    row[0]
                    for row in conn.execute(
                        "SELECT job_id FROM jobs WHERE expires_at <= current_timestamp"
                    ).fetchall()
                ]
                if job_ids:
                    placeholders = ", ".join("?" for _ in job_ids)
                    conn.execute(
                        f"DELETE FROM artifacts WHERE owner_type = 'job' AND owner_id IN ({placeholders})",
                        job_ids,
                    )
                    conn.execute(
                        f"DELETE FROM job_checkpoints WHERE job_id IN ({placeholders})", job_ids
                    )
                    conn.execute(f"DELETE FROM jobs WHERE job_id IN ({placeholders})", job_ids)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return job_ids


@dataclass(frozen=True)
class StoredCursor:
    normalized_query: str
    api_version: str
    page_offset: int
    batch_size: int
    total_size: int
    result_artifact: str
    dataset_snapshot_id: str


@dataclass(frozen=True)
class IngestProgress:
    records_processed: int
    records_failed: int
    csv_row_number: int
