"""Durable, streaming Bulk API 2.0 Ingest control-plane helpers."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

import pyarrow as pa

from fakeforce.bulk.jobs import BulkJobState, BulkJobType
from fakeforce.catalog import DatasetCatalog
from fakeforce.engine import DuckDBEngine, _quote_identifier
from fakeforce.state import StateStore
from fakeforce.storage import atomic_binary_writer, require_disk_reserve


class IngestValidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class IngestUploadTooLarge(ValueError):
    pass


class IngestFileError(ValueError):
    pass


@dataclass(frozen=True)
class IngestConfiguration:
    object_name: str
    operation: str
    content_type: str
    line_ending: str
    column_delimiter: str


def validate_ingest_create(
    payload: Mapping[str, object], catalog: DatasetCatalog
) -> IngestConfiguration:
    object_name = payload.get("object")
    if not isinstance(object_name, str) or not object_name:
        raise IngestValidationError("INVALIDJOB", "Bulk ingest jobs require an object")
    spec = catalog.get(object_name)
    if spec is None:
        raise IngestValidationError("NOT_FOUND", f"Unknown configured object: {object_name}")
    if spec.mode != "mutable":
        raise IngestValidationError("INVALIDJOB", f"{object_name} is not configured for ingest")
    operation = payload.get("operation")
    if operation != "insert":
        raise IngestValidationError("INVALIDJOB", "only insert ingest operation is currently supported")
    content_type = payload.get("contentType", "CSV")
    if content_type != "CSV":
        raise IngestValidationError("INVALIDJOB", "only CSV ingest content is currently supported")
    line_ending = payload.get("lineEnding", "LF")
    column_delimiter = payload.get("columnDelimiter", "COMMA")
    if line_ending not in {"LF", "CRLF"}:
        raise IngestValidationError("INVALIDJOB", "unsupported CSV line ending")
    if column_delimiter not in {"COMMA", "TAB", "PIPE", "SEMICOLON"}:
        raise IngestValidationError("INVALIDJOB", "unsupported CSV column delimiter")
    return IngestConfiguration(object_name, operation, content_type, line_ending, column_delimiter)


class IngestUploadStore:
    def __init__(self, root: Path, *, disk_reserve_bytes: int) -> None:
        self.root = root
        self.disk_reserve_bytes = disk_reserve_bytes

    def path_for(self, job_id: str) -> Path:
        root = self.root.resolve()
        path = (root / job_id / "input.csv").resolve()
        if root not in path.parents:
            raise ValueError("bulk ingest artifact path escapes configured root")
        return path

    async def write(self, job_id: str, chunks: AsyncIterator[bytes], max_bytes: int) -> int:
        path = self.path_for(job_id)
        require_disk_reserve(path.parent, self.disk_reserve_bytes)
        written = 0
        with atomic_binary_writer(path) as stream:
            async for chunk in chunks:
                written += len(chunk)
                if written > max_bytes:
                    raise IngestUploadTooLarge(f"CSV upload exceeds {max_bytes} byte limit")
                stream.write(chunk)
        return written

    def remove_job(self, job_id: str) -> None:
        path = self.path_for(job_id)
        if path.parent.is_dir():
            import shutil

            shutil.rmtree(path.parent)


@dataclass(frozen=True)
class BulkIngestResult:
    job_id: str
    records_processed: int
    records_failed: int


@dataclass(frozen=True)
class _RowOutcome:
    csv_row_number: int
    status: str
    record_id: str | None
    error_message: str | None
    source_values: dict[str, str | None]
    values: tuple[Any, ...] | None = None


class BulkIngestWorker:
    """Applies bounded CSV batches to a configured mutable DuckDB object."""

    def __init__(
        self,
        settings,
        state_store: StateStore,
        engine: DuckDBEngine,
        uploads: IngestUploadStore,
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.engine = engine
        self.uploads = uploads

    def run(self, job_id: str) -> BulkIngestResult:
        job = self.state_store.get_job(job_id)
        if job is None or job.job_type != BulkJobType.INGEST or job.object_name is None:
            raise KeyError(job_id)
        if job.state == BulkJobState.UPLOAD_COMPLETE:
            self.state_store.transition_job(job_id, BulkJobState.IN_PROGRESS)
        elif job.state != BulkJobState.IN_PROGRESS:
            raise ValueError(f"job {job_id} is not ready to run")
        try:
            result = self._stream(job_id, job.object_name)
        except IngestFileError as error:
            self.state_store.transition_job(job_id, BulkJobState.FAILED, "INVALID_CSV", str(error))
            raise
        except BaseException:
            self.state_store.transition_job(
                job_id, BulkJobState.FAILED, "BULK_PROCESSING_ERROR", "Bulk ingest processing failed"
            )
            raise
        self.state_store.transition_job(job_id, BulkJobState.JOB_COMPLETE)
        return result

    def _stream(self, job_id: str, object_name: str) -> BulkIngestResult:
        spec = self.engine.catalog.get(object_name)
        if spec is None or spec.mode != "mutable":
            raise IngestFileError(f"{object_name} is not configured for ingest")
        path = self.uploads.path_for(job_id)
        if not path.is_file():
            raise IngestFileError("CSV upload is missing")
        progress = self.state_store.ingest_progress(job_id)
        processed, failed = progress.records_processed, progress.records_failed
        batch: list[tuple[int, dict[str, str | None]]] = []
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            self._validate_header(reader.fieldnames, spec.schema.names)
            for row_number, row in enumerate(reader, start=1):
                if row_number <= progress.csv_row_number:
                    continue
                if None in row:
                    raise IngestFileError(f"row {row_number} has more values than the CSV header")
                batch.append((row_number, row))
                if len(batch) >= self.settings.bulk_ingest_batch_records:
                    processed, failed = self._apply_batch(
                        job_id, spec, batch, processed, failed, path.stat().st_size
                    )
                    batch = []
            if batch:
                processed, failed = self._apply_batch(
                    job_id, spec, batch, processed, failed, path.stat().st_size
                )
        return BulkIngestResult(job_id, processed, failed)

    @staticmethod
    def _validate_header(fieldnames: list[str] | None, schema_names: list[str]) -> None:
        if not fieldnames or any(not name for name in fieldnames):
            raise IngestFileError("CSV upload must have a valid header")
        unknown = set(fieldnames) - set(schema_names)
        if unknown:
            raise IngestFileError(f"CSV header has unknown fields: {', '.join(sorted(unknown))}")

    def _apply_batch(
        self,
        job_id: str,
        spec,
        batch: list[tuple[int, dict[str, str | None]]],
        processed: int,
        failed: int,
        input_bytes: int,
    ) -> tuple[int, int]:
        outcomes = [self._prepare_row(job_id, number, row, spec) for number, row in batch]
        valid = [outcome for outcome in outcomes if outcome.values is not None]
        duplicate_ids: set[str] = set()
        seen_ids: set[str] = set()
        for outcome in valid:
            assert outcome.record_id is not None
            if outcome.record_id in seen_ids:
                duplicate_ids.add(outcome.record_id)
            seen_ids.add(outcome.record_id)
        if valid:
            table_name = _quote_identifier(f"ff_mutable_{spec.object_name}")
            id_field = _quote_identifier(spec.id_field)
            ids = [outcome.record_id for outcome in valid if outcome.record_id not in duplicate_ids]
            with self.engine.connection() as conn:
                existing = {
                    row[0]
                    for row in conn.execute(
                        f"SELECT {id_field} FROM {table_name} WHERE {id_field} IN ({', '.join('?' for _ in ids)})",
                        ids,
                    ).fetchall()
                } if ids else set()
                outcomes = [
                    self._as_duplicate(outcome) if outcome.record_id in duplicate_ids | existing else outcome
                    for outcome in outcomes
                ]
                self._commit_batch(conn, job_id, spec, outcomes, processed, failed, input_bytes)
        else:
            with self.engine.connection() as conn:
                self._commit_batch(conn, job_id, spec, outcomes, processed, failed, input_bytes)
        succeeded = sum(outcome.status == "success" for outcome in outcomes)
        return processed + succeeded, failed + len(outcomes) - succeeded

    def _prepare_row(self, job_id: str, row_number: int, row: dict[str, str | None], spec) -> _RowOutcome:
        try:
            record_id = row.get(spec.id_field) or self._generated_id(job_id, row_number)
            values = tuple(
                record_id if field.name == spec.id_field else self._coerce(field, row.get(field.name))
                for field in spec.schema
            )
            return _RowOutcome(row_number, "success", record_id, None, row, values)
        except ValueError as error:
            return _RowOutcome(row_number, "failed", None, str(error), row)

    @staticmethod
    def _generated_id(job_id: str, row_number: int) -> str:
        return "001" + hashlib.sha1(f"{job_id}:{row_number}".encode()).hexdigest()[:15]

    @staticmethod
    def _coerce(field: pa.Field, value: str | None) -> Any:
        if value in (None, ""):
            if field.name == "IsDeleted":
                return False
            return None
        try:
            if pa.types.is_boolean(field.type):
                if value.lower() in {"true", "1"}:
                    return True
                if value.lower() in {"false", "0"}:
                    return False
                raise ValueError("must be a boolean")
            if pa.types.is_integer(field.type):
                return int(value)
            if pa.types.is_floating(field.type):
                return float(value)
            if pa.types.is_date(field.type):
                return date.fromisoformat(value)
            if pa.types.is_timestamp(field.type):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return value
        except ValueError as error:
            raise ValueError(f"{field.name}: {error}") from error

    @staticmethod
    def _as_duplicate(outcome: _RowOutcome) -> _RowOutcome:
        return _RowOutcome(
            outcome.csv_row_number, "failed", outcome.record_id,
            "DUPLICATE_VALUE: Id already exists", outcome.source_values,
        )

    @staticmethod
    def _commit_batch(
        conn,
        job_id: str,
        spec,
        outcomes: list[_RowOutcome],
        processed: int,
        failed: int,
        input_bytes: int,
    ) -> None:
        table_name = _quote_identifier(f"ff_mutable_{spec.object_name}")
        columns = ", ".join(_quote_identifier(field.name) for field in spec.schema)
        placeholders = ", ".join("?" for _ in spec.schema)
        succeeded = sum(outcome.status == "success" for outcome in outcomes)
        conn.execute("BEGIN")
        try:
            for outcome in outcomes:
                if outcome.values is not None:
                    conn.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", outcome.values)
                conn.execute(
                    """
                    INSERT INTO job_row_results (
                        job_id, csv_row_number, status, record_id, error_message, source_values_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        job_id, outcome.csv_row_number, outcome.status, outcome.record_id,
                        outcome.error_message, json.dumps(outcome.source_values, sort_keys=True),
                    ],
                )
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
                    job_id, checkpoint_number, processed + succeeded,
                    failed + len(outcomes) - succeeded, input_bytes,
                    outcomes[-1].csv_row_number, None,
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
