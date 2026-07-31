"""Bounded, disk-producing Bulk API 2.0 Query worker."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fakeforce.bulk.jobs import BulkJobState, BulkJobType
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import _INTERNAL_ID_FIELD, LazyQueryService
from fakeforce.state import StateStore
from fakeforce.storage import atomic_binary_writer, require_disk_reserve


@dataclass(frozen=True)
class BulkQueryResult:
    job_id: str
    records_processed: int
    parts_written: int


class BulkQueryWorker:
    def __init__(
        self,
        settings: Settings,
        state_store: StateStore,
        engine: DuckDBEngine,
        query_service: LazyQueryService,
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.engine = engine
        self.query_service = query_service

    def run(self, job_id: str) -> BulkQueryResult:
        job = self.state_store.get_job(job_id)
        if job is None or job.job_type != BulkJobType.QUERY or job.query_text is None:
            raise KeyError(job_id)
        if job.state == BulkJobState.UPLOAD_COMPLETE:
            self.state_store.transition_job(job_id, BulkJobState.IN_PROGRESS)
        elif job.state != BulkJobState.IN_PROGRESS:
            raise ValueError(f"job {job_id} is not ready to run")

        try:
            result = self._stream(job_id, job.query_text)
        except BaseException as error:
            self.state_store.transition_job(
                job_id, BulkJobState.FAILED, "BULK_PROCESSING_ERROR", "Bulk query processing failed"
            )
            raise error
        self.state_store.transition_job(job_id, BulkJobState.JOB_COMPLETE)
        return result

    def _stream(self, job_id: str, query: str) -> BulkQueryResult:
        plan = self.query_service.plan(query, include_deleted=False)
        output_directory = self.settings.bulk_query_results_directory / job_id
        output_directory.mkdir(parents=True, exist_ok=True)
        manifest_parts = self._load_manifest(output_directory)
        self._discard_uncommitted_parts(output_directory, manifest_parts)
        manifest_records = sum(int(part["record_count"]) for part in manifest_parts)
        checkpoint_records = self.state_store.latest_job_checkpoint_records(job_id)
        if checkpoint_records > manifest_records:
            raise RuntimeError("bulk query checkpoint has no committed result part")
        # A manifest is published before its checkpoint.  It is therefore the
        # source of truth after a process crash in that small window.
        resume_records = manifest_records
        records_processed = resume_records
        part_number = max((int(part["part"]) for part in manifest_parts), default=-1) + 1
        written_parts = 0

        with self.engine.connection() as connection:
            cursor = connection.execute(plan.sql, plan.parameters)
            columns = [column[0] for column in cursor.description]
            public_indexes = [
                index for index, column in enumerate(columns) if column != _INTERNAL_ID_FIELD
            ]
            public_columns = [columns[index] for index in public_indexes]
            reader = cursor.to_arrow_reader(self.settings.scan_batch_rows)
            skipped = 0
            part_rows: list[tuple[Any, ...]] = []
            for batch in reader:
                for row in zip(*[column.to_pylist() for column in batch.columns], strict=True):
                    if skipped < resume_records:
                        skipped += 1
                        continue
                    part_rows.append(tuple(row[index] for index in public_indexes))
                    if len(part_rows) >= self.settings.bulk_result_part_records:
                        part = self._write_part(
                            output_directory, part_number, public_columns, part_rows, records_processed
                        )
                        records_processed += len(part_rows)
                        manifest_parts.append(part)
                        self._commit_part(job_id, output_directory, manifest_parts, records_processed, part)
                        part_number += 1
                        written_parts += 1
                        part_rows = []
            if part_rows:
                part = self._write_part(
                    output_directory, part_number, public_columns, part_rows, records_processed
                )
                records_processed += len(part_rows)
                manifest_parts.append(part)
                self._commit_part(job_id, output_directory, manifest_parts, records_processed, part)
                written_parts += 1
        return BulkQueryResult(job_id, records_processed, written_parts)

    @staticmethod
    def _load_manifest(output_directory: Path) -> list[dict[str, Any]]:
        manifest = output_directory / "manifest.json"
        parts = json.loads(manifest.read_text()).get("parts", []) if manifest.is_file() else []
        for part in parts:
            path = output_directory / part["path"]
            if not path.is_file():
                raise RuntimeError(f"committed bulk result part is missing: {path.name}")
        return parts

    @staticmethod
    def _discard_uncommitted_parts(output_directory: Path, parts: list[dict[str, Any]]) -> None:
        """Remove files published before a manifest commit during a crash."""
        committed = {str(part["path"]) for part in parts}
        for path in output_directory.glob("part-*.csv"):
            if path.name not in committed:
                path.unlink()

    def _write_part(
        self,
        output_directory: Path,
        part_number: int,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        start_record: int,
    ) -> dict[str, Any]:
        path = output_directory / f"part-{part_number:05d}.csv"
        require_disk_reserve(output_directory, self.settings.disk_reserve_bytes)
        with atomic_binary_writer(path) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            writer = csv.writer(text)
            writer.writerow(columns)
            writer.writerows(rows)
            text.flush()
            text.detach()
        data = path.read_bytes()
        return {
            "part": part_number,
            "path": path.name,
            "start_record": start_record,
            "end_record": start_record + len(rows) - 1,
            "record_count": len(rows),
            "byte_size": len(data),
            "checksum": hashlib.sha256(data).hexdigest(),
        }

    def _commit_part(
        self,
        job_id: str,
        output_directory: Path,
        manifest_parts: list[dict[str, Any]],
        records_processed: int,
        part: dict[str, Any],
    ) -> None:
        manifest_path = output_directory / "manifest.json"
        with atomic_binary_writer(manifest_path) as stream:
            stream.write(json.dumps({"parts": manifest_parts}, sort_keys=True).encode())
        self.state_store.add_job_checkpoint(
            job_id, records_processed, part["byte_size"], str(output_directory / part["path"])
        )
