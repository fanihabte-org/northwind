"""Idempotent Parquet delta publication for planned CRM events."""

from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from simulator.events import SourceEvent


class CrmDeltaError(RuntimeError):
    pass


class CrmDeltaPublisher:
    """Writes complete changed records, never a full daily CRM snapshot."""

    def __init__(self, root: Path, base_sources: Mapping[str, Path]) -> None:
        self.root = root
        self.base_sources = dict(base_sources)
        if set(self.base_sources) != {"accounts", "opportunities"}:
            raise ValueError("base sources must contain accounts and opportunities")

    def publish(self, run_date: date, events: Iterable[SourceEvent]) -> dict[str, Path]:
        grouped: dict[str, list[SourceEvent]] = defaultdict(list)
        for event in events:
            if event.source_system != "crm":
                raise ValueError("CRM publisher accepts only CRM events")
            grouped[self._object_for(event)].append(event)
        manifest_path = self.root / "manifests" / f"{run_date.isoformat()}.json"
        desired = {name: sorted(event.event_id for event in values) for name, values in grouped.items()}
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text())
            if manifest.get("events") != desired:
                raise CrmDeltaError("a different CRM event set was already published for this date")
            return {name: Path(path) for name, path in manifest["parts"].items()}

        parts: dict[str, Path] = {}
        for object_name, object_events in grouped.items():
            rows = self._materialize(object_name, object_events)
            target = self.root / object_name / f"business_date={run_date.isoformat()}" / "delta.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            self._write_parquet(target, rows, pq.ParquetFile(self.base_sources[object_name]).schema_arrow)
            parts[object_name] = target
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_json(manifest_path, {"events": desired, "parts": {key: str(value) for key, value in parts.items()}})
        return parts

    def _materialize(self, object_name: str, events: list[SourceEvent]) -> list[dict[str, object]]:
        ids = [event.entity_id for event in events]
        if len(ids) != len(set(ids)):
            raise CrmDeltaError("only one CRM event per record and date may be published")
        existing = self._current_rows(object_name, ids)
        rows: list[dict[str, object]] = []
        for event in events:
            is_create = event.event_type.endswith("_created")
            previous = existing.get(event.entity_id)
            if is_create and previous is not None:
                raise CrmDeltaError(f"CRM record already exists: {event.entity_id}")
            if not is_create and previous is None:
                raise CrmDeltaError(f"CRM event refers to an unknown record: {event.entity_id}")
            row = dict(previous or {})
            row.update(event.payload)
            rows.append(row)
        return rows

    def _current_rows(self, object_name: str, ids: list[str]) -> dict[str, dict[str, object]]:
        paths = [self.base_sources[object_name], *sorted((self.root / object_name).glob("**/*.parquet"))]
        paths = [path for path in paths if path.is_file()]
        literals = ", ".join(self._quote(path) for path in paths)
        placeholders = ", ".join("?" for _ in ids)
        schema_names = set(pq.ParquetFile(self.base_sources[object_name]).schema_arrow.names)
        version_field = "SystemModstamp" if "SystemModstamp" in schema_names else "LastModifiedDate"
        query = f"""
            SELECT * EXCLUDE (row_rank) FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY "Id" ORDER BY "{version_field}" DESC
                ) AS row_rank
                FROM read_parquet([{literals}])
                WHERE "Id" IN ({placeholders})
            ) WHERE row_rank = 1
        """
        with duckdb.connect() as conn:
            cursor = conn.execute(query, ids)
            columns = [column[0] for column in cursor.description]
            return {str(row[0]): dict(zip(columns, row, strict=True)) for row in cursor.fetchall()}

    @staticmethod
    def _object_for(event: SourceEvent) -> str:
        if event.event_type.startswith("account_"):
            return "accounts"
        if event.event_type.startswith("opportunity_"):
            return "opportunities"
        raise CrmDeltaError(f"unsupported CRM event type: {event.event_type}")

    @staticmethod
    def _quote(path: Path) -> str:
        return "'" + str(path).replace("'", "''") + "'"

    @staticmethod
    def _write_parquet(target: Path, rows: list[dict[str, object]], schema: pa.Schema) -> None:
        fd, temporary_name = tempfile.mkstemp(prefix=f".{target.stem}.", suffix=".tmp", dir=target.parent)
        os.close(fd)
        temporary = Path(temporary_name)
        try:
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary, compression="zstd")
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    @staticmethod
    def _atomic_json(target: Path, payload: dict[str, object]) -> None:
        fd, temporary_name = tempfile.mkstemp(prefix=f".{target.stem}.", suffix=".tmp", dir=target.parent)
        os.close(fd)
        temporary = Path(temporary_name)
        try:
            temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
