"""Configuration-driven source catalog for files exposed as sObjects."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq


class CatalogError(ValueError):
    """A configured dataset cannot safely be exposed by FakeForce."""


_OBJECT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_SUPPORTED_SUFFIXES = (".parquet", ".csv", ".csv.gz")


@dataclass(frozen=True)
class DatasetSpec:
    object_name: str
    sources: tuple[Path, ...]
    id_field: str
    soft_delete_field: str | None
    mode: str
    schema: pa.Schema


class DatasetCatalog:
    """A validated, metadata-only view of configured disk datasets."""

    def __init__(self, objects: Iterable[DatasetSpec]) -> None:
        self._objects = {obj.object_name: obj for obj in objects}
        if not self._objects:
            raise CatalogError("catalog must expose at least one object")

    @classmethod
    def from_file(cls, path: Path, allowed_roots: Iterable[Path]) -> "DatasetCatalog":
        try:
            raw = json.loads(path.read_text())
        except FileNotFoundError as exc:
            raise CatalogError(f"catalog file does not exist: {path}") from exc
        except json.JSONDecodeError as exc:
            raise CatalogError(f"catalog JSON is invalid: {exc.msg}") from exc

        if raw.get("version") != 1:
            raise CatalogError("catalog version must be 1")
        entries = raw.get("objects")
        if not isinstance(entries, list):
            raise CatalogError("catalog objects must be a list")

        roots = tuple(root.resolve() for root in allowed_roots)
        if not roots:
            raise CatalogError("at least one allowed data root is required")
        objects = [cls._parse_object(entry, roots) for entry in entries]
        names = [obj.object_name for obj in objects]
        if len(names) != len(set(names)):
            raise CatalogError("catalog object names must be unique")
        return cls(objects)

    @staticmethod
    def _parse_object(entry: Any, roots: tuple[Path, ...]) -> DatasetSpec:
        if not isinstance(entry, dict):
            raise CatalogError("catalog object entries must be objects")
        name = entry.get("name")
        if not isinstance(name, str) or not _OBJECT_NAME.fullmatch(name):
            raise CatalogError(f"invalid object name: {name!r}")

        configured_sources = entry.get("sources")
        if not isinstance(configured_sources, list) or not configured_sources:
            raise CatalogError(f"{name}: sources must be a non-empty list")
        source_paths: list[Path] = []
        for configured_source in configured_sources:
            if not isinstance(configured_source, str):
                raise CatalogError(f"{name}: source paths must be strings")
            matches = DatasetCatalog._expand_source(configured_source, roots)
            if not matches:
                raise CatalogError(f"{name}: source does not match a file: {configured_source}")
            source_paths.extend(matches)

        id_field = entry.get("id_field", "Id")
        soft_delete_field = entry.get("soft_delete_field", "IsDeleted")
        mode = entry.get("mode", "read_only")
        if not isinstance(id_field, str) or not id_field:
            raise CatalogError(f"{name}: id_field must be a non-empty string")
        if soft_delete_field is not None and not isinstance(soft_delete_field, str):
            raise CatalogError(f"{name}: soft_delete_field must be a string or null")
        if mode not in {"read_only", "mutable"}:
            raise CatalogError(f"{name}: mode must be read_only or mutable")

        schema = DatasetCatalog._read_schema(source_paths[0])
        names = set(schema.names)
        if id_field not in names:
            raise CatalogError(f"{name}: id field {id_field!r} is absent from source schema")
        if soft_delete_field is not None and soft_delete_field not in names:
            raise CatalogError(
                f"{name}: soft-delete field {soft_delete_field!r} is absent from source schema"
            )
        return DatasetSpec(name, tuple(source_paths), id_field, soft_delete_field, mode, schema)

    @staticmethod
    def _expand_source(pattern: str, roots: tuple[Path, ...]) -> list[Path]:
        matches: list[Path] = []
        for root in roots:
            for candidate in root.glob(pattern):
                resolved = candidate.resolve()
                if candidate.is_file() and any(
                    resolved.is_relative_to(allowed_root) for allowed_root in roots
                ):
                    if not candidate.name.endswith(_SUPPORTED_SUFFIXES):
                        raise CatalogError(f"unsupported source format: {candidate}")
                    matches.append(resolved)
        return sorted(set(matches))

    @staticmethod
    def _read_schema(path: Path) -> pa.Schema:
        if path.name.endswith(".parquet"):
            return pq.ParquetFile(path).schema_arrow
        opener = gzip.open if path.name.endswith(".csv.gz") else open
        with opener(path, "rt", newline="", encoding="utf-8") as stream:
            header = next(csv.reader(stream), None)
        if not header or any(not name for name in header):
            raise CatalogError(f"CSV source has no valid header: {path}")
        return pa.schema([pa.field(name, pa.string()) for name in header])

    @property
    def object_names(self) -> tuple[str, ...]:
        return tuple(self._objects)

    def get(self, object_name: str) -> DatasetSpec | None:
        return self._objects.get(object_name)

    @property
    def snapshot_id(self) -> str:
        """Stable identifier for the exact files and schemas in this catalog."""
        payload = []
        for object_name in sorted(self._objects):
            spec = self._objects[object_name]
            payload.append(
                {
                    "name": spec.object_name,
                    "id_field": spec.id_field,
                    "soft_delete_field": spec.soft_delete_field,
                    "mode": spec.mode,
                    "schema": str(spec.schema),
                    "sources": [
                        {
                            "path": str(path),
                            "size": path.stat().st_size,
                            "modified_ns": path.stat().st_mtime_ns,
                        }
                        for path in spec.sources
                    ],
                }
            )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
