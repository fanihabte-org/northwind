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
    data_roots: tuple[Path, ...]
    delta_patterns: tuple[str, ...] = ()
    version_field: str | None = None
    compatibility_aliases: tuple[tuple[str, str], ...] = ()

    def current_sources(self) -> tuple[Path, ...]:
        """Base files plus optional Parquet deltas published after startup."""
        deltas: list[Path] = []
        for pattern in self.delta_patterns:
            deltas.extend(DatasetCatalog._expand_source(pattern, self.data_roots))
        return tuple(sorted(set((*self.sources, *deltas))))


class DatasetCatalog:
    """A validated, metadata-only view of configured disk datasets."""

    def __init__(self, objects: Iterable[DatasetSpec]) -> None:
        self._objects = {obj.object_name: obj for obj in objects}
        if not self._objects:
            raise CatalogError("catalog must expose at least one object")
        self._objects_by_casefold = {name.casefold(): spec for name, spec in self._objects.items()}
        if len(self._objects_by_casefold) != len(self._objects):
            raise CatalogError("catalog object names must be unique without regard to case")

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
        configured_deltas = entry.get("delta_patterns", [])
        version_field = entry.get("version_field")
        configured_aliases = entry.get("compatibility_aliases", {})
        if not isinstance(id_field, str) or not id_field:
            raise CatalogError(f"{name}: id_field must be a non-empty string")
        if soft_delete_field is not None and not isinstance(soft_delete_field, str):
            raise CatalogError(f"{name}: soft_delete_field must be a string or null")
        if mode not in {"read_only", "mutable"}:
            raise CatalogError(f"{name}: mode must be read_only or mutable")
        if not isinstance(configured_deltas, list) or not all(
            isinstance(pattern, str) for pattern in configured_deltas
        ):
            raise CatalogError(f"{name}: delta_patterns must be a list of strings")
        if version_field is not None and (not isinstance(version_field, str) or not version_field):
            raise CatalogError(f"{name}: version_field must be a non-empty string")
        if not isinstance(configured_aliases, dict) or not all(
            isinstance(alias, str) and alias and isinstance(source, str) and source
            for alias, source in configured_aliases.items()
        ):
            raise CatalogError(f"{name}: compatibility_aliases must map field names to source fields")
        if configured_deltas and not version_field:
            raise CatalogError(f"{name}: delta_patterns require version_field")
        if configured_deltas and not all(path.name.endswith(".parquet") for path in source_paths):
            raise CatalogError(f"{name}: Parquet delta patterns require Parquet base sources")

        source_schema = DatasetCatalog._read_schema(source_paths[0])
        names = set(source_schema.names)
        if id_field not in names:
            raise CatalogError(f"{name}: id field {id_field!r} is absent from source schema")
        if soft_delete_field is not None and soft_delete_field not in names:
            raise CatalogError(
                f"{name}: soft-delete field {soft_delete_field!r} is absent from source schema"
            )
        aliases: list[tuple[str, str]] = []
        schema = source_schema
        for alias, source in configured_aliases.items():
            if source not in names:
                raise CatalogError(
                    f"{name}: compatibility alias source {source!r} is absent from source schema"
                )
            if alias in names:
                continue
            schema = schema.append(pa.field(alias, source_schema.field(source).type))
            aliases.append((alias, source))
        if version_field is not None and version_field not in schema.names:
            raise CatalogError(f"{name}: version field {version_field!r} is absent from source schema")
        return DatasetSpec(
            name,
            tuple(source_paths),
            id_field,
            soft_delete_field,
            mode,
            schema,
            roots,
            tuple(configured_deltas),
            version_field,
            tuple(aliases),
        )

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
        """Resolve an API object name case-insensitively to its canonical spelling."""
        return self._objects_by_casefold.get(object_name.casefold())

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
                        for path in spec.current_sources()
                    ],
                }
            )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
