"""Configuration-driven source catalog for files exposed as sObjects."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.parquet as pq


class CatalogError(ValueError):
    """A configured dataset cannot safely be exposed by FakeForce."""


_OBJECT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_SUPPORTED_SUFFIXES = (".parquet", ".csv", ".csv.gz")
_DECLARED_FIELD_TYPES = {
    "string": pa.string(),
    "boolean": pa.bool_(),
    "integer": pa.int64(),
    "number": pa.float64(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}


_MISSING = object()
COMPACTED_DIRECTORY = "compacted"
COMPACTED_BASE_FILENAME = "base.parquet"
_WILDCARDS = ("*", "?", "[")


def _file_signature(path: Path) -> tuple[int, int] | None:
    """Size and modification time, or None when the file is absent."""
    try:
        status = path.stat()
    except OSError:
        return None
    return status.st_size, status.st_mtime_ns


def _directory_signature(directory: Path) -> tuple[Any, ...]:
    """Signature of a delta directory's immediate entries.

    One ``scandir`` replaces a recursive glob.  Entry modification times are
    included so that a partition rewritten in place is still noticed, not only
    a newly created one.
    """
    try:
        with os.scandir(directory) as entries:
            children = sorted(
                (entry.name, _entry_modified_ns(entry)) for entry in entries
            )
    except OSError:
        return ()
    return tuple(children)


def _entry_modified_ns(entry: os.DirEntry) -> int:
    try:
        return entry.stat(follow_symlinks=False).st_mtime_ns
    except OSError:
        return 0


class _ResolutionCache:
    """Thread-safe memo for a value derived from the filesystem.

    FakeForce resolves sources and recomputes its snapshot identifier on every
    request, but both answers change only when the simulator publishes a
    partition or the seed is regenerated.  Each is therefore memoized behind a
    cheap fingerprint, and recomputed only when that fingerprint moves.
    """

    __slots__ = ("_lock", "_fingerprint", "_value")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fingerprint: Any = _MISSING
        self._value: Any = _MISSING

    def get(self, fingerprint: Any, compute: Callable[[], Any]) -> Any:
        with self._lock:
            if self._value is _MISSING or self._fingerprint != fingerprint:
                self._value = compute()
                self._fingerprint = fingerprint
            return self._value

    def peek(self) -> Any:
        """The last computed value, or None when nothing is cached yet."""
        with self._lock:
            return None if self._value is _MISSING else self._value

    def invalidate(self) -> None:
        with self._lock:
            self._fingerprint = _MISSING
            self._value = _MISSING


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
    computed_table: pa.Table | None = None
    supports_limit: bool = True
    supports_ne: bool = True
    required_filter_fields: tuple[str, ...] = ()
    _cache: _ResolutionCache = field(
        default_factory=_ResolutionCache, compare=False, repr=False
    )

    def current_sources(self) -> tuple[Path, ...]:
        """Base files plus optional Parquet deltas published after startup.

        The glob behind this is recursive and runs on every connection, so the
        resolved set is memoized and re-derived only when the structural
        fingerprint shows a file was added or removed.  Rewriting a file in
        place cannot change which files exist, so it does not belong here --
        ``content_fingerprint`` covers that for snapshot identity.
        """
        return self._cache.get(self.structural_fingerprint(), self._resolve_sources)

    def refresh(self) -> None:
        """Force the next resolution to re-read the filesystem."""
        self._cache.invalidate()

    def _resolve_sources(self) -> tuple[Path, ...]:
        return tuple(sorted(set((*self.effective_base(), *self.published_deltas()))))

    def compacted_base(self) -> Path | None:
        """A merged file that stands in for the configured base, if one exists.

        The seed is the generator's deterministic output and is mounted
        read-only where FakeForce runs, so compaction cannot rewrite it. It
        writes the merged object here instead, and this file then supersedes
        the configured base entirely -- every row it would have contributed is
        already in here.
        """
        for root in self.delta_roots():
            candidate = root / COMPACTED_DIRECTORY / COMPACTED_BASE_FILENAME
            if candidate.is_file():
                return candidate
        return None

    def effective_base(self) -> tuple[Path, ...]:
        """The files that hold one row per id before any delta is applied."""
        compacted = self.compacted_base()
        return (compacted,) if compacted is not None else self.sources

    def published_deltas(self) -> tuple[Path, ...]:
        """Partitions published since the last compaction."""
        deltas: list[Path] = []
        for pattern in self.delta_patterns:
            deltas.extend(DatasetCatalog._expand_source(pattern, self.data_roots))
        return tuple(
            sorted(
                path
                for path in set(deltas)
                if path.parent.name != COMPACTED_DIRECTORY
            )
        )

    def delta_roots(self) -> tuple[Path, ...]:
        """Directories a delta pattern can publish into, without walking them."""
        roots: list[Path] = []
        for pattern in self.delta_patterns:
            prefix: list[str] = []
            for segment in PurePosixPath(pattern).parts:
                if any(wildcard in segment for wildcard in _WILDCARDS):
                    break
                prefix.append(segment)
            for root in self.data_roots:
                roots.append(root.joinpath(*prefix) if prefix else root)
        return tuple(sorted(set(roots)))

    def structural_fingerprint(self) -> tuple[Any, ...]:
        """A signature that moves whenever the *set* of source files can differ.

        Base files are signed directly, so a regenerated seed is noticed.  Each
        delta root is signed by its immediate entries and their modification
        times, so a partition published, removed, or written into is noticed --
        the simulator publishes through ``os.replace`` into the partition
        directory, which updates that directory.

        Nothing here reads the cache it guards, so the first call is already
        stable.
        """
        entries: list[tuple[Any, ...]] = [
            ("base", str(path), _file_signature(path)) for path in self.sources
        ]
        entries.extend(
            ("delta", str(directory), _directory_signature(directory))
            for directory in self.delta_roots()
        )
        return tuple(entries)

    def content_fingerprint(self) -> tuple[Any, ...]:
        """The structural signature plus every resolved file's own signature.

        Cursor validity is pinned to snapshot identity, so a file rewritten in
        place must invalidate it even though the set of files is unchanged.
        This is the exact guarantee the uncached implementation gave.
        """
        return (
            self.structural_fingerprint(),
            tuple((str(path), _file_signature(path)) for path in self.current_sources()),
        )


class DatasetCatalog:
    """A validated, metadata-only view of configured disk datasets."""

    def __init__(self, objects: Iterable[DatasetSpec]) -> None:
        self._snapshot_cache = _ResolutionCache()
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
        objects.extend(_metadata_catalog_specs(tuple(objects)))
        return cls(objects)

    @staticmethod
    def _declared_schema(value: Any, object_name: str) -> pa.Schema | None:
        """Read an optional typed fallback for an unavailable base artifact."""
        if value is None:
            return None
        if not isinstance(value, dict) or not value:
            raise CatalogError(f"{object_name}: schema must be a non-empty field/type mapping")
        fields: list[pa.Field] = []
        for field_name, type_name in value.items():
            if not isinstance(field_name, str) or not field_name:
                raise CatalogError(f"{object_name}: schema field names must be non-empty strings")
            if not isinstance(type_name, str) or type_name not in _DECLARED_FIELD_TYPES:
                supported = ", ".join(sorted(_DECLARED_FIELD_TYPES))
                raise CatalogError(
                    f"{object_name}: unsupported schema type {type_name!r}; use one of {supported}"
                )
            fields.append(pa.field(field_name, _DECLARED_FIELD_TYPES[type_name]))
        return pa.schema(fields)

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
        declared_schema = DatasetCatalog._declared_schema(entry.get("schema"), name)
        source_paths: list[Path] = []
        for configured_source in configured_sources:
            if not isinstance(configured_source, str):
                raise CatalogError(f"{name}: source paths must be strings")
            matches = DatasetCatalog._expand_source(configured_source, roots)
            if not matches and declared_schema is None:
                raise CatalogError(f"{name}: source does not match a file: {configured_source}")
            source_paths.extend(matches)
        if not source_paths and declared_schema is None:
            raise CatalogError(f"{name}: source does not match a file: {configured_sources[0]}")

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
        if configured_deltas and source_paths and not all(
            path.name.endswith(".parquet") for path in source_paths
        ):
            raise CatalogError(f"{name}: Parquet delta patterns require Parquet base sources")

        source_schema = (
            DatasetCatalog._read_schema(source_paths[0]) if source_paths else declared_schema
        )
        assert source_schema is not None
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

    def refresh(self) -> None:
        """Force every cached source resolution to re-read the filesystem."""
        self._snapshot_cache.invalidate()
        for spec in self._objects.values():
            spec.refresh()

    @property
    def snapshot_id(self) -> str:
        """Stable identifier for the exact files and schemas in this catalog.

        Cursor validity is pinned to this value, so it is computed from a full
        per-file signature.  That work is memoized behind the same fingerprint
        the source resolution uses, because this property is read on every
        cursor creation and every page of every extract.
        """
        fingerprint = tuple(
            self._objects[name].content_fingerprint() for name in sorted(self._objects)
        )
        return self._snapshot_cache.get(fingerprint, self._compute_snapshot_id)

    def _compute_snapshot_id(self) -> str:
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


# --------------------------------------------------------------------------
# Metadata Catalog: EntityDefinition and FieldDefinition
#
# These are "virtual" objects -- their rows are computed once from the rest
# of the catalog, not read from a file. Salesforce's real Tooling API
# restricts them in ways ordinary sObjects are not (see query_service.py's
# use of supports_ne/required_filter_fields/supports_limit); that
# restriction set is intentionally partial. COUNT(), GROUP BY, OR, NOT, and
# INCLUDES are not implemented here because they are not implemented for any
# object -- soql.py's grammar has no representation for them at all, so they
# already fail to parse regardless of which sObject is queried. Teaching this
# module to parse them just to then reject them for these two objects would
# be a second, unscoped feature, not a restriction on an existing one.
# --------------------------------------------------------------------------

_ENTITY_DEFINITION_SCHEMA = pa.schema([
    pa.field("DurableId", pa.string()),
    pa.field("QualifiedApiName", pa.string()),
    pa.field("DeveloperName", pa.string()),
    pa.field("Label", pa.string()),
    pa.field("PluralLabel", pa.string()),
    pa.field("MasterLabel", pa.string()),
    pa.field("KeyPrefix", pa.string()),
    pa.field("IsQueryable", pa.bool_()),
    pa.field("IsRetrieveable", pa.bool_()),
    pa.field("IsIdEnabled", pa.bool_()),
    pa.field("IsEverCreatable", pa.bool_()),
    pa.field("IsEverUpdatable", pa.bool_()),
    pa.field("IsEverDeletable", pa.bool_()),
    pa.field("NamespacePrefix", pa.string()),
    pa.field("LastModifiedDate", pa.timestamp("us", tz="UTC")),
])

_FIELD_DEFINITION_SCHEMA = pa.schema([
    pa.field("DurableId", pa.string()),
    pa.field("EntityDefinitionId", pa.string()),
    pa.field("EntityDefinition.QualifiedApiName", pa.string()),
    pa.field("QualifiedApiName", pa.string()),
    pa.field("Label", pa.string()),
    pa.field("DataType", pa.string()),
    pa.field("ValueTypeId", pa.string()),
    pa.field("Length", pa.int64()),
    pa.field("Precision", pa.int64()),
    pa.field("Scale", pa.int64()),
    pa.field("IsNillable", pa.bool_()),
    pa.field("IsNameField", pa.bool_()),
    pa.field("IsIndexed", pa.bool_()),
    pa.field("IsApiFilterable", pa.bool_()),
    pa.field("IsApiSortable", pa.bool_()),
    pa.field("IsApiGroupable", pa.bool_()),
    pa.field("ReferenceTo", pa.string()),
    pa.field("RelationshipName", pa.string()),
])


def _first_id_in_file(path: Path, id_field: str, length: int) -> str | None:
    """The first Id in one source file, or None if it has no rows."""
    if path.name.endswith(".parquet"):
        column = pq.ParquetFile(path).read_row_group(0, columns=[id_field]).column(0)
        if len(column) == 0 or column[0].as_py() is None:
            return None
        return str(column[0].as_py())[:length]
    opener = gzip.open if path.name.endswith(".csv.gz") else open
    with opener(path, "rt", newline="", encoding="utf-8") as stream:
        first_row = next(csv.DictReader(stream), None)
    return str(first_row[id_field])[:length] if first_row else None


def _sample_id_prefix(spec: DatasetSpec, length: int = 3) -> str:
    """First few characters of a real Id, mirroring Salesforce's KeyPrefix.

    Reads only the configured base -- not ``current_sources()``, which also
    globs delta partitions. That glob is lazy and memoized on first real use
    (see test_catalog_resolution_cache.py); running it here, at catalog load,
    would defeat the memoization test's premise that the *first* call after
    construction is the one that walks the filesystem. A base with no rows of
    its own (e.g. OpportunityHistory, where every row arrives as a delta)
    falls back to a placeholder rather than chasing into deltas for it.
    """
    for path in spec.sources:
        prefix = _first_id_in_file(path, spec.id_field, length)
        if prefix is not None:
            return prefix
    return "000"


def _field_type_info(
    pa_field: pa.Field, spec: DatasetSpec, object_names: frozenset[str]
) -> tuple[str, str, int | None, int | None, int | None, str | None]:
    """(DataType, ValueTypeId, Length, Precision, Scale, ReferenceTo).

    DataType is the UI display string Salesforce shows, not the API type
    name. ReferenceTo is guessed from naming convention (an "XyzId" field
    where "Xyz" is a configured object) since the catalog carries no formal
    relationship metadata -- there is nothing else to derive it from.
    """
    name = pa_field.name
    if name == spec.id_field:
        return "Id", "xsd:string", 18, None, None, None
    if name.endswith("Id"):
        candidate = name[:-2]
        if candidate in object_names:
            return f"Lookup({candidate})", "xsd:string", 18, None, None, candidate
    field_type = pa_field.type
    if pa.types.is_boolean(field_type):
        return "Checkbox", "xsd:boolean", None, None, None, None
    if pa.types.is_integer(field_type):
        return "Number(18, 0)", "xsd:int", None, 18, 0, None
    if pa.types.is_floating(field_type):
        return "Number(18, 2)", "xsd:double", None, 18, 2, None
    if pa.types.is_date(field_type):
        return "Date", "xsd:date", None, None, None, None
    if pa.types.is_timestamp(field_type):
        return "Date/Time", "xsd:dateTime", None, None, None, None
    return "Text(255)", "xsd:string", 255, None, None, None


def _entity_definition_spec(
    objects: tuple[DatasetSpec, ...], key_prefixes: dict[str, str]
) -> DatasetSpec:
    rows = []
    for spec in objects:
        developer_name = (
            spec.object_name[:-3] if spec.object_name.endswith("__c") else spec.object_name
        )
        modified = (
            datetime.fromtimestamp(spec.sources[0].stat().st_mtime, tz=timezone.utc)
            if spec.sources else datetime.fromtimestamp(0, tz=timezone.utc)
        )
        key_prefix = key_prefixes[spec.object_name]
        rows.append({
            "DurableId": key_prefix,
            "QualifiedApiName": spec.object_name,
            "DeveloperName": developer_name,
            "Label": spec.object_name,
            "PluralLabel": f"{spec.object_name}s",
            "MasterLabel": spec.object_name,
            "KeyPrefix": key_prefix,
            "IsQueryable": True,
            "IsRetrieveable": True,
            "IsIdEnabled": True,
            "IsEverCreatable": spec.mode == "mutable",
            "IsEverUpdatable": spec.mode == "mutable",
            "IsEverDeletable": spec.mode == "mutable",
            "NamespacePrefix": None,
            "LastModifiedDate": modified,
        })
    table = pa.Table.from_pylist(rows, schema=_ENTITY_DEFINITION_SCHEMA)
    return DatasetSpec(
        object_name="EntityDefinition",
        sources=(),
        id_field="DurableId",
        soft_delete_field=None,
        mode="computed",
        schema=table.schema,
        data_roots=(),
        computed_table=table,
        supports_limit=False,
        supports_ne=False,
    )


def _field_definition_spec(
    objects: tuple[DatasetSpec, ...], key_prefixes: dict[str, str]
) -> DatasetSpec:
    object_names = frozenset(spec.object_name for spec in objects)
    rows = []
    for spec in objects:
        entity_id = key_prefixes[spec.object_name]
        for pa_field in spec.schema:
            name = pa_field.name
            data_type, value_type_id, length, precision, scale, reference_to = _field_type_info(
                pa_field, spec, object_names
            )
            rows.append({
                "DurableId": f"{entity_id}.{name}",
                "EntityDefinitionId": entity_id,
                "EntityDefinition.QualifiedApiName": spec.object_name,
                "QualifiedApiName": name,
                "Label": name,
                "DataType": data_type,
                "ValueTypeId": value_type_id,
                "Length": length,
                "Precision": precision,
                "Scale": scale,
                "IsNillable": name != spec.id_field,
                "IsNameField": name == "Name",
                "IsIndexed": name in (spec.id_field, spec.soft_delete_field, spec.version_field),
                "IsApiFilterable": True,
                "IsApiSortable": True,
                "IsApiGroupable": True,
                "ReferenceTo": reference_to,
                "RelationshipName": reference_to,
            })
    table = pa.Table.from_pylist(rows, schema=_FIELD_DEFINITION_SCHEMA)
    return DatasetSpec(
        object_name="FieldDefinition",
        sources=(),
        id_field="DurableId",
        soft_delete_field=None,
        mode="computed",
        schema=table.schema,
        data_roots=(),
        computed_table=table,
        supports_limit=False,
        supports_ne=False,
        required_filter_fields=("EntityDefinition.QualifiedApiName", "EntityDefinitionId"),
    )


def _metadata_catalog_specs(objects: tuple[DatasetSpec, ...]) -> tuple[DatasetSpec, ...]:
    key_prefixes = {spec.object_name: _sample_id_prefix(spec) for spec in objects}
    return (
        _entity_definition_spec(objects, key_prefixes),
        _field_definition_spec(objects, key_prefixes),
    )
