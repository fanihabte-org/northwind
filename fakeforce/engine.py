"""Lazy DuckDB access to configured FakeForce source datasets."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

import duckdb
import pyarrow as pa

from fakeforce.catalog import DatasetCatalog, DatasetSpec
from fakeforce.config import Settings
from fakeforce.state import StateStore
from fakeforce.storage import require_disk_reserve


_VERSION_RANK = "__fakeforce_version_rank"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


class DuckDBEngine:
    """Opens configured DuckDB sessions without loading source rows at startup."""

    def __init__(self, settings: Settings, catalog: DatasetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog
        self._mutable_database_path: Path | None = None

    @contextmanager
    def connection(
        self,
        database_path: Path | str | None = None,
        *,
        create_source_views: bool = True,
        objects: Iterable[str] | None = None,
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """Open a configured session, optionally scoped to the objects needed.

        Views are rebuilt per connection, and building one resolves that
        object's sources.  A caller that already knows which object it is about
        to read should name it, so a single-object query stops paying for every
        other object in the catalog.
        """
        self.settings.temp_directory.mkdir(parents=True, exist_ok=True)
        require_disk_reserve(
            self.settings.temp_directory, self.settings.disk_reserve_bytes
        )
        path = database_path or self._mutable_database_path or ":memory:"
        conn = duckdb.connect(str(path))
        try:
            self.configure_connection(conn)
            if create_source_views:
                self._create_read_only_views(conn, objects)
            yield conn
        finally:
            conn.close()

    def configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute("SET memory_limit = ?", [self.settings.memory_limit])
        conn.execute("SET temp_directory = ?", [str(self.settings.temp_directory)])
        conn.execute("SET max_temp_directory_size = ?", [self.settings.max_temp_size])
        conn.execute("SET threads = ?", [self.settings.heavy_query_workers])

    def relation_name(self, object_name: str) -> str:
        if self.catalog.get(object_name) is None:
            raise KeyError(f"unknown configured object: {object_name}")
        return f"ff_source_{object_name}"

    def _create_read_only_views(
        self, conn: duckdb.DuckDBPyConnection, objects: Iterable[str] | None = None
    ) -> None:
        names = self.catalog.object_names if objects is None else tuple(objects)
        for object_name in names:
            spec = self.catalog.get(object_name)
            if spec is None:
                raise KeyError(f"unknown configured object: {object_name}")
            if spec.mode == "read_only":
                self._create_parquet_or_csv_view(conn, spec)
            else:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {_quote_identifier(self.relation_name(spec.object_name))} "
                    f"AS SELECT * FROM {_quote_identifier(f'ff_mutable_{spec.object_name}')}"
                )

    def _create_parquet_or_csv_view(
        self, conn: duckdb.DuckDBPyConnection, spec: DatasetSpec
    ) -> None:
        conn.execute(
            f"CREATE OR REPLACE VIEW {_quote_identifier(self.relation_name(spec.object_name))} "
            f"AS {self._latest_version_sql(spec)}"
        )

    def _aliased_reader(self, spec: DatasetSpec, paths: tuple[Path, ...]) -> str:
        """A reader over ``paths`` carrying this object's compatibility aliases.

        An empty path set produces a typed projection built from the declared
        schema, which already lists the alias columns, so aliasing it again
        would duplicate them.
        """
        reader = self._source_reader(paths, spec.schema)
        if paths and spec.compatibility_aliases:
            aliases = ", ".join(
                f"{_quote_identifier(source)} AS {_quote_identifier(alias)}"
                for alias, source in spec.compatibility_aliases
            )
            reader = f"(SELECT *, {aliases} FROM {reader})"
        return reader

    def _latest_version_sql(self, spec: DatasetSpec) -> str:
        """Resolve each id to its newest record.

        Ranking is skipped outright when no delta has been published, because
        nothing can supersede anything and the base file already holds one row
        per id. That is the state a fresh deployment starts in and the state
        compaction returns the object to.

        Where deltas do exist, base and deltas are ranked together. An earlier
        revision split this into an anti-join -- deltas ranked among themselves,
        unioned with the base rows no delta supersedes -- on the theory that a
        window function blocks predicate pushdown. Measurement refuted it:
        DuckDB pushes a filter on the PARTITION BY key through row_number(),
        and the anti-join was slower on every access shape except LIMIT
        (point lookup 21ms -> 39ms, count 45ms -> 57ms). The window stays.

        What does dominate at high partition counts is per-file Parquet
        overhead, not ranking: 366 files cost 12ms to open and scan where one
        compacted file costs 0.4ms. Compaction is the lever, not this SQL.
        """
        sources = spec.current_sources()
        if spec.version_field is None:
            return f"SELECT * FROM {self._aliased_reader(spec, sources)}"

        base_files = set(spec.sources)
        delta_paths = tuple(path for path in sources if path not in base_files)
        if not delta_paths:
            base_paths = tuple(path for path in sources if path in base_files)
            return f"SELECT * FROM {self._aliased_reader(spec, base_paths)}"

        return self._newest_per_id_sql(spec, self._aliased_reader(spec, sources))

    @staticmethod
    def _newest_per_id_sql(spec: DatasetSpec, reader: str) -> str:
        return (
            f"SELECT * EXCLUDE ({_VERSION_RANK}) FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY {_quote_identifier(spec.id_field)} "
            f"ORDER BY {_quote_identifier(spec.version_field)} DESC) AS {_VERSION_RANK} "
            f"FROM {reader}) WHERE {_VERSION_RANK} = 1"
        )

    def initialize_mutable_tables(self, state_store: StateStore) -> None:
        """Copy configured mutable sources disk-to-disk into persistent tables.

        DuckDB executes the copy internally, so this never materializes the
        complete source object in Python memory.  The operation is idempotent:
        existing tables are retained for restart-safe ingest processing.
        """
        self._mutable_database_path = state_store.database_path
        # The first bootstrap connection cannot create a view over a table
        # that has not been created yet.
        with self.connection(create_source_views=False) as conn:
            for object_name in self.catalog.object_names:
                spec = self.catalog.get(object_name)
                assert spec is not None
                if spec.mode != "mutable":
                    continue
                table_name = _quote_identifier(f"ff_mutable_{spec.object_name}")
                reader = self._source_reader(spec.current_sources())
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {reader}"
                )
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS "
                    f"{_quote_identifier(f'ff_mutable_{spec.object_name}_{spec.id_field}_unique')} "
                    f"ON {table_name} ({_quote_identifier(spec.id_field)})"
                )

    @staticmethod
    def _source_reader(source_paths: tuple[Path, ...], schema: pa.Schema | None = None) -> str:
        if not source_paths:
            if schema is None:
                raise ValueError("an empty source set requires a declared schema")
            projection = ", ".join(
                f"CAST(NULL AS {DuckDBEngine._duckdb_type(field.type)}) "
                f"AS {_quote_identifier(field.name)}"
                for field in schema
            )
            return f"(SELECT {projection} WHERE FALSE)"
        sources = ", ".join(_quote_literal(path) for path in source_paths)
        if all(path.name.endswith(".parquet") for path in source_paths):
            return f"read_parquet([{sources}], union_by_name = true)"
        if all(path.name.endswith((".csv", ".csv.gz")) for path in source_paths):
            return f"read_csv_auto([{sources}], header = true, union_by_name = true)"
        raise ValueError("all configured sources must share a supported format")

    @staticmethod
    def _duckdb_type(data_type: pa.DataType) -> str:
        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            return "VARCHAR"
        if pa.types.is_boolean(data_type):
            return "BOOLEAN"
        if pa.types.is_integer(data_type):
            return "BIGINT"
        if pa.types.is_floating(data_type):
            return "DOUBLE"
        if pa.types.is_date(data_type):
            return "DATE"
        if pa.types.is_timestamp(data_type):
            return "TIMESTAMPTZ" if data_type.tz else "TIMESTAMP"
        raise ValueError(f"unsupported declared Arrow type: {data_type}")
