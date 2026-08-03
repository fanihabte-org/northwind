"""Lazy DuckDB access to configured FakeForce source datasets."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from fakeforce.catalog import DatasetCatalog, DatasetSpec
from fakeforce.config import Settings
from fakeforce.state import StateStore
from fakeforce.storage import require_disk_reserve


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
        self, database_path: Path | str | None = None, *, create_source_views: bool = True
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        self.settings.temp_directory.mkdir(parents=True, exist_ok=True)
        require_disk_reserve(
            self.settings.temp_directory, self.settings.disk_reserve_bytes
        )
        path = database_path or self._mutable_database_path or ":memory:"
        conn = duckdb.connect(str(path))
        try:
            self.configure_connection(conn)
            if create_source_views:
                self._create_read_only_views(conn)
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

    def _create_read_only_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        for object_name in self.catalog.object_names:
            spec = self.catalog.get(object_name)
            assert spec is not None
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
        reader = self._source_reader(spec.current_sources())
        if spec.version_field is not None:
            view_sql = (
                "SELECT * EXCLUDE (__fakeforce_version_rank) FROM ("
                f"SELECT *, row_number() OVER (PARTITION BY {_quote_identifier(spec.id_field)} "
                f"ORDER BY {_quote_identifier(spec.version_field)} DESC) AS __fakeforce_version_rank "
                f"FROM {reader}) WHERE __fakeforce_version_rank = 1"
            )
        else:
            view_sql = f"SELECT * FROM {reader}"
        conn.execute(
            f"CREATE OR REPLACE VIEW {_quote_identifier(self.relation_name(spec.object_name))} "
            f"AS {view_sql}"
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
    def _source_reader(source_paths: tuple[Path, ...]) -> str:
        sources = ", ".join(_quote_literal(path) for path in source_paths)
        if all(path.name.endswith(".parquet") for path in source_paths):
            return f"read_parquet([{sources}], union_by_name = true)"
        if all(path.name.endswith((".csv", ".csv.gz")) for path in source_paths):
            return f"read_csv_auto([{sources}], header = true, union_by_name = true)"
        raise ValueError(
            "all configured sources must share a supported format"
        )
