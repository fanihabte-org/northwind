from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine
from fakeforce.state import StateStore


def test_engine_queries_registered_parquet_through_a_lazy_view(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001", "002"], "IsDeleted": [False, True], "Name": ["A", "B"]}),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [{"name": "Account", "sources": ["accounts.parquet"]}],
            }
        )
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data_root),
            "FAKEFORCE_DATA_ROOTS": str(data_root),
            "FAKEFORCE_CATALOG_PATH": str(catalog_path),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
            "FAKEFORCE_MEMORY_LIMIT": "128MB",
        }
    )
    engine = DuckDBEngine(
        settings, DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    )

    with engine.connection() as conn:
        rows = conn.execute(
            'SELECT "Id", "Name" FROM "ff_source_Account" WHERE NOT "IsDeleted"'
        ).fetchall()

    assert rows == [("001", "A")]


def test_engine_initializes_mutable_source_as_a_persistent_duckdb_table(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001"], "IsDeleted": [False], "Name": ["A"]}),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [
                    {"name": "Account", "sources": ["accounts.parquet"], "mode": "mutable"}
                ],
            }
        )
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data_root),
            "FAKEFORCE_DATA_ROOTS": str(data_root),
            "FAKEFORCE_CATALOG_PATH": str(catalog_path),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    engine = DuckDBEngine(settings, catalog)
    state_store = StateStore.from_settings(settings)
    state_store.initialize()

    engine.initialize_mutable_tables(state_store)

    with engine.connection() as conn:
        assert conn.execute('SELECT "Name" FROM "ff_source_Account"').fetchall() == [("A",)]


def test_engine_checks_spill_directory_reserve_before_opening_connection(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001"], "IsDeleted": [False]}), data_root / "accounts.parquet"
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps({"version": 1, "objects": [{"name": "Account", "sources": ["accounts.parquet"]}]})
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data_root),
            "FAKEFORCE_DATA_ROOTS": str(data_root),
            "FAKEFORCE_CATALOG_PATH": str(catalog_path),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
            "FAKEFORCE_DISK_RESERVE_BYTES": "42",
        }
    )
    calls = []
    monkeypatch.setattr(
        "fakeforce.engine.require_disk_reserve",
        lambda directory, reserve: calls.append((directory, reserve)),
    )
    engine = DuckDBEngine(
        settings, DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    )

    with engine.connection():
        pass

    assert calls == [(settings.temp_directory, 42)]
