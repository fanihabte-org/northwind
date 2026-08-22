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


def test_engine_discovers_parquet_deltas_and_returns_only_latest_record(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table(
            {
                "Id": ["001"],
                "IsDeleted": [False],
                "Name": ["Before"],
                "LastModifiedDate": ["2026-07-24T08:00:00.000+0000"],
            }
        ),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [
                    {
                        "name": "Account",
                        "sources": ["accounts.parquet"],
                        "delta_patterns": ["deltas/accounts/**/*.parquet"],
                        "version_field": "LastModifiedDate",
                    }
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
    delta = data_root / "deltas" / "accounts" / "business_date=2026-07-25" / "delta.parquet"
    delta.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "Id": ["001"],
                "IsDeleted": [False],
                "Name": ["After"],
                "LastModifiedDate": ["2026-07-25T08:00:00.000+0000"],
            }
        ),
        delta,
    )

    with DuckDBEngine(settings, catalog).connection() as conn:
        assert conn.execute('SELECT "Name" FROM "ff_source_Account"').fetchall() == [("After",)]


def test_engine_exposes_empty_declared_schema_when_history_base_is_absent(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "OpportunityHistory",
        "sources": ["crm_opportunity_history.parquet"],
        "schema": {"Id": "string", "StageName": "string"},
        "soft_delete_field": None,
    }]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root), "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path), "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)

    with DuckDBEngine(settings, catalog).connection() as conn:
        assert conn.execute('SELECT count(*) FROM "ff_source_OpportunityHistory"').fetchone() == (0,)


def test_engine_exposes_all_immutable_opportunity_history_partitions(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    history_schema = pa.schema([
        ("Id", pa.string()), ("OpportunityId", pa.string()),
        ("PreviousStageName", pa.string()), ("StageName", pa.string()),
        ("CreatedDate", pa.string()), ("CreatedById", pa.string()),
        ("SystemModstamp", pa.string()),
    ])
    pq.write_table(
        pa.Table.from_pylist([], schema=history_schema),
        data_root / "crm_opportunity_history.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "OpportunityHistory",
        "sources": ["crm_opportunity_history.parquet"],
        "delta_patterns": ["opportunity_history/**/*.parquet"],
        "version_field": "SystemModstamp",
        "soft_delete_field": None,
    }]}))
    for business_date, record in (
        ("2026-07-25", {
            "Id": "crm-history-001", "OpportunityId": "0061",
            "PreviousStageName": "Prospecting", "StageName": "Proposal",
            "CreatedDate": "2026-07-25T08:00:00.000+0000", "CreatedById": "REP-0001",
            "SystemModstamp": "2026-07-25T08:00:00.000+0000",
        }),
        ("2026-07-26", {
            "Id": "crm-history-002", "OpportunityId": "0061",
            "PreviousStageName": "Proposal", "StageName": "Closed Won",
            "CreatedDate": "2026-07-26T08:00:00.000+0000", "CreatedById": "REP-0001",
            "SystemModstamp": "2026-07-26T08:00:00.000+0000",
        }),
    ):
        target = data_root / "opportunity_history" / f"business_date={business_date}" / "delta.parquet"
        target.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([record], schema=history_schema), target)

    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root), "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path), "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)

    with DuckDBEngine(settings, catalog).connection() as conn:
        rows = conn.execute(
            'SELECT "PreviousStageName", "StageName" FROM "ff_source_OpportunityHistory" '
            'WHERE "OpportunityId" = ? ORDER BY "CreatedDate"', ["0061"]
        ).fetchall()

    assert rows == [("Prospecting", "Proposal"), ("Proposal", "Closed Won")]


def test_engine_exposes_audit_compatibility_aliases_and_can_version_by_system_modstamp(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({
            "Id": ["001"], "OwnerId": ["REP-0001"], "IsDeleted": [False],
            "Name": ["Before"], "LastModifiedDate": ["2026-07-24T08:00:00.000+0000"],
        }),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "Account", "sources": ["accounts.parquet"],
        "version_field": "SystemModstamp",
        "compatibility_aliases": {
            "CreatedById": "OwnerId", "LastModifiedById": "OwnerId",
            "SystemModstamp": "LastModifiedDate",
        },
    }]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root), "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path), "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)

    with DuckDBEngine(settings, catalog).connection() as conn:
        row = conn.execute(
            'SELECT "CreatedById", "LastModifiedById", "SystemModstamp" FROM "ff_source_Account"'
        ).fetchone()

    assert row == ("REP-0001", "REP-0001", "2026-07-24T08:00:00.000+0000")
