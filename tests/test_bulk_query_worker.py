from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType
from fakeforce.bulk.query import BulkQueryWorker
from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import LazyQueryService
from fakeforce.state import StateStore


def _build_worker(tmp_path, query: str):
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001", "002", "003"], "IsDeleted": [False, False, False], "Name": ["A", "B", "C"]}),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps({"version": 1, "objects": [{"name": "Account", "sources": ["accounts.parquet"]}]})
    )
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root),
        "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)
    engine = DuckDBEngine(settings, catalog)
    state = StateStore.from_settings(settings)
    state.initialize()
    state.create_job(BulkJob(
        "job-1", BulkJobType.QUERY, "v60.0", BulkJobState.UPLOAD_COMPLETE,
        None, "query", query, catalog.snapshot_id,
    ))
    worker = BulkQueryWorker(settings, state, engine, LazyQueryService(catalog, engine, "v60.0"))
    return worker


def test_bulk_query_resume_orders_by_the_internal_id_when_the_query_has_no_order_by(
    tmp_path, monkeypatch
) -> None:
    """A resumed job re-executes the query from scratch and skip-counts rows,
    so it must see the same order every run. Without an ORDER BY of its own,
    scan order over the source is unspecified and must be made deterministic."""
    worker = _build_worker(tmp_path, "SELECT Id, Name FROM Account")
    executed_sql: list[str] = []
    import duckdb
    original_execute = duckdb.DuckDBPyConnection.execute

    def recorded(self, sql, *args, **kwargs):
        executed_sql.append(sql)
        return original_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", recorded)

    worker.run("job-1")

    [query_sql] = [
        sql for sql in executed_sql
        if "ff_source_Account" in sql and not sql.startswith("CREATE")
    ]
    assert "ORDER BY" in query_sql
    assert "__fakeforce_record_id" in query_sql


def test_bulk_query_resume_does_not_duplicate_the_order_by_when_the_query_has_one(
    tmp_path, monkeypatch
) -> None:
    worker = _build_worker(tmp_path, "SELECT Id, Name FROM Account ORDER BY Name")
    executed_sql: list[str] = []
    import duckdb
    original_execute = duckdb.DuckDBPyConnection.execute

    def recorded(self, sql, *args, **kwargs):
        executed_sql.append(sql)
        return original_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "execute", recorded)

    worker.run("job-1")

    [query_sql] = [
        sql for sql in executed_sql
        if "ff_source_Account" in sql and not sql.startswith("CREATE")
    ]
    assert query_sql.count("ORDER BY") == 1
    assert "ORDER BY \"Name\"" in query_sql


def test_bulk_query_worker_streams_durable_csv_parts_and_checkpoints(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001", "002", "003"], "IsDeleted": [False, False, False], "Name": ["A", "B", "C"]}),
        data_root / "accounts.parquet",
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
            "FAKEFORCE_BULK_RESULT_PART_RECORDS": "2",
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    engine = DuckDBEngine(settings, catalog)
    state = StateStore.from_settings(settings)
    state.initialize()
    state.create_job(
        BulkJob(
            "job-1", BulkJobType.QUERY, "v60.0", BulkJobState.UPLOAD_COMPLETE,
            None, "query", "SELECT Id, Name FROM Account ORDER BY Id", catalog.snapshot_id
        )
    )
    worker = BulkQueryWorker(settings, state, engine, LazyQueryService(catalog, engine, "v60.0"))

    result = worker.run("job-1")

    output = settings.state_directory / "bulk" / "query" / "job-1"
    manifest = json.loads((output / "manifest.json").read_text())
    assert result.records_processed == 3
    assert result.parts_written == 2
    assert [part["record_count"] for part in manifest["parts"]] == [2, 1]
    assert (output / "part-00000.csv").read_text().splitlines()[0] == "Id,Name"
    assert state.get_job("job-1").state == BulkJobState.JOB_COMPLETE
    assert state.latest_job_checkpoint_records("job-1") == 3


def test_bulk_query_resume_uses_a_published_manifest_when_checkpoint_is_missing(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001", "002", "003"], "IsDeleted": [False, False, False], "Name": ["A", "B", "C"]}),
        data_root / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "Account", "sources": ["accounts.parquet"]
    }]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root),
        "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
        "FAKEFORCE_BULK_RESULT_PART_RECORDS": "2",
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)
    engine = DuckDBEngine(settings, catalog)
    state = StateStore.from_settings(settings)
    state.initialize()
    state.create_job(BulkJob(
        "job-1", BulkJobType.QUERY, "v60.0", BulkJobState.IN_PROGRESS,
        None, "query", "SELECT Id, Name FROM Account ORDER BY Id", catalog.snapshot_id,
    ))
    output = settings.state_directory / "bulk" / "query" / "job-1"
    output.mkdir(parents=True)
    (output / "part-00000.csv").write_text("Id,Name\n001,A\n002,B\n")
    (output / "part-uncommitted.csv").write_text("Id,Name\nshould,vanish\n")
    (output / "manifest.json").write_text(json.dumps({"parts": [{
        "part": 0, "path": "part-00000.csv", "record_count": 2,
        "byte_size": 18, "checksum": "ignored", "start_record": 0, "end_record": 1,
    }]}))
    worker = BulkQueryWorker(settings, state, engine, LazyQueryService(catalog, engine, "v60.0"))

    worker.run("job-1")

    manifest = json.loads((output / "manifest.json").read_text())
    assert [part["record_count"] for part in manifest["parts"]] == [2, 1]
    assert (output / "part-00001.csv").read_text().splitlines() == ["Id,Name", "003,C"]
    assert not (output / "part-uncommitted.csv").exists()
