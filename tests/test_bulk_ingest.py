from __future__ import annotations

import asyncio
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.bulk.ingest import (
    BulkIngestWorker,
    IngestUploadStore,
    IngestUploadTooLarge,
    IngestValidationError,
    validate_ingest_create,
)
from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType
from fakeforce.engine import DuckDBEngine
from fakeforce.state import StateStore


def _mutable_catalog(tmp_path) -> DatasetCatalog:
    data = tmp_path / "data"
    data.mkdir()
    pq.write_table(
        pa.table({"Id": ["001"], "IsDeleted": [False], "Name": ["A"]}),
        data / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "Account", "sources": ["accounts.parquet"], "mode": "mutable"
    }]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data),
        "FAKEFORCE_DATA_ROOTS": str(data),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
    })
    return DatasetCatalog.from_file(catalog_path, settings.data_roots)


def test_ingest_create_is_catalog_driven_and_limited_to_mutable_insert_csv(tmp_path) -> None:
    catalog = _mutable_catalog(tmp_path)

    config = validate_ingest_create({"object": "Account", "operation": "insert"}, catalog)

    assert config.object_name == "Account"
    assert config.content_type == "CSV"
    with pytest.raises(IngestValidationError, match="only insert"):
        validate_ingest_create({"object": "Account", "operation": "update"}, catalog)
    with pytest.raises(IngestValidationError, match="Unknown configured object"):
        validate_ingest_create({"object": "NoSuchObject", "operation": "insert"}, catalog)


def test_ingest_upload_streams_to_an_atomic_disk_file_with_a_limit(tmp_path) -> None:
    store = IngestUploadStore(tmp_path / "ingest", disk_reserve_bytes=0)

    async def chunks():
        yield b"Id,Name\n"
        yield b"001,Acme\n"

    written = asyncio.run(store.write("job-1", chunks(), max_bytes=100))

    assert written == len(b"Id,Name\n001,Acme\n")
    assert store.path_for("job-1").read_bytes() == b"Id,Name\n001,Acme\n"

    async def oversized():
        yield b"too-large"

    with pytest.raises(IngestUploadTooLarge):
        asyncio.run(store.write("job-2", oversized(), max_bytes=1))
    assert not store.path_for("job-2").exists()


def test_ingest_worker_commits_bounded_rows_results_and_checkpoint_together(tmp_path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    pq.write_table(
        pa.table({"Id": ["001"], "IsDeleted": [False], "Name": ["Existing"]}),
        data / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [{
        "name": "Account", "sources": ["accounts.parquet"], "mode": "mutable"
    }]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data),
        "FAKEFORCE_DATA_ROOTS": str(data),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
        "FAKEFORCE_BULK_INGEST_BATCH_RECORDS": "2",
    })
    catalog = DatasetCatalog.from_file(catalog_path, settings.data_roots)
    state = StateStore.from_settings(settings)
    state.initialize()
    engine = DuckDBEngine(settings, catalog)
    engine.initialize_mutable_tables(state)
    uploads = IngestUploadStore(tmp_path / "uploads", disk_reserve_bytes=0)
    state.create_job(BulkJob(
        "job-1", BulkJobType.INGEST, "v60.0", BulkJobState.UPLOAD_COMPLETE,
        "Account", "insert", None, catalog.snapshot_id,
    ))

    async def rows():
        yield b"Id,Name,IsDeleted\n"
        yield b",New,false\n001,Duplicate,false\n,Invalid,not-a-bool\n"

    asyncio.run(uploads.write("job-1", rows(), max_bytes=1000))
    result = BulkIngestWorker(settings, state, engine, uploads).run("job-1")

    assert result.records_processed == 1
    assert result.records_failed == 2
    assert state.get_job("job-1").state == BulkJobState.JOB_COMPLETE
    assert state.ingest_progress("job-1").csv_row_number == 3
    with engine.connection() as conn:
        assert conn.execute('SELECT Name FROM "ff_mutable_Account" ORDER BY Name').fetchall() == [
            ("Existing",), ("New",)
        ]
    with state.connection() as conn:
        assert conn.execute(
            "SELECT status FROM job_row_results WHERE job_id = 'job-1' ORDER BY csv_row_number"
        ).fetchall() == [("success",), ("failed",), ("failed",)]
    assert b"sf__Id,sf__Created,sf__Error" in b"".join(
        state.stream_ingest_results("job-1", "success", ["Id", "Name", "IsDeleted"])
    )
    failures = b"".join(state.stream_ingest_results("job-1", "failed", ["Id", "Name", "IsDeleted"]))
    assert b"DUPLICATE_VALUE" in failures
    assert b"not-a-bool" in failures
