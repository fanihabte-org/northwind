from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import (
    LazyQueryService,
    QueryOptionError,
    QueryValidationError,
    parse_query_batch_size,
)


@pytest.fixture()
def query_service(tmp_path) -> LazyQueryService:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table(
            {
                "Id": ["001", "002", "003"],
                "IsDeleted": [False, True, False],
                "Name": ["Acme", "Deleted", "Beta"],
            }
        ),
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
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    return LazyQueryService(catalog, DuckDBEngine(settings, catalog), "v60.0")


def test_query_executes_against_lazy_view_with_projection_and_filter(query_service) -> None:
    page = query_service.fetch_page(
        "SELECT Id, Name FROM Account WHERE Name LIKE 'A%'", False, page_size=2
    )

    assert page.total_size == 1
    assert page.records == [
        {
            "attributes": {
                "type": "Account",
                "url": "/services/data/v60.0/sobjects/Account/001",
            },
            "Id": "001",
            "Name": "Acme",
        }
    ]


def test_query_all_includes_soft_deleted_records(query_service) -> None:
    assert query_service.fetch_page("SELECT Id FROM Account", True, page_size=10).total_size == 3


def test_query_can_rehydrate_requested_fields_by_cursor_ids(query_service) -> None:
    records = query_service.fetch_records_by_ids(
        "SELECT Name FROM Account", False, ["003", "001"]
    )

    assert [record["Name"] for record in records] == ["Beta", "Acme"]
    assert all("__fakeforce_record_id" not in record for record in records)


def test_query_service_writes_id_only_cursor_index(query_service, tmp_path) -> None:
    artifacts = CursorArtifactStore(tmp_path / "artifacts")
    artifact = query_service.build_cursor_index(
        "SELECT Name FROM Account ORDER BY Id", False, "locator-1", artifacts
    )
    import duckdb

    with duckdb.connect() as conn:
        assert artifacts.read_page_ids(conn, artifact, offset=0, size=10) == ["001", "003"]


def test_first_page_and_cursor_index_share_one_query_operation(query_service, tmp_path) -> None:
    artifacts = CursorArtifactStore(tmp_path / "artifacts")
    page, artifact = query_service.fetch_page_with_cursor_index(
        "SELECT Name FROM Account ORDER BY Id", False, 1, 0, "locator-1", artifacts
    )

    assert [record["Name"] for record in page.records] == ["Acme"]
    assert artifact is not None


def test_query_validation_retains_salesforce_offset_error(query_service) -> None:
    with pytest.raises(QueryValidationError) as error:
        query_service.fetch_page("SELECT Id FROM Account OFFSET 2001", False, page_size=1)
    assert error.value.code == "NUMBER_OUTSIDE_VALID_RANGE"


@pytest.mark.parametrize(
    ("header", "expected"), [(None, 2000), ("batchSize=200", 200), ("batchSize=2000", 2000)]
)
def test_query_options_allow_salesforce_batch_size_range(header, expected) -> None:
    assert parse_query_batch_size(header) == expected


@pytest.mark.parametrize("header", ["batchSize=199", "batchSize=2001", "batchSize=abc", "other=1"])
def test_query_options_reject_invalid_values(header) -> None:
    with pytest.raises(QueryOptionError):
        parse_query_batch_size(header)
