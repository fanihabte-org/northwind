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
    history_delta = data_root / "opportunity_history" / "business_date=2026-07-25" / "delta.parquet"
    history_delta.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([
        {
            "Id": "crm-history-001", "OpportunityId": "0061",
            "PreviousStageName": "Prospecting", "StageName": "Proposal",
            "CreatedDate": "2026-07-25T08:00:00.000+0000", "CreatedById": "REP-0001",
            "SystemModstamp": "2026-07-25T08:00:00.000+0000",
        },
        {
            "Id": "crm-history-002", "OpportunityId": "0061",
            "PreviousStageName": "Proposal", "StageName": "Closed Won",
            "CreatedDate": "2026-07-26T08:00:00.000+0000", "CreatedById": "REP-0001",
            "SystemModstamp": "2026-07-26T08:00:00.000+0000",
        },
    ], schema=history_schema), history_delta)
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps({"version": 1, "objects": [
            {"name": "Account", "sources": ["accounts.parquet"]},
            {
                "name": "OpportunityHistory", "sources": ["crm_opportunity_history.parquet"],
                "delta_patterns": ["opportunity_history/**/*.parquet"],
                "version_field": "SystemModstamp", "soft_delete_field": None,
            },
        ]})
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


def test_query_resolves_object_and_field_identifiers_without_case_sensitivity(
    query_service,
) -> None:
    page = query_service.fetch_page(
        "select id, name from account where name = 'acme' order by id", False, page_size=2
    )

    assert page.total_size == 1
    assert page.records[0]["Id"] == "001"
    assert page.records[0]["Name"] == "Acme"


def test_query_reads_immutable_opportunity_history_partitions(query_service) -> None:
    page = query_service.fetch_page(
        "SELECT Id, OpportunityId, PreviousStageName, StageName "
        "FROM opportunityhistory WHERE opportunityid = '0061' ORDER BY createddate",
        False,
        page_size=10,
    )

    assert page.total_size == 2
    assert [record["StageName"] for record in page.records] == ["Proposal", "Closed Won"]
    assert [record["PreviousStageName"] for record in page.records] == ["Prospecting", "Proposal"]
    assert all(record["attributes"]["type"] == "OpportunityHistory" for record in page.records)


def test_query_text_like_and_in_literals_are_case_insensitive(query_service) -> None:
    like_page = query_service.fetch_page(
        "SELECT Id FROM Account WHERE Name LIKE 'a%'", False, page_size=2
    )
    in_page = query_service.fetch_page(
        "SELECT Id FROM Account WHERE Name IN ('ACME', 'BETA') ORDER BY Id", False, page_size=2
    )

    assert [record["Id"] for record in like_page.records] == ["001"]
    assert [record["Id"] for record in in_page.records] == ["001", "003"]


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
