"""EntityDefinition/FieldDefinition: schema discovery through SOQL, not just /describe.

Salesforce's real Tooling API restricts these two objects in ways ordinary
sObjects are not -- see catalog.py's module docstring above
`_metadata_catalog_specs` for which restrictions are implemented and why
some (OR, NOT, GROUP BY, COUNT(), INCLUDES) are deliberately not.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import LazyQueryService, QueryValidationError


@pytest.fixture()
def catalog(tmp_path) -> DatasetCatalog:
    data_root = tmp_path / "data"
    data_root.mkdir()
    pq.write_table(
        pa.table({"Id": ["001", "002"], "IsDeleted": [False, False], "Name": ["Acme", "Beta"]}),
        data_root / "accounts.parquet",
    )
    pq.write_table(
        pa.table({
            "Id": ["006"], "IsDeleted": [False], "Name": ["Big Deal"], "AccountId": ["001"],
        }),
        data_root / "opportunities.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"version": 1, "objects": [
        {"name": "Account", "sources": ["accounts.parquet"]},
        {"name": "Opportunity", "sources": ["opportunities.parquet"], "mode": "mutable"},
    ]}))
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data_root),
        "FAKEFORCE_DATA_ROOTS": str(data_root),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    return DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)


@pytest.fixture()
def query_service(tmp_path, catalog) -> LazyQueryService:
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(tmp_path / "data"),
        "FAKEFORCE_DATA_ROOTS": str(tmp_path / "data"),
        "FAKEFORCE_CATALOG_PATH": str(tmp_path / "catalog.json"),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    return LazyQueryService(catalog, DuckDBEngine(settings, catalog), "v60.0")


def test_catalog_always_exposes_the_two_metadata_objects(catalog) -> None:
    assert "EntityDefinition" in catalog.object_names
    assert "FieldDefinition" in catalog.object_names


def test_entity_definition_reflects_configured_objects(catalog) -> None:
    spec = catalog.get("EntityDefinition")
    assert spec is not None
    rows = {row["QualifiedApiName"]: row for row in spec.computed_table.to_pylist()}

    assert rows["Account"]["KeyPrefix"] == "001"
    assert rows["Account"]["IsEverCreatable"] is False  # mode: read_only (the default)
    assert rows["Opportunity"]["KeyPrefix"] == "006"
    assert rows["Opportunity"]["IsEverCreatable"] is True  # mode: mutable


def test_field_definition_carries_the_dotted_owning_object_column(catalog) -> None:
    spec = catalog.get("FieldDefinition")
    assert spec is not None
    assert "EntityDefinition.QualifiedApiName" in spec.schema.names
    rows = [row for row in spec.computed_table.to_pylist() if row["QualifiedApiName"] == "AccountId"]

    [account_id_row] = rows
    assert account_id_row["EntityDefinition.QualifiedApiName"] == "Opportunity"
    assert account_id_row["DataType"] == "Lookup(Account)"
    assert account_id_row["ReferenceTo"] == "Account"


def test_field_definition_marks_the_id_field_as_the_id_type(catalog) -> None:
    spec = catalog.get("FieldDefinition")
    rows = [row for row in spec.computed_table.to_pylist() if row["QualifiedApiName"] == "Id"]

    assert all(row["DataType"] == "Id" for row in rows)
    assert all(row["IsNillable"] is False for row in rows)


def test_soql_lists_configured_objects_through_entity_definition(query_service) -> None:
    page = query_service.fetch_page(
        "SELECT QualifiedApiName, KeyPrefix FROM EntityDefinition ORDER BY QualifiedApiName",
        False, page_size=10,
    )

    names = {record["QualifiedApiName"] for record in page.records}
    assert {"Account", "Opportunity"}.issubset(names)


def test_soql_limit_is_silently_ignored_on_entity_definition(query_service) -> None:
    unlimited = query_service.fetch_page("SELECT QualifiedApiName FROM EntityDefinition", False, page_size=10)
    limited = query_service.fetch_page(
        "SELECT QualifiedApiName FROM EntityDefinition LIMIT 1", False, page_size=10
    )

    assert len(limited.records) == len(unlimited.records)
    assert limited.total_size == unlimited.total_size


def test_soql_rejects_ne_on_a_metadata_object(query_service) -> None:
    with pytest.raises(QueryValidationError, match="Only equals comparisons permitted"):
        query_service.fetch_page(
            "SELECT QualifiedApiName FROM EntityDefinition WHERE QualifiedApiName != 'Account'",
            False, page_size=10,
        )


def test_field_definition_requires_a_scoping_filter(query_service) -> None:
    with pytest.raises(QueryValidationError, match="requires a filter"):
        query_service.fetch_page("SELECT QualifiedApiName FROM FieldDefinition", False, page_size=10)


def test_field_definition_scoped_by_dotted_entity_definition_field(query_service) -> None:
    page = query_service.fetch_page(
        "SELECT QualifiedApiName FROM FieldDefinition "
        "WHERE EntityDefinition.QualifiedApiName = 'Account'",
        False, page_size=10,
    )

    assert {record["QualifiedApiName"] for record in page.records} == {"Id", "IsDeleted", "Name"}


def test_field_definition_scoped_by_entity_definition_id(query_service) -> None:
    entity_page = query_service.fetch_page(
        "SELECT DurableId FROM EntityDefinition WHERE QualifiedApiName = 'Account'", False, page_size=1
    )
    durable_id = entity_page.records[0]["DurableId"]

    page = query_service.fetch_page(
        f"SELECT QualifiedApiName FROM FieldDefinition WHERE EntityDefinitionId = '{durable_id}'",
        False, page_size=10,
    )

    assert {record["QualifiedApiName"] for record in page.records} == {"Id", "IsDeleted", "Name"}
