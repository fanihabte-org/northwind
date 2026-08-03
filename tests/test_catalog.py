from __future__ import annotations

import json
import os

import pytest

from fakeforce.catalog import CatalogError, DatasetCatalog


def write_catalog(path, objects) -> None:
    path.write_text(json.dumps({"version": 1, "objects": objects}))


def test_catalog_discovers_configured_csv_without_hardcoded_object_mapping(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    (data_root / "customers.csv").write_text("record_key,deleted,name\na,false,Acme\n")
    catalog_path = tmp_path / "catalog.json"
    write_catalog(
        catalog_path,
        [
            {
                "name": "Customer__c",
                "sources": ["customers.csv"],
                "id_field": "record_key",
                "soft_delete_field": "deleted",
            }
        ],
    )

    catalog = DatasetCatalog.from_file(catalog_path, [data_root])
    spec = catalog.get("Customer__c")

    assert catalog.object_names == ("Customer__c",)
    assert spec is not None
    assert spec.id_field == "record_key"
    assert spec.schema.names == ["record_key", "deleted", "name"]


def test_catalog_rejects_files_outside_allowed_roots(tmp_path) -> None:
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("Id,IsDeleted\n1,false\n")
    catalog_path = tmp_path / "catalog.json"
    write_catalog(
        catalog_path,
        [{"name": "Account", "sources": ["../outside.csv"]}],
    )

    with pytest.raises(CatalogError, match="source does not match a file"):
        DatasetCatalog.from_file(catalog_path, [allowed_root])


def test_snapshot_id_changes_when_a_configured_source_changes(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    source = data_root / "accounts.csv"
    source.write_text("Id,IsDeleted\n1,false\n")
    catalog_path = tmp_path / "catalog.json"
    write_catalog(catalog_path, [{"name": "Account", "sources": ["accounts.csv"]}])

    before = DatasetCatalog.from_file(catalog_path, [data_root]).snapshot_id
    source.write_text("Id,IsDeleted\n1,false\n2,false\n")
    os.utime(source, None)
    after = DatasetCatalog.from_file(catalog_path, [data_root]).snapshot_id

    assert after != before


def test_catalog_exposes_compatibility_aliases_for_an_older_source_schema(tmp_path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    (data_root / "accounts.csv").write_text(
        "Id,OwnerId,LastModifiedDate,IsDeleted\n001,REP-0001,2026-07-24T08:00:00.000+0000,false\n"
    )
    catalog_path = tmp_path / "catalog.json"
    write_catalog(
        catalog_path,
        [{
            "name": "Account",
            "sources": ["accounts.csv"],
            "version_field": "SystemModstamp",
            "compatibility_aliases": {
                "CreatedById": "OwnerId",
                "LastModifiedById": "OwnerId",
                "SystemModstamp": "LastModifiedDate",
            },
        }],
    )

    spec = DatasetCatalog.from_file(catalog_path, [data_root]).get("Account")

    assert spec is not None
    assert spec.schema.names[-3:] == ["CreatedById", "LastModifiedById", "SystemModstamp"]
    assert spec.compatibility_aliases == (
        ("CreatedById", "OwnerId"),
        ("LastModifiedById", "OwnerId"),
        ("SystemModstamp", "LastModifiedDate"),
    )
