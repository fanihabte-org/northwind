"""Connections carry only the views their query needs, and pages open one.

Views are rebuilt per connection and building one resolves that object's
sources, so an unscoped connection makes a single-object query pay for every
object in the catalog. Serving a cursor page used to open two connections and
pay that cost twice.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from fakeforce import app as fakeforce
from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import LazyQueryService, QueryValidationError


def _write(path, ids):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "Id": list(ids),
                "IsDeleted": [False] * len(ids),
                "Name": [f"n{identifier}" for identifier in ids],
            }
        ),
        path,
    )


@pytest.fixture()
def two_object_engine(tmp_path) -> tuple[DuckDBEngine, DatasetCatalog]:
    data_root = tmp_path / "data"
    _write(data_root / "crm_accounts.parquet", ["001", "002"])
    _write(data_root / "crm_opportunities.parquet", ["006", "007"])
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [
                    {"name": "Account", "sources": ["crm_accounts.parquet"]},
                    {"name": "Opportunity", "sources": ["crm_opportunities.parquet"]},
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
            "FAKEFORCE_MEMORY_LIMIT": "256MB",
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    return DuckDBEngine(settings, catalog), catalog


def _views(connection) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        ).fetchall()
    }


def test_a_scoped_connection_builds_only_the_named_view(two_object_engine) -> None:
    engine, _ = two_object_engine

    with engine.connection(objects=("Account",)) as connection:
        assert _views(connection) == {"ff_source_Account"}


def test_an_unscoped_connection_still_builds_every_view(two_object_engine) -> None:
    engine, _ = two_object_engine

    with engine.connection() as connection:
        assert _views(connection) == {"ff_source_Account", "ff_source_Opportunity"}


def test_a_scoped_connection_rejects_an_unknown_object(two_object_engine) -> None:
    engine, _ = two_object_engine

    with pytest.raises(KeyError, match="unknown configured object"):
        with engine.connection(objects=("Contact",)):
            pass


def test_a_query_opens_one_connection_scoped_to_its_object(two_object_engine, monkeypatch) -> None:
    engine, catalog = two_object_engine
    service = LazyQueryService(catalog, engine, "v60.0")
    scopes: list[tuple[str, ...] | None] = []
    original = DuckDBEngine.connection

    def recorded(self, database_path=None, *, create_source_views=True, objects=None):
        scopes.append(objects)
        return original(
            self, database_path, create_source_views=create_source_views, objects=objects
        )

    monkeypatch.setattr(DuckDBEngine, "connection", recorded)
    page = service.fetch_page("SELECT Id FROM Account", False, 10)

    assert len(page.records) == 2
    assert scopes == [("Account",)]


def test_planning_happens_before_a_connection_is_opened(two_object_engine, monkeypatch) -> None:
    """A malformed query must not cost a connection or a view rebuild."""
    engine, catalog = two_object_engine
    service = LazyQueryService(catalog, engine, "v60.0")
    opened: list[int] = []

    def recorded(self, database_path=None, *, create_source_views=True, objects=None):
        opened.append(1)
        raise AssertionError("no connection should be opened for an invalid query")

    monkeypatch.setattr(DuckDBEngine, "connection", recorded)

    with pytest.raises(QueryValidationError):
        service.fetch_page("SELECT Id FROM Contact", False, 10)
    with pytest.raises(QueryValidationError):
        service.fetch_page("SELECT Nope FROM Account", False, 10)

    assert opened == []


def test_locator_rehydration_reads_ids_and_records_in_one_connection(
    two_object_engine, tmp_path, monkeypatch
) -> None:
    engine, catalog = two_object_engine
    service = LazyQueryService(catalog, engine, "v60.0")
    from fakeforce.cursor_artifacts import CursorArtifactStore

    artifacts = CursorArtifactStore(tmp_path / "cursors")
    query = "SELECT Id, Name FROM Account ORDER BY Id"
    _, artifact = service.fetch_page_with_cursor_index(query, False, 1, 0, "loc1", artifacts)
    assert artifact is not None

    opened: list[tuple[str, ...] | None] = []
    original = DuckDBEngine.connection

    def recorded(self, database_path=None, *, create_source_views=True, objects=None):
        opened.append(objects)
        return original(
            self, database_path, create_source_views=create_source_views, objects=objects
        )

    monkeypatch.setattr(DuckDBEngine, "connection", recorded)
    records = service.fetch_records_for_locator(query, False, artifact, 1, 1, artifacts)

    assert [record["Id"] for record in records] == ["002"]
    assert opened == [("Account",)], "one connection, scoped to the queried object"


def test_locator_rehydration_returns_nothing_past_the_end(
    two_object_engine, tmp_path
) -> None:
    engine, catalog = two_object_engine
    service = LazyQueryService(catalog, engine, "v60.0")
    from fakeforce.cursor_artifacts import CursorArtifactStore

    artifacts = CursorArtifactStore(tmp_path / "cursors")
    query = "SELECT Id FROM Account ORDER BY Id"
    _, artifact = service.fetch_page_with_cursor_index(query, False, 1, 0, "loc2", artifacts)

    assert service.fetch_records_for_locator(query, False, artifact, 99, 10, artifacts) == []


@pytest.fixture()
def client() -> Iterator[TestClient]:
    fakeforce.reset_chaos()
    with TestClient(fakeforce.app) as test_client:
        yield test_client
    fakeforce.reset_chaos()


@pytest.fixture()
def auth_headers(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "scoped",
            "client_secret": "scoped",
        },
    )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def test_serving_a_cursor_page_over_rest_opens_a_single_connection(
    client: TestClient, auth_headers: dict[str, str], monkeypatch
) -> None:
    fakeforce.chaos["page_size"] = 1
    query = "SELECT Id, Name FROM Account ORDER BY Id LIMIT 3"
    first = client.get(
        "/services/data/v60.0/query", params={"q": query}, headers=auth_headers
    )
    locator = first.json()["nextRecordsUrl"].rsplit("/", 1)[-1]

    opened: list[tuple[str, ...] | None] = []
    original = DuckDBEngine.connection

    def recorded(self, database_path=None, *, create_source_views=True, objects=None):
        opened.append(objects)
        return original(
            self, database_path, create_source_views=create_source_views, objects=objects
        )

    monkeypatch.setattr(DuckDBEngine, "connection", recorded)
    second = client.get(f"/services/data/v60.0/query/{locator}", headers=auth_headers)

    assert second.status_code == 200
    assert len(second.json()["records"]) == 1
    assert opened == [("Account",)], "a page must not rebuild views twice"
