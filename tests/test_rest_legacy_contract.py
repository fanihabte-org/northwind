"""Small compatibility contract for the currently supported REST surface.

These tests describe intentional public behavior, not the in-memory mechanics
behind it.  They remain valid while FF-004 replaces the data repository.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from fakeforce import app as fakeforce
from fakeforce.bulk.ingest import IngestUploadStore
from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.deadline import QueryDeadlineExceeded
from fakeforce.state import StateStore


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
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def test_query_returns_salesforce_shaped_records(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id, Name FROM Account LIMIT 2"},
        headers=auth_headers,
    )

    assert response.status_code == 200
    body = response.json()
    assert body["totalSize"] == 2
    assert body["done"] is True
    assert len(body["records"]) == 2
    assert body["records"][0]["attributes"]["type"] == "Account"
    assert {"Id", "Name"}.issubset(body["records"][0])


def test_offset_above_salesforce_limit_has_shaped_error(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id FROM Account OFFSET 2001"},
        headers=auth_headers,
    )

    assert response.status_code == 400
    assert response.json()[0]["errorCode"] == "NUMBER_OUTSIDE_VALID_RANGE"


def test_cursor_paging_keeps_only_small_query_metadata(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    fakeforce.chaos["page_size"] = 1
    first = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id, Name FROM Account ORDER BY Id LIMIT 2"},
        headers=auth_headers,
    )

    assert first.status_code == 200
    locator = first.json()["nextRecordsUrl"].rsplit("/", 1)[-1]
    locator_id, _, offset_text = locator.rpartition("-")
    cursor = fakeforce.STATE_STORE.get_cursor(locator_id)
    assert cursor is not None
    assert cursor.normalized_query == "SELECT Id, Name FROM Account ORDER BY Id LIMIT 2"
    assert cursor.result_artifact.endswith(f"{locator_id}.parquet")
    assert offset_text == "1"

    second = client.get(
        f"/services/data/v60.0/query/{locator}", headers=auth_headers
    )
    replay = client.get(
        f"/services/data/v60.0/query/{locator}", headers=auth_headers
    )

    assert second.status_code == 200
    assert second.json()["records"] == replay.json()["records"]
    assert first.json()["records"][0]["Id"] != second.json()["records"][0]["Id"]


def test_query_options_rejects_sync_page_sizes_outside_salesforce_range(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id FROM Account"},
        headers={**auth_headers, "Sforce-Query-Options": "batchSize=2001"},
    )

    assert response.status_code == 400
    assert response.json()[0]["errorCode"] == "MALFORMED_QUERY"


def test_query_deadline_returns_a_shaped_timeout_error(
    client: TestClient, auth_headers: dict[str, str], monkeypatch
) -> None:
    monkeypatch.setattr(
        fakeforce.QUERY_SERVICE,
        "fetch_page_with_cursor_index",
        lambda *_: (_ for _ in ()).throw(QueryDeadlineExceeded()),
    )

    response = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id FROM Account"},
        headers=auth_headers,
    )

    assert response.status_code == 500
    assert response.json()[0]["errorCode"] == "QUERY_TIMEOUT"


def test_data_responses_include_durable_salesforce_limit_usage(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get(
        "/services/data/v60.0/limits",
        headers=auth_headers,
    )

    assert response.status_code == 200
    assert response.headers["Sforce-Limit-Info"].startswith("api-usage=")
    assert response.json()["DailyApiRequests"]["Remaining"] <= response.json()["DailyApiRequests"]["Max"]


def test_missing_bearer_token_has_a_salesforce_shaped_authentication_error(client: TestClient) -> None:
    response = client.get(
        "/services/data/v60.0/query",
        params={"q": "SELECT Id FROM Account"},
    )

    assert response.status_code == 401
    assert response.json()[0]["errorCode"] == "INVALID_SESSION_ID"


def test_bulk_query_job_can_be_created_inspected_and_aborted(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    created = client.post(
        "/services/data/v60.0/jobs/query",
        headers=auth_headers,
        json={"operation": "query", "query": "SELECT Id FROM Account LIMIT 1"},
    )

    assert created.status_code == 200
    job_id = created.json()["id"]
    assert created.json()["state"] == "UploadComplete"
    assert client.get(f"/services/data/v60.0/jobs/query/{job_id}", headers=auth_headers).status_code == 200

    aborted = client.patch(
        f"/services/data/v60.0/jobs/query/{job_id}",
        headers=auth_headers,
        json={"state": "Aborted"},
    )
    assert aborted.status_code == 200
    assert aborted.json()["state"] == "Aborted"


def test_bulk_query_polling_returns_retry_guidance_when_the_shared_throttle_is_full(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    """Bulk submission, polling, and result downloads share the API limiter."""
    fakeforce.chaos["rate_limit_per_min"] = 1
    created = client.post(
        "/services/data/v60.0/jobs/query",
        headers=auth_headers,
        json={"operation": "query", "query": "SELECT Id FROM Account LIMIT 1"},
    )
    assert created.status_code == 200

    throttled = client.get(
        f"/services/data/v60.0/jobs/query/{created.json()['id']}", headers=auth_headers
    )

    assert throttled.status_code == 429
    assert throttled.json()[0]["errorCode"] == "REQUEST_LIMIT_EXCEEDED"
    assert 1 <= int(throttled.headers["Retry-After"]) <= 60
    assert throttled.headers["Sforce-Limit-Info"].startswith("api-usage=")


def test_bulk_ingest_job_uploads_csv_to_disk_then_closes_durably(
    client: TestClient, auth_headers: dict[str, str], tmp_path, monkeypatch
) -> None:
    data = tmp_path / "data"
    data.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table({"Id": ["001"], "IsDeleted": [False], "Name": ["A"]}),
        data / "accounts.parquet",
    )
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        '{"version":1,"objects":[{"name":"Account","sources":["accounts.parquet"],"mode":"mutable"}]}'
    )
    settings = Settings.from_env({
        "FAKEFORCE_SEED_DIR": str(data),
        "FAKEFORCE_DATA_ROOTS": str(data),
        "FAKEFORCE_CATALOG_PATH": str(catalog_path),
        "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
    })
    state = StateStore.from_settings(settings)
    state.initialize()
    monkeypatch.setattr(fakeforce, "CATALOG", DatasetCatalog.from_file(catalog_path, settings.data_roots))
    monkeypatch.setattr(fakeforce, "STATE_STORE", state)
    uploads = IngestUploadStore(tmp_path / "uploads", disk_reserve_bytes=0)
    monkeypatch.setattr(fakeforce, "BULK_INGEST_UPLOADS", uploads)
    monkeypatch.setattr(fakeforce.BULK_INGEST_DISPATCHER, "submit", lambda _job_id: None)

    created = client.post(
        "/services/data/v60.0/jobs/ingest",
        headers=auth_headers,
        json={"object": "Account", "operation": "insert", "contentType": "CSV"},
    )

    assert created.status_code == 200
    assert created.json()["state"] == "Open"
    job_id = created.json()["id"]
    uploaded = client.put(
        f"/services/data/v60.0/jobs/ingest/{job_id}/batches",
        headers=auth_headers,
        content=b"Name\nNew Account\n",
    )
    assert uploaded.status_code == 201
    assert uploads.path_for(job_id).is_file()
    closed = client.patch(
        f"/services/data/v60.0/jobs/ingest/{job_id}",
        headers=auth_headers,
        json={"state": "UploadComplete"},
    )

    assert closed.status_code == 200
    assert closed.json()["state"] == "UploadComplete"
    assert client.get(f"/services/data/v60.0/jobs/ingest/{job_id}", headers=auth_headers).status_code == 200
