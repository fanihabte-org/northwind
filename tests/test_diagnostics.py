from __future__ import annotations

from fastapi.testclient import TestClient

from fakeforce import app as fakeforce


def test_internal_diagnostics_expose_memory_query_and_job_snapshots() -> None:
    with TestClient(fakeforce.app) as client:
        memory = client.get("/_diagnostics/memory")
        overview = client.get("/_diagnostics")
        queries = client.get("/_diagnostics/queries")
        jobs = client.get("/_diagnostics/jobs")

    assert memory.status_code == 200
    assert {"process_rss_bytes", "duckdb_memory_bytes", "temporary_directory_bytes"}.issubset(
        memory.json()
    )
    assert overview.status_code == 200
    assert {"memory", "disk", "queries", "jobs", "api_calls_last_24_hours"} == set(
        overview.json()
    )
    assert overview.json()["disk"]["state_filesystem_free_bytes"] > 0
    assert overview.json()["disk"]["disk_reserve_bytes"] > 0
    assert queries.json()["open_cursors"] >= 0
    assert jobs.json()["active_bulk_jobs"] == 0
    assert jobs.json()["queued_bulk_jobs"] == 0
    assert jobs.json()["checkpointed_bulk_jobs"] >= 0
    assert jobs.json()["bulk_job_retention_hours"] > 0
