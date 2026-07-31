from __future__ import annotations

from fastapi.testclient import TestClient

from fakeforce import app as fakeforce


def test_internal_diagnostics_expose_memory_query_and_job_snapshots() -> None:
    with TestClient(fakeforce.app) as client:
        memory = client.get("/_diagnostics/memory")
        queries = client.get("/_diagnostics/queries")
        jobs = client.get("/_diagnostics/jobs")

    assert memory.status_code == 200
    assert {"process_rss_bytes", "duckdb_memory_bytes", "temporary_directory_bytes"}.issubset(
        memory.json()
    )
    assert queries.json()["open_cursors"] >= 0
    assert jobs.json() == {"active_bulk_jobs": 0, "queued_bulk_jobs": 0}
