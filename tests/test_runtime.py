from __future__ import annotations

import asyncio

from fakeforce.runtime import AdmissionController, classify_operation


def test_admission_controller_serializes_configured_heavy_queries() -> None:
    async def run() -> int:
        controller = AdmissionController(lightweight_workers=2, query_workers=1, bulk_workers=1)
        active = 0
        maximum_active = 0

        async def query() -> None:
            nonlocal active, maximum_active
            async with controller.slot("query"):
                active += 1
                maximum_active = max(maximum_active, active)
                await asyncio.sleep(0)
                active -= 1

        await asyncio.gather(query(), query())
        return maximum_active

    assert asyncio.run(run()) == 1


def test_operation_classification_keeps_status_checks_lightweight() -> None:
    assert classify_operation("/services/data/v60.0/query", "v60.0") == "query"
    assert classify_operation("/services/data/v60.0/query/locator", "v60.0") == "query"
    assert classify_operation("/services/data/v60.0/jobs/query", "v60.0") == "bulk_query"
    assert classify_operation("/services/data/v60.0/jobs/ingest/job-1", "v60.0") == "bulk_ingest"
    assert classify_operation("/services/data/v60.0/limits", "v60.0") == "lightweight"
