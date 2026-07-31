"""Local admission control separate from Salesforce API allocation limits."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Literal


OperationKind = Literal["lightweight", "query", "bulk_query", "bulk_ingest"]


@dataclass(frozen=True)
class AdmissionSnapshot:
    active: dict[str, int]
    queued: dict[str, int]


class AdmissionController:
    """Bound concurrent local work so expensive requests cannot OOM the host."""

    def __init__(
        self, lightweight_workers: int, query_workers: int, bulk_workers: int
    ) -> None:
        self._semaphores = {
            "lightweight": asyncio.Semaphore(lightweight_workers),
            "query": asyncio.Semaphore(query_workers),
            "bulk_query": asyncio.Semaphore(bulk_workers),
            "bulk_ingest": asyncio.Semaphore(bulk_workers),
        }
        self._active = {kind: 0 for kind in self._semaphores}
        self._queued = {kind: 0 for kind in self._semaphores}

    @asynccontextmanager
    async def slot(self, kind: OperationKind) -> AsyncIterator[None]:
        semaphore = self._semaphores[kind]
        self._queued[kind] += 1
        try:
            await semaphore.acquire()
        finally:
            self._queued[kind] -= 1
        self._active[kind] += 1
        try:
            yield
        finally:
            self._active[kind] -= 1
            semaphore.release()

    def snapshot(self) -> AdmissionSnapshot:
        return AdmissionSnapshot(dict(self._active), dict(self._queued))


def classify_operation(path: str, api_version: str) -> OperationKind:
    query_prefix = f"/services/data/{api_version}/query"
    if path.startswith(f"/services/data/{api_version}/jobs/query"):
        return "bulk_query"
    if path.startswith(f"/services/data/{api_version}/jobs/ingest"):
        return "bulk_ingest"
    if path.startswith(query_prefix) or path.endswith("/deleted"):
        return "query"
    return "lightweight"
