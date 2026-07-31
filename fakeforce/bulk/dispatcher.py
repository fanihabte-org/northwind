"""Bounded asynchronous dispatch for durable Bulk Query jobs."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from fakeforce.bulk.jobs import BulkJobState
from fakeforce.bulk.ingest import BulkIngestWorker
from fakeforce.bulk.query import BulkQueryWorker
from fakeforce.config import Settings
from fakeforce.state import StateStore


class BulkQueryDispatcher:
    def __init__(self, settings: Settings, state_store: StateStore, worker: BulkQueryWorker) -> None:
        self.settings = settings
        self.state_store = state_store
        self.worker = worker
        self.executor = ThreadPoolExecutor(
            max_workers=settings.bulk_workers, thread_name_prefix="fakeforce-bulk-query"
        )

    def submit(self, job_id: str) -> None:
        self.executor.submit(self._run, job_id)

    def resume_pending(self) -> None:
        for job_id in self.state_store.resumable_query_jobs():
            self.submit(job_id)

    def _run(self, job_id: str) -> None:
        time.sleep(self.settings.bulk_queue_delay_ms / 1000)
        job = self.state_store.get_job(job_id)
        if job is None or job.state not in {BulkJobState.UPLOAD_COMPLETE, BulkJobState.IN_PROGRESS}:
            return
        try:
            self.worker.run(job_id)
        except Exception:
            # The worker persists its failure state; the detached future must
            # not leak an exception into the request that submitted the job.
            return


class BulkIngestDispatcher:
    """Schedules ingest through the same bounded executor as Bulk Query."""

    def __init__(
        self,
        settings: Settings,
        state_store: StateStore,
        worker: BulkIngestWorker,
        executor: ThreadPoolExecutor,
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.worker = worker
        self.executor = executor

    def submit(self, job_id: str) -> None:
        self.executor.submit(self._run, job_id)

    def resume_pending(self) -> None:
        for job_id in self.state_store.resumable_ingest_jobs():
            self.submit(job_id)

    def _run(self, job_id: str) -> None:
        time.sleep(self.settings.bulk_queue_delay_ms / 1000)
        job = self.state_store.get_job(job_id)
        if job is None or job.state not in {BulkJobState.UPLOAD_COMPLETE, BulkJobState.IN_PROGRESS}:
            return
        try:
            self.worker.run(job_id)
        except Exception:
            return
