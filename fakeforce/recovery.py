"""Restart reconciliation for durable FakeForce cursor state."""

from __future__ import annotations

from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.state import StateStore
from typing import Protocol


class BulkArtifactStore(Protocol):
    def remove_job(self, job_id: str) -> None: ...


def recover_cursor_artifacts(state_store: StateStore, artifacts: CursorArtifactStore) -> None:
    """Delete only expired/orphaned cursor files after durable state is available."""
    artifacts.reconcile(state_store.expire_cursors(), state_store.active_cursor_artifacts())


def recover_bulk_artifacts(state_store: StateStore, *artifacts: BulkArtifactStore) -> None:
    """Remove every artifact class owned by jobs whose retention has ended."""
    for job_id in state_store.expire_jobs():
        for store in artifacts:
            store.remove_job(job_id)
