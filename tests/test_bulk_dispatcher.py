from __future__ import annotations

import time

from fakeforce.bulk.dispatcher import BulkQueryDispatcher
from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType
from fakeforce.config import Settings
from fakeforce.state import StateStore


class RecordingWorker:
    def __init__(self) -> None:
        self.jobs: list[str] = []

    def run(self, job_id: str) -> None:
        self.jobs.append(job_id)


def test_dispatcher_runs_resumable_job_asynchronously(tmp_path) -> None:
    settings = Settings.from_env(
        {"FAKEFORCE_STATE_DIR": str(tmp_path / "state"), "FAKEFORCE_BULK_QUEUE_DELAY_MS": "1"}
    )
    state = StateStore.from_settings(settings)
    state.initialize()
    state.create_job(
        BulkJob("job-1", BulkJobType.QUERY, "v60.0", BulkJobState.UPLOAD_COMPLETE,
                None, "query", "SELECT Id FROM Account", "snapshot")
    )
    worker = RecordingWorker()
    dispatcher = BulkQueryDispatcher(settings, state, worker)  # type: ignore[arg-type]

    dispatcher.resume_pending()
    for _ in range(50):
        if worker.jobs:
            break
        time.sleep(0.01)

    assert worker.jobs == ["job-1"]
