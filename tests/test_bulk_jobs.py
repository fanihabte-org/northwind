from __future__ import annotations

import pytest

from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType, InvalidJobTransition
from fakeforce.config import Settings
from fakeforce.state import StateStore


def test_bulk_job_transitions_are_durable_and_constrained(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)
    store.initialize()
    job = BulkJob(
        "job-1",
        BulkJobType.QUERY,
        "v60.0",
        BulkJobState.UPLOAD_COMPLETE,
        None,
        None,
        "SELECT Id FROM Account",
        "snapshot",
    )
    store.create_job(job)

    in_progress = store.transition_job("job-1", BulkJobState.IN_PROGRESS)
    complete = store.transition_job("job-1", BulkJobState.JOB_COMPLETE)

    assert in_progress.state == BulkJobState.IN_PROGRESS
    assert complete.state == BulkJobState.JOB_COMPLETE
    assert store.get_job("job-1") == complete
    with pytest.raises(InvalidJobTransition):
        store.transition_job("job-1", BulkJobState.ABORTED)
