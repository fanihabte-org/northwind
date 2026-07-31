"""Durable Bulk API 2.0 job state model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from dataclasses import field
from enum import StrEnum


class BulkJobType(StrEnum):
    QUERY = "query"
    INGEST = "ingest"


class BulkJobState(StrEnum):
    OPEN = "Open"
    UPLOAD_COMPLETE = "UploadComplete"
    IN_PROGRESS = "InProgress"
    JOB_COMPLETE = "JobComplete"
    FAILED = "Failed"
    ABORTED = "Aborted"


class InvalidJobTransition(ValueError):
    pass


@dataclass(frozen=True)
class BulkJob:
    job_id: str
    job_type: BulkJobType
    api_version: str
    state: BulkJobState
    object_name: str | None
    operation: str | None
    query_text: str | None
    dataset_snapshot_id: str | None
    error_code: str | None = None
    error_message: str | None = None
    expires_at: datetime | None = None
    options: dict[str, str] = field(default_factory=dict)


_ALLOWED_TRANSITIONS = {
    BulkJobState.OPEN: {BulkJobState.UPLOAD_COMPLETE, BulkJobState.ABORTED},
    BulkJobState.UPLOAD_COMPLETE: {BulkJobState.IN_PROGRESS, BulkJobState.ABORTED},
    BulkJobState.IN_PROGRESS: {
        BulkJobState.JOB_COMPLETE,
        BulkJobState.FAILED,
        BulkJobState.ABORTED,
    },
    BulkJobState.JOB_COMPLETE: set(),
    BulkJobState.FAILED: set(),
    BulkJobState.ABORTED: set(),
}


def validate_transition(current: BulkJobState, next_state: BulkJobState) -> None:
    if next_state not in _ALLOWED_TRANSITIONS[current]:
        raise InvalidJobTransition(f"cannot transition bulk job from {current} to {next_state}")
