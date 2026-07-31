"""Internal observability snapshots for local-memory debugging."""

from __future__ import annotations

import os
import resource
import sys
from pathlib import Path

import pyarrow as pa

from fakeforce.config import Settings
from fakeforce.runtime import AdmissionController
from fakeforce.state import StateStore


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def process_rss_bytes() -> int:
    try:
        with open("/proc/self/status") as stream:
            for line in stream:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


class Diagnostics:
    def __init__(
        self, settings: Settings, state_store: StateStore, runtime: AdmissionController
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.runtime = runtime

    def memory(self) -> dict[str, int]:
        with self.state_store.connection() as connection:
            duckdb_memory, duckdb_spill = connection.execute(
                """
                SELECT coalesce(sum(memory_usage_bytes), 0),
                       coalesce(sum(temporary_storage_bytes), 0)
                FROM duckdb_memory()
                """
            ).fetchone()
        return {
            "process_rss_bytes": process_rss_bytes(),
            "duckdb_memory_bytes": duckdb_memory,
            "duckdb_spill_bytes": duckdb_spill,
            "temporary_directory_bytes": directory_size(self.settings.temp_directory),
            "cursor_artifact_bytes": directory_size(
                self.settings.state_directory / "artifacts" / "cursors"
            ),
            "arrow_allocated_bytes": pa.total_allocated_bytes(),
        }

    def queries(self) -> dict[str, int]:
        admission = self.runtime.snapshot()
        return {
            "active_sync_queries": admission.active["query"],
            "queued_sync_queries": admission.queued["query"],
            "active_lightweight_requests": admission.active["lightweight"],
            "queued_lightweight_requests": admission.queued["lightweight"],
            "open_cursors": self.state_store.count_open_cursors(),
        }

    def jobs(self) -> dict[str, int]:
        with self.state_store.connection() as connection:
            active_jobs, queued_jobs = connection.execute(
                """
                SELECT count(*) FILTER (WHERE state IN ('InProgress', 'UploadComplete')),
                       count(*) FILTER (WHERE state = 'Open')
                FROM jobs
                """
            ).fetchone()
        return {"active_bulk_jobs": active_jobs, "queued_bulk_jobs": queued_jobs}
