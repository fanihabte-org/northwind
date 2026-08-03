"""Internal observability snapshots for local-memory debugging."""

from __future__ import annotations

import os
import resource
import shutil
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


def filesystem_usage(path: Path):
    """Get usage without creating an empty configured artifact directory."""
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return shutil.disk_usage(candidate)


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

    def disk(self) -> dict[str, int]:
        """Report durable storage consumption and remaining capacity.

        The state and Bulk Query export directories may intentionally be on
        different volumes, so report free capacity for each instead of
        assuming one value applies to both.
        """
        state_usage = filesystem_usage(self.settings.state_directory)
        export_usage = filesystem_usage(self.settings.bulk_query_results_directory)
        return {
            "state_directory_bytes": directory_size(self.settings.state_directory),
            "bulk_query_export_bytes": directory_size(
                self.settings.bulk_query_results_directory
            ),
            "state_filesystem_free_bytes": state_usage.free,
            "bulk_export_filesystem_free_bytes": export_usage.free,
            "disk_reserve_bytes": self.settings.disk_reserve_bytes,
        }

    def jobs(self) -> dict[str, int | dict[str, int]]:
        with self.state_store.connection() as connection:
            active_jobs, queued_jobs = connection.execute(
                """
                SELECT count(*) FILTER (WHERE state IN ('InProgress', 'UploadComplete')),
                       count(*) FILTER (WHERE state = 'Open')
                FROM jobs
                """
            ).fetchone()
            state_counts = {
                state: count
                for state, count in connection.execute(
                    "SELECT state, count(*) FROM jobs GROUP BY state ORDER BY state"
                ).fetchall()
            }
            checkpointed_jobs = connection.execute(
                "SELECT count(DISTINCT job_id) FROM job_checkpoints"
            ).fetchone()[0]
            checkpointed_bytes = connection.execute(
                "SELECT coalesce(sum(bytes_written), 0) FROM job_checkpoints"
            ).fetchone()[0]
        return {
            "active_bulk_jobs": active_jobs,
            "queued_bulk_jobs": queued_jobs,
            "jobs_by_state": state_counts,
            "checkpointed_bulk_jobs": checkpointed_jobs,
            "checkpointed_output_bytes": checkpointed_bytes,
            "bulk_job_retention_hours": self.settings.bulk_job_retention_hours,
        }

    def overview(self, *, api_calls_last_24_hours: int) -> dict[str, object]:
        """One stable, low-cost snapshot for an operator or monitoring probe."""
        return {
            "memory": self.memory(),
            "disk": self.disk(),
            "queries": self.queries(),
            "jobs": self.jobs(),
            "api_calls_last_24_hours": api_calls_last_24_hours,
        }
