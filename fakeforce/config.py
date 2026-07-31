"""Runtime configuration for FakeForce's disk-backed architecture.

Configuration is read once at process startup.  Keeping it separate from the
HTTP module makes resource limits testable and prevents environment lookups
from being scattered through request handlers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


ROOT = Path(__file__).resolve().parents[1]


def _positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero, got {parsed}")
    return parsed


@dataclass(frozen=True)
class Settings:
    """Validated configuration needed before query execution starts."""

    seed_directory: Path
    catalog_path: Path
    data_roots: tuple[Path, ...]
    state_directory: Path
    bulk_query_results_directory: Path
    memory_limit: str
    temp_directory: Path
    max_temp_size: str
    disk_reserve_bytes: int
    scan_batch_rows: int
    batch_readahead: int
    fragment_readahead: int
    lightweight_request_workers: int
    heavy_query_workers: int
    bulk_workers: int
    bulk_result_part_records: int
    bulk_queue_delay_ms: int
    bulk_job_retention_hours: int
    bulk_ingest_max_upload_bytes: int
    bulk_ingest_batch_records: int
    sync_query_timeout_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        values = os.environ if env is None else env
        state_directory = Path(values.get("FAKEFORCE_STATE_DIR", ROOT / "state"))
        seed_directory = Path(values.get("FAKEFORCE_SEED_DIR", ROOT / "seed"))
        data_roots = tuple(
            Path(value)
            for value in values.get("FAKEFORCE_DATA_ROOTS", str(seed_directory)).split(os.pathsep)
            if value
        )
        return cls(
            seed_directory=seed_directory,
            catalog_path=Path(
                values.get("FAKEFORCE_CATALOG_PATH", ROOT / "fakeforce" / "catalog.json")
            ),
            data_roots=data_roots,
            state_directory=state_directory,
            bulk_query_results_directory=Path(
                values.get(
                    "FAKEFORCE_BULK_QUERY_RESULTS_DIR", state_directory / "bulk" / "query"
                )
            ),
            memory_limit=values.get("FAKEFORCE_MEMORY_LIMIT", "4GB"),
            temp_directory=Path(
                values.get("FAKEFORCE_TEMP_DIRECTORY", state_directory / "spill")
            ),
            max_temp_size=values.get("FAKEFORCE_MAX_TEMP_SIZE", "100GB"),
            disk_reserve_bytes=_positive_int(
                values.get("FAKEFORCE_DISK_RESERVE_BYTES", str(5 * 1024**3)),
                "FAKEFORCE_DISK_RESERVE_BYTES",
            ),
            scan_batch_rows=_positive_int(
                values.get("FAKEFORCE_SCAN_BATCH_ROWS", "16384"),
                "FAKEFORCE_SCAN_BATCH_ROWS",
            ),
            batch_readahead=_positive_int(
                values.get("FAKEFORCE_BATCH_READAHEAD", "2"),
                "FAKEFORCE_BATCH_READAHEAD",
            ),
            fragment_readahead=_positive_int(
                values.get("FAKEFORCE_FRAGMENT_READAHEAD", "1"),
                "FAKEFORCE_FRAGMENT_READAHEAD",
            ),
            lightweight_request_workers=_positive_int(
                values.get("FAKEFORCE_LIGHTWEIGHT_REQUEST_WORKERS", "8"),
                "FAKEFORCE_LIGHTWEIGHT_REQUEST_WORKERS",
            ),
            heavy_query_workers=_positive_int(
                values.get("FAKEFORCE_HEAVY_QUERY_WORKERS", "1"),
                "FAKEFORCE_HEAVY_QUERY_WORKERS",
            ),
            bulk_workers=_positive_int(
                values.get("FAKEFORCE_BULK_WORKERS", "1"),
                "FAKEFORCE_BULK_WORKERS",
            ),
            bulk_result_part_records=_positive_int(
                values.get("FAKEFORCE_BULK_RESULT_PART_RECORDS", "100000"),
                "FAKEFORCE_BULK_RESULT_PART_RECORDS",
            ),
            bulk_queue_delay_ms=_positive_int(
                values.get("FAKEFORCE_BULK_QUEUE_DELAY_MS", "250"),
                "FAKEFORCE_BULK_QUEUE_DELAY_MS",
            ),
            bulk_job_retention_hours=_positive_int(
                values.get("FAKEFORCE_BULK_JOB_RETENTION_HOURS", "168"),
                "FAKEFORCE_BULK_JOB_RETENTION_HOURS",
            ),
            bulk_ingest_max_upload_bytes=_positive_int(
                values.get("FAKEFORCE_BULK_INGEST_MAX_UPLOAD_BYTES", str(1024**3)),
                "FAKEFORCE_BULK_INGEST_MAX_UPLOAD_BYTES",
            ),
            bulk_ingest_batch_records=_positive_int(
                values.get("FAKEFORCE_BULK_INGEST_BATCH_RECORDS", "1000"),
                "FAKEFORCE_BULK_INGEST_BATCH_RECORDS",
            ),
            sync_query_timeout_seconds=_positive_int(
                values.get("FAKEFORCE_SYNC_QUERY_TIMEOUT_SECONDS", "120"),
                "FAKEFORCE_SYNC_QUERY_TIMEOUT_SECONDS",
            ),
        )
