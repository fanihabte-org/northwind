from __future__ import annotations

import pytest

from fakeforce.config import Settings


def test_settings_have_local_safe_defaults() -> None:
    settings = Settings.from_env({})

    assert settings.memory_limit == "4GB"
    assert settings.max_temp_size == "100GB"
    assert settings.scan_batch_rows == 16_384
    assert settings.lightweight_request_workers == 8
    assert settings.bulk_result_part_records == 100_000
    assert settings.bulk_queue_delay_ms == 250
    assert settings.bulk_job_retention_hours == 168
    assert settings.bulk_ingest_max_upload_bytes == 1024**3
    assert settings.bulk_ingest_batch_records == 1000
    assert settings.heavy_query_workers == 1
    assert settings.sync_query_timeout_seconds == 120
    assert settings.temp_directory == settings.state_directory / "spill"
    assert settings.bulk_query_results_directory == settings.state_directory / "bulk" / "query"
    assert settings.data_roots == (settings.seed_directory,)


def test_settings_allow_explicit_state_and_resource_overrides() -> None:
    settings = Settings.from_env(
        {
            "FAKEFORCE_STATE_DIR": "/tmp/fakeforce-state",
            "FAKEFORCE_MEMORY_LIMIT": "2GB",
            "FAKEFORCE_TEMP_DIRECTORY": "/tmp/fakeforce-spill",
            "FAKEFORCE_BULK_QUERY_RESULTS_DIR": "/tmp/fakeforce-exports",
            "FAKEFORCE_HEAVY_QUERY_WORKERS": "2",
        }
    )

    assert str(settings.state_directory) == "/tmp/fakeforce-state"
    assert str(settings.temp_directory) == "/tmp/fakeforce-spill"
    assert settings.memory_limit == "2GB"
    assert settings.heavy_query_workers == 2
    assert str(settings.bulk_query_results_directory) == "/tmp/fakeforce-exports"


@pytest.mark.parametrize("value", ["0", "-1", "not-a-number"])
def test_settings_reject_invalid_positive_integer_limits(value: str) -> None:
    with pytest.raises(ValueError, match="FAKEFORCE_SCAN_BATCH_ROWS"):
        Settings.from_env({"FAKEFORCE_SCAN_BATCH_ROWS": value})
