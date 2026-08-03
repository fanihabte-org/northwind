from pathlib import Path

import pytest

from simulator.config import SimulatorDuckDBSettings


def test_simulator_duckdb_settings_derive_spill_directory_from_state() -> None:
    settings = SimulatorDuckDBSettings.from_env(
        Path("/var/northwind/state"),
        {
            "SIMULATION_DUCKDB_MEMORY_LIMIT": "768MB",
            "SIMULATION_DUCKDB_MAX_TEMP_SIZE": "12GB",
            "SIMULATION_DUCKDB_THREADS": "2",
        },
    )

    assert settings.memory_limit == "768MB"
    assert settings.temp_directory == Path("/var/northwind/state/simulator-spill")
    assert settings.max_temp_size == "12GB"
    assert settings.duckdb_config()["threads"] == "2"
    assert settings.duckdb_config()["preserve_insertion_order"] == "false"


def test_simulator_duckdb_settings_reject_non_positive_threads() -> None:
    with pytest.raises(ValueError, match="SIMULATION_DUCKDB_THREADS"):
        SimulatorDuckDBSettings.from_env(Path("/state"), {"SIMULATION_DUCKDB_THREADS": "0"})
