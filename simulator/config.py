"""Resource settings for the simulator's bounded DuckDB snapshot reads."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


def _positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero, got {parsed}")
    return parsed


@dataclass(frozen=True)
class SimulatorDuckDBSettings:
    """Limits applied to each short-lived CRM snapshot connection.

    The simulator performs metadata queries and bounded samples; it must never
    claim all memory available to Docker while doing so.  DuckDB spills sort
    and window-operation intermediates to the durable state mount instead.
    """

    memory_limit: str
    temp_directory: Path
    max_temp_size: str
    threads: int

    @classmethod
    def from_env(
        cls,
        state_directory: Path,
        env: Mapping[str, str] | None = None,
    ) -> "SimulatorDuckDBSettings":
        values = os.environ if env is None else env
        return cls(
            memory_limit=values.get("SIMULATION_DUCKDB_MEMORY_LIMIT", "512MB"),
            temp_directory=Path(
                values.get("SIMULATION_DUCKDB_TEMP_DIRECTORY", state_directory / "simulator-spill")
            ),
            max_temp_size=values.get("SIMULATION_DUCKDB_MAX_TEMP_SIZE", "20GB"),
            threads=_positive_int(values.get("SIMULATION_DUCKDB_THREADS", "1"), "SIMULATION_DUCKDB_THREADS"),
        )

    def duckdb_config(self) -> dict[str, str]:
        return {
            "memory_limit": self.memory_limit,
            "temp_directory": str(self.temp_directory),
            "max_temp_directory_size": self.max_temp_size,
            "threads": str(self.threads),
            # Ordering guarantees consume memory and are unnecessary for the
            # deterministic SQL ordering used by the simulator itself.
            "preserve_insertion_order": "false",
        }
