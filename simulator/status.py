"""Print the durable daily-simulation status as JSON."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-directory", type=Path, default=Path("state"))
    args = parser.parse_args()
    state = SimulationStateStore(
        args.state_directory / "simulation" / "simulation.duckdb", SimulationPolicy()
    )
    print(json.dumps(state.status_snapshot(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
