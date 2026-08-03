from datetime import date

import pytest

from simulator.policy import SimulationPolicy
from simulator.scheduler import DailySimulationRunner
from simulator.state import SimulationStateStore


class Source:
    def __init__(self, name, state, calls, *, complete=False, fail=False):
        self.name = name
        self.state = state
        self.calls = calls
        self.complete = complete
        self.fail = fail

    def run(self, run_date, seed):
        self.calls.append((self.name, run_date, seed))
        if self.fail:
            raise RuntimeError(f"{self.name} failed")
        self.state.claim_run(run_date, seed)
        self.state.mark_source_applied(run_date, self.name, 0, 0)
        if self.complete:
            self.state.complete_run(run_date)


def test_scheduler_catches_up_each_missing_day_in_source_order(tmp_path) -> None:
    state = SimulationStateStore(tmp_path / "simulation.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 1)
    state.initialize(baseline)
    calls = []
    runner = DailySimulationRunner(
        state,
        Source("crm", state, calls),
        Source("ops", state, calls),
        Source("erp", state, calls, complete=True),
    )

    completed = runner.run_through(date(2026, 7, 3), seed=42)

    assert completed == [date(2026, 7, 2), date(2026, 7, 3)]
    assert [name for name, _, _ in calls] == ["crm", "ops", "erp", "crm", "ops", "erp"]
    assert state.last_completed_date() == date(2026, 7, 3)


def test_scheduler_stops_before_later_sources_when_a_step_fails(tmp_path) -> None:
    state = SimulationStateStore(tmp_path / "simulation.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 1)
    state.initialize(baseline)
    calls = []
    runner = DailySimulationRunner(
        state,
        Source("crm", state, calls),
        Source("ops", state, calls, fail=True),
        Source("erp", state, calls, complete=True),
    )

    with pytest.raises(RuntimeError, match="ops failed"):
        runner.run_through(date(2026, 7, 3), seed=42)

    assert [name for name, _, _ in calls] == ["crm", "ops"]
    assert state.last_completed_date() == baseline
