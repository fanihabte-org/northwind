from datetime import date

import pytest

from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateError, SimulationStateStore


def test_state_catches_up_each_missing_date_in_order(tmp_path) -> None:
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 24)
    first_missing = date(2026, 7, 25)
    second_missing = date(2026, 7, 26)
    store.initialize(baseline)

    assert store.due_dates(second_missing) == [first_missing, second_missing]
    assert store.claim_run(first_missing, seed=42)
    for source in ("crm", "ops", "erp"):
        store.mark_source_applied(first_missing, source, 3, 3)
    store.complete_run(first_missing)

    assert store.last_completed_date() == first_missing
    assert store.baseline_completed_date() == baseline
    assert store.due_dates(second_missing) == [second_missing]


def test_state_restart_keeps_immutable_baseline_after_watermark_advances(tmp_path) -> None:
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 24)
    completed = date(2026, 7, 25)
    store.initialize(baseline)
    store.claim_run(completed, seed=42)
    for source in ("crm", "ops", "erp"):
        store.mark_source_applied(completed, source, 0, 0)
    store.complete_run(completed)

    store.initialize(baseline)

    assert store.baseline_completed_date() == baseline
    assert store.last_completed_date() == completed
    with pytest.raises(SimulationStateError, match="different baseline"):
        store.initialize(date(2026, 7, 23))


def test_state_upgrades_legacy_watermark_without_rewinding_it(tmp_path) -> None:
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 24)
    advanced_watermark = date(2026, 8, 3)
    store.initialize(baseline)
    with store.connection() as connection:
        connection.execute("DELETE FROM simulation_metadata WHERE key = 'baseline_completed_date'")
        connection.execute(
            "UPDATE simulation_metadata SET value = ? WHERE key = 'last_completed_date'",
            [advanced_watermark.isoformat()],
        )

    store.initialize(baseline)

    assert store.baseline_completed_date() == baseline
    assert store.last_completed_date() == advanced_watermark


def test_interrupted_run_is_reclaimed_without_advancing_the_watermark(tmp_path) -> None:
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    baseline = date(2026, 7, 24)
    missing = date(2026, 7, 25)
    store.initialize(baseline)
    store.claim_run(missing, seed=42)
    store.mark_source_applied(missing, "crm", 4, 4)

    assert store.last_completed_date() == baseline
    assert store.claim_run(missing, seed=42)
    assert store.source_run_state(missing, "crm") == "applied"


def test_state_rejects_out_of_order_source_and_date_completion(tmp_path) -> None:
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    first_missing = date(2026, 7, 25)
    second_missing = date(2026, 7, 26)
    store.initialize(date(2026, 7, 24))
    store.claim_run(first_missing, seed=42)

    with pytest.raises(SimulationStateError, match="CRM, Ops, ERP"):
        store.mark_source_applied(first_missing, "ops", 1, 1)
    with pytest.raises(SimulationStateError, match="claimed in order"):
        store.claim_run(second_missing, seed=42)
