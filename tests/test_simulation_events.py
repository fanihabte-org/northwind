from datetime import date, timedelta

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateStore


def test_events_have_stable_identity_and_preserve_anomaly_metadata() -> None:
    arguments = dict(
        business_date=date(2026, 7, 25),
        source_system="crm",
        event_type="opportunity_stage_changed",
        entity_id="006000000000000001",
        payload={"StageName": "Closed Won"},
        available_on=date(2026, 7, 27),
        anomaly_type="long_open_opportunity",
    )

    first = SourceEvent.create(**arguments)
    second = SourceEvent.create(**arguments)

    assert first.event_id == second.event_id
    assert first.available_on == date(2026, 7, 27)
    assert first.anomaly_type == "long_open_opportunity"


def test_event_ledger_is_idempotent_for_a_reclaimed_run(tmp_path) -> None:
    run_date = date(2026, 7, 25)
    store = SimulationStateStore(tmp_path / "simulator.duckdb", SimulationPolicy())
    store.initialize(run_date - timedelta(days=1))
    store.claim_run(run_date, seed=42)
    event = SourceEvent.create(
        business_date=run_date,
        source_system="crm",
        event_type="account_created",
        entity_id="001000000000000001",
        payload={"Name": "Example Co"},
    )

    store.record_events([event])
    store.record_events([event])

    assert store.event_count(run_date, "crm") == 1
