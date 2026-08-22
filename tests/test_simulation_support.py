from datetime import date

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy
from simulator.support import SupportCasePlanner, SupportSnapshot


def test_support_case_planner_is_deterministic_and_does_not_reopen_a_shipment() -> None:
    planner = SupportCasePlanner(SimulationPolicy())
    deliveries = [
        SourceEvent.create(
            business_date=date(2026, 7, 1), source_system="ops", event_type="shipment_delivered",
            entity_id=str(shipment_id), payload={"shipment_id": shipment_id, "order_id": shipment_id},
        )
        for shipment_id in range(1, 400)
    ]
    first = planner.plan(date(2026, 7, 10), deliveries, SupportSnapshot(900))
    assert first
    assert first == planner.plan(date(2026, 7, 10), deliveries, SupportSnapshot(900))
    existing = [*deliveries, first[0]]
    repeated = planner.plan(date(2026, 7, 10), existing, SupportSnapshot(900))
    assert first[0].payload["shipment_id"] not in {
        event.payload["shipment_id"] for event in repeated
    }


def test_support_case_planner_schedules_each_resolution_once() -> None:
    planner = SupportCasePlanner(SimulationPolicy())
    opening = SourceEvent.create(
        business_date=date(2026, 7, 1), source_system="ops", event_type="support_case_opened",
        entity_id="900", payload={"case_id": 900, "shipment_id": 500, "order_id": 100, "priority": "P3"},
    )
    planned = planner.plan(date(2026, 7, 10), [opening], SupportSnapshot(901))
    resolution = next(event for event in planned if event.event_type == "support_case_resolved")
    assert resolution.payload["case_id"] == 900
    assert not planner.plan(date(2026, 7, 10), [opening, resolution], SupportSnapshot(901))


def test_support_case_planner_schedules_each_closure_after_resolution() -> None:
    planner = SupportCasePlanner(SimulationPolicy())
    resolution = SourceEvent.create(
        business_date=date(2026, 7, 1), source_system="ops", event_type="support_case_resolved",
        entity_id="900", payload={"case_id": 900, "priority": "P3"},
    )
    planned = planner.plan(date(2026, 7, 10), [resolution], SupportSnapshot(901))
    closure = next(event for event in planned if event.event_type == "support_case_closed")
    assert closure.payload["case_id"] == 900
    assert not planner.plan(date(2026, 7, 10), [resolution, closure], SupportSnapshot(901))
