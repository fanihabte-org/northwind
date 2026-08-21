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
