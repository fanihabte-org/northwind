from datetime import date

from simulator.events import SourceEvent
from simulator.fulfillment import FulfillmentPlanner, FulfillmentSnapshot
from simulator.policy import AnomalyRates, SimulationPolicy


def _order(order_id: int = 100) -> SourceEvent:
    return SourceEvent.create(
        business_date=date(2026, 7, 1),
        source_system="ops",
        event_type="order_created",
        entity_id=str(order_id),
        payload={"order_id": order_id},
    )


def test_fulfillment_creates_shipment_then_invoice_after_sla() -> None:
    planner = FulfillmentPlanner(SimulationPolicy())

    shipments = planner.plan(date(2026, 7, 10), [_order()], set(), {100}, FulfillmentSnapshot(500))
    shipment = next(event for event in shipments if event.event_type == "shipment_created")
    prior_shipment = SourceEvent.create(
        business_date=date(2026, 7, 1),
        source_system="ops",
        event_type="shipment_created",
        entity_id="500",
        payload={"shipment_id": 500, "order_id": 100},
    )
    invoices = planner.plan(
        date(2026, 7, 10), [_order(), prior_shipment], {100}, set(), FulfillmentSnapshot(501)
    )
    invoice = next(event for event in invoices if event.event_type == "invoice_created")

    assert shipment.payload["shipment_id"] == 500
    assert invoice.payload["order_id"] == 100
    assert invoice.business_date == date(2026, 7, 10)


def test_fulfillment_does_not_recreate_existing_order_steps() -> None:
    planner = FulfillmentPlanner(SimulationPolicy())

    events = planner.plan(date(2026, 7, 10), [_order()], {100}, {100}, FulfillmentSnapshot(500))

    assert events == []


def test_stalled_shipment_is_deterministic_and_delayed() -> None:
    policy = SimulationPolicy(anomalies=AnomalyRates(stalled_shipment=0.05))
    planner = FulfillmentPlanner(policy)
    orders = [_order(order_id) for order_id in range(1, 400)]

    events = planner.plan(date(2026, 7, 20), orders, set(), set(), FulfillmentSnapshot(1))

    stalled = [event for event in events if event.anomaly_type == "stalled_shipment"]
    assert stalled
    assert all(event.payload["order_to_shipment_delay_days"] > policy.order_to_shipment.maximum for event in stalled)
