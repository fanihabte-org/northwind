"""Plan the delayed Ops fulfillment steps between order and ERP posting."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy


@dataclass(frozen=True)
class FulfillmentSnapshot:
    next_shipment_id: int


class FulfillmentPlanner:
    """Create deterministic shipment and invoice events without mutating Ops."""

    def __init__(self, policy: SimulationPolicy) -> None:
        self.policy = policy

    def plan(
        self,
        run_date: date,
        ops_events: Iterable[SourceEvent],
        shipped_order_ids: set[int],
        invoiced_order_ids: set[int],
        delivered_shipment_ids: set[int],
        snapshot: FulfillmentSnapshot,
    ) -> list[SourceEvent]:
        events = list(ops_events)
        shipments = self._shipments(run_date, events, shipped_order_ids, snapshot)
        invoices = self._invoices(
            run_date, events, shipments, invoiced_order_ids
        )
        deliveries = self._deliveries(
            run_date, [*events, *shipments], delivered_shipment_ids
        )
        return shipments + invoices + deliveries

    def _shipments(
        self,
        run_date: date,
        events: Iterable[SourceEvent],
        shipped_order_ids: set[int],
        snapshot: FulfillmentSnapshot,
    ) -> list[SourceEvent]:
        eligible: list[tuple[SourceEvent, int, str | None]] = []
        for event in events:
            if event.event_type != "order_created":
                continue
            order_id = int(event.payload["order_id"])
            if order_id in shipped_order_ids:
                continue
            delay, anomaly = self._shipment_delay(event)
            if event.business_date + timedelta(days=delay) <= run_date:
                eligible.append((event, delay, anomaly))

        shipments: list[SourceEvent] = []
        for offset, (order, delay, anomaly) in enumerate(sorted(eligible, key=lambda item: item[0].event_id)):
            order_id = int(order.payload["order_id"])
            shipments.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="ops",
                    event_type="shipment_created",
                    entity_id=str(snapshot.next_shipment_id + offset),
                    payload={
                        "shipment_id": snapshot.next_shipment_id + offset,
                        "order_id": order_id,
                        "order_created_date": order.business_date.isoformat(),
                        "order_to_shipment_delay_days": delay,
                    },
                    anomaly_type=anomaly,
                )
            )
        return shipments

    def _invoices(
        self,
        run_date: date,
        existing_events: Iterable[SourceEvent],
        new_shipments: Iterable[SourceEvent],
        invoiced_order_ids: set[int],
    ) -> list[SourceEvent]:
        shipments = [
            event
            for event in [*existing_events, *new_shipments]
            if event.event_type == "shipment_created"
        ]
        invoices: list[SourceEvent] = []
        for shipment in sorted(shipments, key=lambda event: event.event_id):
            order_id = int(shipment.payload["order_id"])
            if order_id in invoiced_order_ids:
                continue
            delay = self._invoice_delay(shipment)
            if shipment.business_date + timedelta(days=delay) > run_date:
                continue
            invoices.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="ops",
                    event_type="invoice_created",
                    entity_id=str(order_id),
                    payload={
                        "order_id": order_id,
                        "shipment_id": int(shipment.payload["shipment_id"]),
                        "shipment_date": shipment.business_date.isoformat(),
                        "shipment_to_invoice_delay_days": delay,
                    },
                )
            )
        return invoices

    def _deliveries(
        self,
        run_date: date,
        events: Iterable[SourceEvent],
        delivered_shipment_ids: set[int],
    ) -> list[SourceEvent]:
        deliveries: list[SourceEvent] = []
        for shipment in sorted(
            (event for event in events if event.event_type == "shipment_created"),
            key=lambda event: event.event_id,
        ):
            shipment_id = int(shipment.payload["shipment_id"])
            if shipment_id in delivered_shipment_ids:
                continue
            delay = self._delivery_delay(shipment)
            if shipment.business_date + timedelta(days=delay) > run_date:
                continue
            deliveries.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="ops",
                    event_type="shipment_delivered",
                    entity_id=str(shipment_id),
                    payload={
                        "shipment_id": shipment_id,
                        "order_id": int(shipment.payload["order_id"]),
                        "shipment_date": shipment.business_date.isoformat(),
                        "shipment_to_delivery_delay_days": delay,
                    },
                )
            )
        return deliveries

    def _shipment_delay(self, order: SourceEvent) -> tuple[int, str | None]:
        digest = hashlib.sha256(f"shipment:{order.event_id}".encode()).digest()
        span = self.policy.order_to_shipment.maximum - self.policy.order_to_shipment.minimum + 1
        delay = self.policy.order_to_shipment.minimum + digest[0] % span
        stalled = digest[1] / 255 < self.policy.anomalies.stalled_shipment
        if stalled:
            return self.policy.order_to_shipment.maximum + 3, "stalled_shipment"
        return delay, None

    def _invoice_delay(self, shipment: SourceEvent) -> int:
        digest = hashlib.sha256(f"invoice:{shipment.event_id}".encode()).digest()
        span = self.policy.shipment_to_invoice.maximum - self.policy.shipment_to_invoice.minimum + 1
        return self.policy.shipment_to_invoice.minimum + digest[0] % span

    def _delivery_delay(self, shipment: SourceEvent) -> int:
        digest = hashlib.sha256(f"delivery:{shipment.event_id}".encode()).digest()
        span = self.policy.shipment_to_delivery.maximum - self.policy.shipment_to_delivery.minimum + 1
        return self.policy.shipment_to_delivery.minimum + digest[0] % span
