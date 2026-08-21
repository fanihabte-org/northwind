"""Plan delayed, deterministic support-case openings after deliveries."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy


@dataclass(frozen=True)
class SupportSnapshot:
    next_case_id: int


class SupportCasePlanner:
    """Create a small, proportional set of support cases from completed deliveries."""

    def __init__(self, policy: SimulationPolicy) -> None:
        self.policy = policy

    def plan(
        self, run_date: date, ops_events: Iterable[SourceEvent], snapshot: SupportSnapshot
    ) -> list[SourceEvent]:
        events = list(ops_events)
        already_opened = {
            int(event.payload["shipment_id"])
            for event in events
            if event.event_type == "support_case_opened"
        }
        eligible = [
            event
            for event in events
            if event.event_type == "shipment_delivered"
            and int(event.payload["shipment_id"]) not in already_opened
            and self._opens_case(event)
            and event.business_date + timedelta(days=self._opening_delay(event)) <= run_date
        ]
        return [
            SourceEvent.create(
                business_date=run_date,
                source_system="ops",
                event_type="support_case_opened",
                entity_id=str(snapshot.next_case_id + offset),
                payload={
                    "case_id": snapshot.next_case_id + offset,
                    "shipment_id": int(delivery.payload["shipment_id"]),
                    "order_id": int(delivery.payload["order_id"]),
                    "priority": "P3",
                },
            )
            for offset, delivery in enumerate(sorted(eligible, key=lambda event: event.event_id))
        ]

    @staticmethod
    def _opens_case(delivery: SourceEvent) -> bool:
        return hashlib.sha256(f"support:{delivery.event_id}".encode()).digest()[0] / 255 < 0.02

    @staticmethod
    def _opening_delay(delivery: SourceEvent) -> int:
        return 1 + hashlib.sha256(f"support-delay:{delivery.event_id}".encode()).digest()[0] % 7
