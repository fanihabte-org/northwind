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
        openings = [
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
        resolved_case_ids = {
            int(event.payload["case_id"])
            for event in events
            if event.event_type == "support_case_resolved"
        }
        resolutions = []
        for opening in [*events, *openings]:
            if opening.event_type != "support_case_opened":
                continue
            case_id = int(opening.payload["case_id"])
            if case_id in resolved_case_ids:
                continue
            if opening.business_date + timedelta(days=self._resolution_delay(opening)) > run_date:
                continue
            resolutions.append(SourceEvent.create(
                business_date=run_date, source_system="ops", event_type="support_case_resolved",
                entity_id=str(case_id), payload={"case_id": case_id, "priority": opening.payload["priority"]},
            ))
        closed_case_ids = {
            int(event.payload["case_id"])
            for event in events
            if event.event_type == "support_case_closed"
        }
        closures = []
        for resolution in events:
            if resolution.event_type != "support_case_resolved":
                continue
            case_id = int(resolution.payload["case_id"])
            if case_id in closed_case_ids:
                continue
            if resolution.business_date + timedelta(days=self._closure_delay(resolution)) > run_date:
                continue
            closures.append(SourceEvent.create(
                business_date=run_date, source_system="ops", event_type="support_case_closed",
                entity_id=str(case_id), payload={"case_id": case_id, "priority": resolution.payload["priority"]},
            ))
        return openings + resolutions + closures

    @staticmethod
    def _opens_case(delivery: SourceEvent) -> bool:
        return hashlib.sha256(f"support:{delivery.event_id}".encode()).digest()[0] / 255 < 0.02

    @staticmethod
    def _opening_delay(delivery: SourceEvent) -> int:
        return 1 + hashlib.sha256(f"support-delay:{delivery.event_id}".encode()).digest()[0] % 7

    @staticmethod
    def _resolution_delay(opening: SourceEvent) -> int:
        return 1 + hashlib.sha256(f"support-resolution:{opening.event_id}".encode()).digest()[0] % 3

    @staticmethod
    def _closure_delay(resolution: SourceEvent) -> int:
        return 1 + hashlib.sha256(f"support-close:{resolution.event_id}".encode()).digest()[0] % 2
