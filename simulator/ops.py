"""Plan SLA-gated Ops orders from CRM Closed Won events."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy


@dataclass(frozen=True)
class OpsSnapshot:
    next_order_id: int


class OpsDailyPlanner:
    def __init__(self, policy: SimulationPolicy) -> None:
        self.policy = policy

    def plan(
        self,
        run_date: date,
        crm_events: Iterable[SourceEvent],
        already_converted_opportunities: set[str],
        snapshot: OpsSnapshot,
    ) -> list[SourceEvent]:
        eligible: list[tuple[SourceEvent, int, str | None]] = []
        for event in crm_events:
            if event.event_type != "opportunity_stage_changed" or event.payload.get("StageName") != "Closed Won":
                continue
            if event.entity_id in already_converted_opportunities:
                continue
            delay, anomaly = self._conversion_delay(event)
            if event.business_date + timedelta(days=delay) <= run_date:
                eligible.append((event, delay, anomaly))

        orders: list[SourceEvent] = []
        for offset, (event, delay, anomaly) in enumerate(sorted(eligible, key=lambda item: item[0].event_id)):
            account_id = str(event.payload.get("AccountId", ""))
            if not account_id.startswith("001") or not account_id[3:].isdigit():
                continue
            order_id = snapshot.next_order_id + offset
            orders.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="ops",
                    event_type="order_created",
                    entity_id=str(order_id),
                    payload={
                        "order_id": order_id,
                        "customer_id": int(account_id[3:]),
                        "opportunity_ref": event.entity_id,
                        "order_date": run_date.isoformat(),
                        "status": "PENDING",
                        "sales_channel": "DIRECT",
                        "crm_closed_won_date": event.business_date.isoformat(),
                        "crm_to_ops_delay_days": delay,
                    },
                    anomaly_type=anomaly,
                )
            )
        return orders

    def _conversion_delay(self, event: SourceEvent) -> tuple[int, str | None]:
        digest = hashlib.sha256(f"ops:{event.event_id}".encode()).digest()
        span = self.policy.crm_to_ops.maximum - self.policy.crm_to_ops.minimum + 1
        delay = self.policy.crm_to_ops.minimum + digest[0] % span
        delayed = digest[1] / 255 < self.policy.anomalies.delayed_crm_to_ops
        if delayed:
            return self.policy.crm_to_ops.maximum + 1, "delayed_crm_to_ops"
        return delay, None
