"""Serializable daily business events shared by CRM, Ops, and ERP planners."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Mapping


SourceSystem = Literal["crm", "ops", "erp"]


@dataclass(frozen=True)
class SourceEvent:
    """One deterministic business event before its source-specific application."""

    event_id: str
    business_date: date
    source_system: SourceSystem
    event_type: str
    entity_id: str
    payload: Mapping[str, Any]
    available_on: date
    anomaly_type: str | None = None

    @classmethod
    def create(
        cls,
        *,
        business_date: date,
        source_system: SourceSystem,
        event_type: str,
        entity_id: str,
        payload: Mapping[str, Any],
        available_on: date | None = None,
        anomaly_type: str | None = None,
    ) -> "SourceEvent":
        stable_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        event_id = hashlib.sha256(
            f"{business_date.isoformat()}|{source_system}|{event_type}|{entity_id}|{stable_payload}".encode()
        ).hexdigest()
        return cls(
            event_id=event_id,
            business_date=business_date,
            source_system=source_system,
            event_type=event_type,
            entity_id=entity_id,
            payload=dict(payload),
            available_on=available_on or business_date,
            anomaly_type=anomaly_type,
        )

    def payload_json(self) -> str:
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":"), default=str)
