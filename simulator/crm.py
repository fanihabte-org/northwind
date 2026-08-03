"""Deterministic daily CRM event planning from the current CRM snapshot."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Iterable, Mapping

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy


OPEN_STAGES = ("Prospecting", "Qualification", "Proposal", "Negotiation")
STAGE_SUCCESSOR = {
    "Prospecting": "Qualification",
    "Qualification": "Proposal",
    "Proposal": "Negotiation",
    "Negotiation": "Closed Won",
}
INDUSTRIES = ("Technology", "Healthcare", "Manufacturing", "Financial Services")
COUNTRIES = ("United States", "Canada", "Germany", "United Kingdom", "Japan")


@dataclass(frozen=True)
class CrmSnapshot:
    """Minimal current-state information needed to plan the next CRM business day."""

    accounts: tuple[Mapping[str, Any], ...]
    opportunities: tuple[Mapping[str, Any], ...]
    account_population: int | None = None
    open_opportunity_population: int | None = None
    account_next_sequence: int | None = None
    opportunity_next_sequence: int | None = None
    owner_ids: tuple[str, ...] = ()

    @classmethod
    def from_rows(
        cls, accounts: Iterable[Mapping[str, Any]], opportunities: Iterable[Mapping[str, Any]]
    ) -> "CrmSnapshot":
        return cls(tuple(accounts), tuple(opportunities))


class CrmDailyPlanner:
    def __init__(self, policy: SimulationPolicy) -> None:
        self.policy = policy

    def plan(self, run_date: date, seed: int, snapshot: CrmSnapshot) -> list[SourceEvent]:
        rng = random.Random(self._seed(run_date, seed))
        account_population = snapshot.account_population if snapshot.account_population is not None else len(snapshot.accounts)
        account_count = self.policy.proportional_daily_volume(account_population)
        next_account = snapshot.account_next_sequence or self._next_sequence(snapshot.accounts, "Id", "001")
        next_opportunity = snapshot.opportunity_next_sequence or self._next_sequence(snapshot.opportunities, "Id", "006")
        owner_ids = self._owner_ids(snapshot)
        events: list[SourceEvent] = []
        new_account_ids: list[str] = []

        for index in range(account_count):
            sequence = next_account + index
            account_id = f"001{sequence:015d}"
            new_account_ids.append(account_id)
            events.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="crm",
                    event_type="account_created",
                    entity_id=account_id,
                    payload={
                        "Id": account_id,
                        "Name": f"Daily Systems {sequence:06d}",
                        "AccountNumber": f"ACC-{sequence:06d}",
                        "Industry": rng.choice(INDUSTRIES),
                        "Type": rng.choice(("Customer - Direct", "Customer - Channel", "Partner")),
                        "BillingCountry": rng.choice(COUNTRIES),
                        "AnnualRevenue": float(rng.randrange(500, 50_000) * 1_000),
                        "NumberOfEmployees": rng.randrange(10, 5_000),
                        "OwnerId": rng.choice(owner_ids),
                        "CreatedDate": self._timestamp(run_date),
                        "LastModifiedDate": self._timestamp(run_date),
                        "IsDeleted": False,
                    },
                )
            )

        opportunity_count = max(1, round(account_count * 1.8))
        account_ids = new_account_ids + [str(row["Id"]) for row in snapshot.accounts if "Id" in row]
        for index in range(opportunity_count):
            sequence = next_opportunity + index
            opportunity_id = f"006{sequence:015d}"
            account_id = rng.choice(account_ids)
            events.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="crm",
                    event_type="opportunity_created",
                    entity_id=opportunity_id,
                    payload={
                        "Id": opportunity_id,
                        "AccountId": account_id,
                        "Name": f"Daily opportunity {sequence:06d}",
                        "StageName": "Prospecting",
                        "Amount": float(rng.randrange(10, 2_000) * 1_000),
                        "CurrencyIsoCode": "USD",
                        "CloseDate": (run_date + timedelta(days=rng.randrange(30, 121))).isoformat(),
                        "Probability": 10,
                        "LeadSource": rng.choice(("Web", "Partner", "Referral", "Marketing Campaign")),
                        "CampaignId": None,
                        "DiscountPercent": 0.0,
                        "LossReason": None,
                        "SalesCycleDays": 0,
                        "OwnerId": rng.choice(owner_ids),
                        "CreatedDate": self._timestamp(run_date),
                        "LastModifiedDate": self._timestamp(run_date),
                        "IsDeleted": False,
                    },
                )
            )

        events.extend(
            self._stage_events(
                run_date, rng, snapshot.opportunities, snapshot.open_opportunity_population
            )
        )
        return events

    def _stage_events(
        self,
        run_date: date,
        rng: random.Random,
        opportunities: tuple[Mapping[str, Any], ...],
        open_opportunity_population: int | None,
    ) -> list[SourceEvent]:
        open_rows = [row for row in opportunities if row.get("StageName") in OPEN_STAGES]
        if not open_rows:
            return []
        population = open_opportunity_population if open_opportunity_population is not None else len(open_rows)
        transition_count = max(1, self.policy.proportional_daily_volume(population))
        selected = rng.sample(open_rows, min(transition_count, len(open_rows)))
        events: list[SourceEvent] = []
        stalled_count = min(
            len(selected), max(1, round(len(selected) * self.policy.anomalies.stalled_opportunity))
        )
        stalled_ids = {str(row["Id"]) for row in selected[:stalled_count]}
        for row in selected:
            opportunity_id = str(row["Id"])
            stage = str(row["StageName"])
            if opportunity_id in stalled_ids:
                events.append(
                    SourceEvent.create(
                        business_date=run_date,
                        source_system="crm",
                        event_type="opportunity_stalled",
                        entity_id=opportunity_id,
                        payload={"StageName": stage, "LastModifiedDate": self._timestamp(run_date)},
                        anomaly_type="long_open_opportunity",
                    )
                )
                continue
            next_stage = STAGE_SUCCESSOR[stage]
            payload: dict[str, Any] = {
                "StageName": next_stage,
                "LastModifiedDate": self._timestamp(run_date),
            }
            if row.get("AccountId"):
                payload["AccountId"] = str(row["AccountId"])
            if next_stage == "Closed Won":
                payload.update({"Probability": 100, "CloseDate": run_date.isoformat()})
            events.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="crm",
                    event_type="opportunity_stage_changed",
                    entity_id=opportunity_id,
                    payload=payload,
                )
            )
        return events

    @staticmethod
    def _next_sequence(rows: tuple[Mapping[str, Any], ...], field: str, prefix: str) -> int:
        sequences = [int(str(row[field])[len(prefix) :]) for row in rows if str(row.get(field, "")).startswith(prefix)]
        return max(sequences, default=0) + 1

    @staticmethod
    def _owner_ids(snapshot: CrmSnapshot) -> tuple[str, ...]:
        if snapshot.owner_ids:
            return snapshot.owner_ids
        owners = {str(row["OwnerId"]) for row in snapshot.accounts if row.get("OwnerId")}
        owners.update(str(row["OwnerId"]) for row in snapshot.opportunities if row.get("OwnerId"))
        return tuple(sorted(owners)) or ("REP-0001",)

    @staticmethod
    def _seed(run_date: date, seed: int) -> int:
        return int.from_bytes(hashlib.sha256(f"crm:{seed}:{run_date}".encode()).digest()[:8])

    @staticmethod
    def _timestamp(value: date) -> str:
        return f"{value.isoformat()}T08:00:00.000+0000"
