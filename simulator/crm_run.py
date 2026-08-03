"""Restart-safe orchestration for the CRM portion of one simulation date."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from simulator.crm import CrmDailyPlanner
from simulator.crm_snapshot import CrmSnapshotReader
from simulator.crm_storage import CrmDeltaPublisher
from simulator.state import SimulationStateStore


@dataclass(frozen=True)
class CrmRunResult:
    run_date: date
    records: int
    parts: dict[str, Path]


class CrmRun:
    def __init__(
        self,
        state: SimulationStateStore,
        planner: CrmDailyPlanner,
        reader: CrmSnapshotReader,
        publisher: CrmDeltaPublisher,
        accounts_path: Path,
        opportunities_path: Path,
    ) -> None:
        self.state = state
        self.planner = planner
        self.reader = reader
        self.publisher = publisher
        self.accounts_path = accounts_path
        self.opportunities_path = opportunities_path

    def run(self, run_date: date, seed: int) -> CrmRunResult:
        self.state.claim_run(run_date, seed)
        events = self.state.events_for(run_date, "crm")
        if not events:
            snapshot = self.reader.read(self.accounts_path, self.opportunities_path, seed)
            events = self.planner.plan(run_date, seed, snapshot)
            self.state.record_events(events)
        parts = self.publisher.publish(run_date, events)
        self.state.mark_source_applied(run_date, "crm", len(events), len(events))
        return CrmRunResult(run_date, len(events), parts)
