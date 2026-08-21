"""Bounded source-to-source checks before a simulated business date completes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from simulator.events import SourceEvent
from simulator.state import SimulationStateStore


class ReconciliationError(RuntimeError):
    """A source application marker or business record is missing."""


@dataclass(frozen=True)
class ReconciliationReport:
    run_date: date
    ops_events: int
    erp_events: int


class DailySourceReconciler:
    """Verify only the day's deterministic event IDs and business keys.

    Cross-database foreign keys are intentionally unavailable. These bounded
    lookups provide the equivalent integrity check for simulator-created data
    without scanning historical source tables into memory.
    """

    def __init__(
        self,
        state: SimulationStateStore,
        ops_connection_factory: Callable[[], Any],
        erp_connection_factory: Callable[[], Any],
    ) -> None:
        self.state = state
        self.ops_connection_factory = ops_connection_factory
        self.erp_connection_factory = erp_connection_factory

    def reconcile(self, run_date: date) -> ReconciliationReport:
        ops_events = self.state.events_for(run_date, "ops")
        erp_events = self.state.events_for(run_date, "erp")
        problems = self._ops_problems(ops_events)
        problems.extend(self._erp_problems(erp_events))
        if problems:
            raise ReconciliationError(
                f"simulation reconciliation failed for {run_date.isoformat()}: " + "; ".join(problems)
            )
        return ReconciliationReport(run_date, len(ops_events), len(erp_events))

    def _ops_problems(self, events: list[SourceEvent]) -> list[str]:
        connection = self.ops_connection_factory()
        try:
            cursor = connection.cursor()
            problems = self._missing_markers(cursor, "Ops", events)
            problems.extend(self._missing_key_problems(cursor, "ops.orders", "order_id", events, "order_created", "order_id"))
            problems.extend(self._missing_key_problems(cursor, "ops.shipments", "shipment_id", events, "shipment_created", "shipment_id"))
            problems.extend(self._delivered_shipment_problems(cursor, events))
            problems.extend(self._missing_key_problems(cursor, "ops.invoices", "order_id", events, "invoice_created", "order_id"))
            return problems
        finally:
            connection.close()

    def _erp_problems(self, events: list[SourceEvent]) -> list[str]:
        connection = self.erp_connection_factory()
        try:
            cursor = connection.cursor()
            problems = self._missing_markers(cursor, "ERP", events)
            problems.extend(
                self._missing_key_problems(
                    cursor,
                    "erp.revenue_postings",
                    "posting_id",
                    events,
                    "revenue_posting_created",
                    "posting_id",
                )
            )
            return problems
        finally:
            connection.close()

    @staticmethod
    def _missing_markers(cursor: Any, label: str, events: Iterable[SourceEvent]) -> list[str]:
        event_ids = [event.event_id for event in events]
        if not event_ids:
            return []
        cursor.execute(
            "SELECT event_id FROM simulation.applied_events WHERE event_id = ANY(%s)", [event_ids]
        )
        actual = {str(row[0]) for row in cursor.fetchall()}
        missing = sorted(set(event_ids) - actual)
        return [f"{label} applied-event markers missing: {', '.join(missing)}"] if missing else []

    @staticmethod
    def _missing_key_problems(
        cursor: Any,
        table: str,
        column: str,
        events: Iterable[SourceEvent],
        event_type: str,
        payload_key: str,
    ) -> list[str]:
        ids = [int(event.payload[payload_key]) for event in events if event.event_type == event_type]
        if not ids:
            return []
        cursor.execute(f"SELECT {column} FROM {table} WHERE {column} = ANY(%s)", [ids])
        actual = {int(row[0]) for row in cursor.fetchall()}
        missing = sorted(set(ids) - actual)
        return [f"{table} {column} values missing: {', '.join(map(str, missing))}"] if missing else []

    @staticmethod
    def _delivered_shipment_problems(cursor: Any, events: Iterable[SourceEvent]) -> list[str]:
        shipment_ids = [
            int(event.payload["shipment_id"])
            for event in events
            if event.event_type == "shipment_delivered"
        ]
        if not shipment_ids:
            return []
        cursor.execute(
            "SELECT shipment_id FROM ops.shipments "
            "WHERE shipment_id = ANY(%s) AND delivered_date IS NOT NULL",
            [shipment_ids],
        )
        actual = {int(row[0]) for row in cursor.fetchall()}
        missing = sorted(set(shipment_ids) - actual)
        return [f"ops.shipments delivered shipment_id values missing: {', '.join(map(str, missing))}"] if missing else []
