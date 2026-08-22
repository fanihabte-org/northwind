"""Restart-safe orchestration for the Ops portion of one simulation date."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from simulator.ops import OpsDailyPlanner, OpsSnapshot
from simulator.ops_apply import OpsEventApplier
from simulator.fulfillment import FulfillmentPlanner, FulfillmentSnapshot
from simulator.support import SupportCasePlanner, SupportSnapshot
from simulator.state import SimulationStateError, SimulationStateStore


class Connection(Protocol):
    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class OpsRunResult:
    run_date: date
    records: int
    applied: int


class OpsRun:
    """Plan and apply due Ops orders; the caller supplies the Postgres connection."""

    def __init__(
        self,
        state: SimulationStateStore,
        planner: OpsDailyPlanner,
        fulfillment_planner: FulfillmentPlanner,
        support_case_planner: SupportCasePlanner,
        applier: OpsEventApplier,
        connection_factory: Callable[[], Connection],
    ) -> None:
        self.state = state
        self.planner = planner
        self.fulfillment_planner = fulfillment_planner
        self.support_case_planner = support_case_planner
        self.applier = applier
        self.connection_factory = connection_factory

    def run(self, run_date: date, seed: int) -> OpsRunResult:
        self.state.claim_run(run_date, seed)
        if self.state.source_run_state(run_date, "crm") != "applied":
            raise SimulationStateError("CRM must apply before Ops planning")

        current_events = self.state.events_for(run_date, "ops")
        order_events = [event for event in current_events if event.event_type == "order_created"]
        if not order_events:
            crm_events = self.state.events_through(run_date, "crm")
            snapshot, converted = self._read_snapshot(
                [event.entity_id for event in crm_events]
            )
            order_events = self.planner.plan(run_date, crm_events, converted, snapshot)
            self.state.record_events(order_events)

        applied = self._apply(order_events)
        all_ops_events = self.state.events_through(run_date, "ops")
        fulfillment = self._plan_fulfillment(run_date, all_ops_events)
        self.state.record_events(fulfillment)
        support_cases = self._plan_support_cases(run_date, self.state.events_through(run_date, "ops"))
        self.state.record_events(support_cases)
        fulfillment_events = [
            event
            for event in self.state.events_for(run_date, "ops")
            if event.event_type in {"shipment_created", "shipment_delivered", "invoice_created", "support_case_opened", "support_case_resolved", "support_case_closed"}
        ]
        applied += self._apply(fulfillment_events)
        records = len(self.state.events_for(run_date, "ops"))
        self.state.mark_source_applied(run_date, "ops", records, applied)
        return OpsRunResult(run_date, records, applied)

    def _read_snapshot(self, opportunity_refs: list[str]) -> tuple[OpsSnapshot, set[str]]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT coalesce(max(order_id), 0) + 1 FROM ops.orders")
            next_order_id = cursor.fetchone()[0]
            if not opportunity_refs:
                return OpsSnapshot(next_order_id), set()
            cursor.execute(
                "SELECT opportunity_ref FROM ops.orders WHERE opportunity_ref = ANY(%s)",
                [opportunity_refs],
            )
            converted = {row[0] for row in cursor.fetchall()}
            return OpsSnapshot(next_order_id), converted
        finally:
            connection.close()

    def _plan_fulfillment(self, run_date: date, ops_events: list[Any]) -> list[Any]:
        order_ids = [
            int(event.payload["order_id"])
            for event in ops_events
            if event.event_type == "order_created"
        ]
        snapshot, shipped, invoiced, delivered = self._read_fulfillment_snapshot(order_ids)
        return self.fulfillment_planner.plan(run_date, ops_events, shipped, invoiced, delivered, snapshot)

    def _read_fulfillment_snapshot(
        self, order_ids: list[int]
    ) -> tuple[FulfillmentSnapshot, set[int], set[int], set[int]]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT coalesce(max(shipment_id), 0) + 1 FROM ops.shipments")
            snapshot = FulfillmentSnapshot(cursor.fetchone()[0])
            if not order_ids:
                return snapshot, set(), set(), set()
            cursor.execute("SELECT order_id FROM ops.shipments WHERE order_id = ANY(%s)", [order_ids])
            shipped = {row[0] for row in cursor.fetchall()}
            cursor.execute("SELECT order_id FROM ops.invoices WHERE order_id = ANY(%s)", [order_ids])
            invoiced = {row[0] for row in cursor.fetchall()}
            cursor.execute(
                "SELECT shipment_id FROM ops.shipments "
                "WHERE order_id = ANY(%s) AND delivered_date IS NOT NULL",
                [order_ids],
            )
            delivered = {row[0] for row in cursor.fetchall()}
            return snapshot, shipped, invoiced, delivered
        finally:
            connection.close()

    def _plan_support_cases(self, run_date: date, ops_events: list[Any]) -> list[Any]:
        connection = self.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT coalesce(max(case_id), 0) + 1 FROM ops.support_cases")
            return self.support_case_planner.plan(
                run_date, ops_events, SupportSnapshot(cursor.fetchone()[0])
            )
        finally:
            connection.close()

    def _apply(self, events: list[Any]) -> int:
        connection = self.connection_factory()
        try:
            applied = self.applier.apply(connection.cursor(), events)
            connection.commit()
            return applied
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
