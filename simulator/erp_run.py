"""Restart-safe orchestration for the ERP portion of one simulation date."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from simulator.erp import ErpDailyPlanner, ErpSnapshot, OpsInvoice
from simulator.erp_apply import ErpEventApplier
from simulator.state import SimulationStateError, SimulationStateStore


class Connection(Protocol):
    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class ErpRunResult:
    run_date: date
    records: int
    applied: int


class ErpRun:
    """Read the Ops invoice hand-off and apply due postings to Finance only."""

    def __init__(
        self,
        state: SimulationStateStore,
        planner: ErpDailyPlanner,
        applier: ErpEventApplier,
        ops_connection_factory: Callable[[], Connection],
        erp_connection_factory: Callable[[], Connection],
    ) -> None:
        self.state = state
        self.planner = planner
        self.applier = applier
        self.ops_connection_factory = ops_connection_factory
        self.erp_connection_factory = erp_connection_factory

    def run(self, run_date: date, seed: int) -> ErpRunResult:
        self.state.claim_run(run_date, seed)
        if self.state.source_run_state(run_date, "ops") != "applied":
            raise SimulationStateError("Ops must apply before ERP planning")

        events = self.state.events_for(run_date, "erp")
        if not events:
            invoices = self._read_ops_invoices()
            snapshot, posted_order_ids, currencies = self._read_erp_snapshot(
                [invoice.company_code for invoice in invoices],
                [invoice.order_id for invoice in invoices],
            )
            enriched_invoices = [
                OpsInvoice(
                    invoice_id=invoice.invoice_id,
                    invoice_number=invoice.invoice_number,
                    order_id=invoice.order_id,
                    invoice_date=invoice.invoice_date,
                    company_code=invoice.company_code,
                    company_currency=currencies[invoice.company_code],
                    document_currency=invoice.document_currency,
                    amount_doc=invoice.amount_doc,
                )
                for invoice in invoices
            ]
            events = self.planner.plan(run_date, seed, enriched_invoices, posted_order_ids, snapshot)
            self.state.record_events(events)

        applied = self._apply(events)
        self.state.mark_source_applied(run_date, "erp", len(events), applied)
        self.state.complete_run(run_date)
        return ErpRunResult(run_date, len(events), applied)

    def _read_ops_invoices(self) -> list[OpsInvoice]:
        connection = self.ops_connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT i.invoice_id, i.invoice_number, i.order_id, i.invoice_date,
                       c.company_code, i.currency_code, i.amount
                FROM ops.invoices AS i
                JOIN ops.orders AS o ON o.order_id = i.order_id
                JOIN ops.customers AS c ON c.customer_id = o.customer_id
                WHERE i.status = 'ISSUED'
                ORDER BY i.invoice_id
                """
            )
            return [
                OpsInvoice(
                    invoice_id=row[0],
                    invoice_number=row[1],
                    order_id=row[2],
                    invoice_date=row[3],
                    company_code=row[4],
                    company_currency="",  # resolved from ERP's own company master below
                    document_currency=row[5],
                    amount_doc=row[6],
                )
                for row in cursor.fetchall()
            ]
        finally:
            connection.close()

    def _read_erp_snapshot(
        self, company_codes: list[str], order_ids: list[int]
    ) -> tuple[ErpSnapshot, set[int], dict[str, str]]:
        connection = self.erp_connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT coalesce(max(posting_id), 0) + 1 FROM erp.revenue_postings")
            snapshot = ErpSnapshot(cursor.fetchone()[0])
            posted_order_ids: set[int] = set()
            if order_ids:
                cursor.execute(
                    "SELECT order_ref FROM erp.revenue_postings WHERE order_ref = ANY(%s)", [order_ids]
                )
                posted_order_ids = {row[0] for row in cursor.fetchall()}
            if not company_codes:
                return snapshot, posted_order_ids, {}
            cursor.execute(
                "SELECT company_code, functional_currency FROM erp.companies WHERE company_code = ANY(%s)",
                [sorted(set(company_codes))],
            )
            currencies = dict(cursor.fetchall())
            missing = set(company_codes) - currencies.keys()
            if missing:
                raise RuntimeError(f"Ops invoice refers to unknown ERP companies: {sorted(missing)}")
            return snapshot, posted_order_ids, currencies
        finally:
            connection.close()

    def _apply(self, events: list[Any]) -> int:
        connection = self.erp_connection_factory()
        try:
            applied = self.applier.apply(connection.cursor(), events)
            connection.commit()
            return applied
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
