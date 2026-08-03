from datetime import date

import pytest

from simulator.events import SourceEvent
from simulator.reconciliation import DailySourceReconciler, ReconciliationError


class State:
    def __init__(self, events):
        self.events = events

    def events_for(self, run_date, source):
        return self.events.get(source, [])


class Cursor:
    def __init__(self, markers, records):
        self.markers = set(markers)
        self.records = records
        self.result = []

    def execute(self, statement, parameters=None):
        requested = parameters[0]
        if "simulation.applied_events" in statement:
            self.result = [(event_id,) for event_id in requested if event_id in self.markers]
            return
        table = statement.split(" FROM ", 1)[1].split(" WHERE", 1)[0]
        self.result = [(record_id,) for record_id in requested if record_id in self.records.get(table, set())]

    def fetchall(self):
        return self.result


class Connection:
    def __init__(self, markers, records):
        self.cursor_value = Cursor(markers, records)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self):
        self.closed = True


def _events(run_date):
    ops = [
        SourceEvent.create(
            business_date=run_date,
            source_system="ops",
            event_type="order_created",
            entity_id="100",
            payload={"order_id": 100},
        ),
        SourceEvent.create(
            business_date=run_date,
            source_system="ops",
            event_type="shipment_created",
            entity_id="200",
            payload={"shipment_id": 200, "order_id": 100},
        ),
        SourceEvent.create(
            business_date=run_date,
            source_system="ops",
            event_type="invoice_created",
            entity_id="100",
            payload={"order_id": 100, "shipment_id": 200},
        ),
    ]
    erp = [
        SourceEvent.create(
            business_date=run_date,
            source_system="erp",
            event_type="revenue_posting_created",
            entity_id="300",
            payload={"posting_id": 300, "order_ref": 100},
        )
    ]
    return ops, erp


def test_reconciliation_checks_source_markers_and_business_records() -> None:
    run_date = date(2026, 7, 25)
    ops_events, erp_events = _events(run_date)
    ops = Connection(
        [event.event_id for event in ops_events],
        {"ops.orders": {100}, "ops.shipments": {200}, "ops.invoices": {100}},
    )
    erp = Connection([event.event_id for event in erp_events], {"erp.revenue_postings": {300}})

    report = DailySourceReconciler(
        State({"ops": ops_events, "erp": erp_events}), lambda: ops, lambda: erp
    ).reconcile(run_date)

    assert report.ops_events == 3
    assert report.erp_events == 1
    assert ops.closed and erp.closed


def test_reconciliation_keeps_the_day_incomplete_when_a_record_is_missing() -> None:
    run_date = date(2026, 7, 25)
    ops_events, erp_events = _events(run_date)
    ops = Connection(
        [event.event_id for event in ops_events],
        {"ops.orders": {100}, "ops.shipments": {200}, "ops.invoices": set()},
    )
    erp = Connection([event.event_id for event in erp_events], {"erp.revenue_postings": {300}})

    with pytest.raises(ReconciliationError, match="ops.invoices order_id values missing: 100"):
        DailySourceReconciler(
            State({"ops": ops_events, "erp": erp_events}), lambda: ops, lambda: erp
        ).reconcile(run_date)
