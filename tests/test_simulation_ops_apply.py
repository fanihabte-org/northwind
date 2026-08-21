from datetime import date

from simulator.events import SourceEvent
from simulator.ops_apply import OpsEventApplier


class RecordingCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.responses = iter([None, None, None, (1, 100.0, 55.0), (900,), None])

    def execute(self, query, parameters=None):
        self.queries.append(query)

    def fetchone(self):
        return next(self.responses)


def test_ops_applier_inserts_customer_order_line_and_event_marker() -> None:
    event = SourceEvent.create(
        business_date=date(2026, 7, 28),
        source_system="ops",
        event_type="order_created",
        entity_id="100",
        payload={"order_id": 100, "customer_id": 10, "opportunity_ref": "00610"},
    )
    cursor = RecordingCursor()

    applied = OpsEventApplier().apply(cursor, [event])

    assert applied == 1
    statements = "\n".join(cursor.queries)
    assert "INSERT INTO ops.customers" in statements
    assert "INSERT INTO ops.orders" in statements
    assert "INSERT INTO ops.order_lines" in statements
    assert "INSERT INTO ops.order_status_history" in statements
    assert "NULL, 'PENDING'" in statements
    assert "INSERT INTO simulation.applied_events" in statements


def test_ops_applier_applies_shipment_and_invoice_lifecycle_events() -> None:
    shipment = SourceEvent.create(
        business_date=date(2026, 7, 28),
        source_system="ops",
        event_type="shipment_created",
        entity_id="500",
        payload={"shipment_id": 500, "order_id": 100},
    )
    shipment_cursor = RecordingCursor()
    shipment_cursor.responses = iter([None, None, ("PENDING",), ("WH-SEA",), ("UPS",)])

    assert OpsEventApplier().apply(shipment_cursor, [shipment]) == 1
    shipment_statements = "\n".join(shipment_cursor.queries)
    assert "INSERT INTO ops.shipments" in shipment_statements
    assert "status = 'SHIPPED'" in shipment_statements

    invoice = SourceEvent.create(
        business_date=date(2026, 7, 29),
        source_system="ops",
        event_type="invoice_created",
        entity_id="100",
        payload={"shipment_id": 500, "order_id": 100},
    )
    invoice_cursor = RecordingCursor()
    invoice_cursor.responses = iter([None, None, ("SHIPPED", "USD"), (1,), (100.0,), (900,)])

    assert OpsEventApplier().apply(invoice_cursor, [invoice]) == 1
    invoice_statements = "\n".join(invoice_cursor.queries)
    assert "INSERT INTO ops.invoices" in invoice_statements
    assert "status = 'INVOICED'" in invoice_statements
