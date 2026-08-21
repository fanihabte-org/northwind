from datetime import date

from simulator.events import SourceEvent
from simulator.ops_apply import OpsEventApplier


class RecordingCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.parameters: list[object] = []
        self.responses = iter([None, None, None, (1, 100.0, 55.0), (900,), None])

    def execute(self, query, parameters=None):
        self.queries.append(query)
        self.parameters.append(parameters)

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
        payload={
            "shipment_id": 500,
            "order_id": 100,
            "order_created_date": "2026-07-24",
        },
    )
    shipment_cursor = RecordingCursor()
    shipment_cursor.responses = iter(
        [None, None, ("PENDING", date(2026, 7, 24)), ("WH-SEA",), ("UPS",)]
    )

    assert OpsEventApplier().apply(shipment_cursor, [shipment]) == 1
    shipment_statements = "\n".join(shipment_cursor.queries)
    assert "INSERT INTO ops.shipments" in shipment_statements
    assert "INSERT INTO ops.shipment_status_history" in shipment_statements
    assert "NULL, 'SHIPPED'" in shipment_statements
    assert "INSERT INTO ops.order_status_history" in shipment_statements
    assert "'PENDING', 'SHIPPED'" in shipment_statements
    assert "status = 'SHIPPED'" in shipment_statements
    history_parameters = next(
        parameters
        for query, parameters in zip(shipment_cursor.queries, shipment_cursor.parameters)
        if "INSERT INTO ops.order_status_history" in query
    )
    assert history_parameters[4:] == ["2026-07-29 08:00:00", "ON_TIME", None]
    shipment_history_parameters = next(
        parameters
        for query, parameters in zip(shipment_cursor.queries, shipment_cursor.parameters)
        if "INSERT INTO ops.shipment_status_history" in query
    )
    assert shipment_history_parameters[0] == 500
    assert shipment_history_parameters[1:3] == ["2026-07-28 08:00:00"] * 2
    assert shipment_history_parameters[-1] is None

    invoice = SourceEvent.create(
        business_date=date(2026, 7, 29),
        source_system="ops",
        event_type="invoice_created",
        entity_id="100",
        payload={"shipment_id": 500, "order_id": 100},
    )
    invoice_cursor = RecordingCursor()
    invoice_cursor.responses = iter(
        [None, None, ("SHIPPED", "USD"), (date(2026, 7, 28),), (100.0,), (900,)]
    )

    assert OpsEventApplier().apply(invoice_cursor, [invoice]) == 1
    invoice_statements = "\n".join(invoice_cursor.queries)
    assert "INSERT INTO ops.invoices" in invoice_statements
    assert "INSERT INTO ops.invoice_status_history" in invoice_statements
    assert "NULL, 'ISSUED'" in invoice_statements
    assert "INSERT INTO ops.order_status_history" in invoice_statements
    assert "'SHIPPED', 'INVOICED'" in invoice_statements
    assert "status = 'INVOICED'" in invoice_statements
    history_parameters = next(
        parameters
        for query, parameters in zip(invoice_cursor.queries, invoice_cursor.parameters)
        if "INSERT INTO ops.order_status_history" in query
    )
    assert history_parameters[4:] == ["2026-07-31 08:00:00", "ON_TIME", None]
    invoice_history_parameters = next(
        parameters
        for query, parameters in zip(invoice_cursor.queries, invoice_cursor.parameters)
        if "INSERT INTO ops.invoice_status_history" in query
    )
    assert invoice_history_parameters[0] == 900
    assert invoice_history_parameters[1:3] == ["2026-07-29 08:00:00"] * 2
    assert invoice_history_parameters[4:] == ["2026-07-31 08:00:00", "ON_TIME", None]
