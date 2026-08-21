"""Transactional, idempotent application of planned Ops events."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable, Protocol

from simulator.events import SourceEvent


class Cursor(Protocol):
    def execute(self, query: str, parameters: Iterable[Any] | None = None) -> Any: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...


class OpsEventApplier:
    """Caller owns the database transaction; this class never commits partial work."""

    def apply(self, cursor: Cursor, events: Iterable[SourceEvent]) -> int:
        applied = 0
        for event in sorted(events, key=lambda event: (event.business_date, self._event_priority(event))):
            if event.source_system != "ops":
                raise ValueError("Ops applier supports only Ops events")
            cursor.execute(
                "SELECT 1 FROM simulation.applied_events WHERE event_id = %s", [event.event_id]
            )
            if cursor.fetchone() is not None:
                continue
            self._apply_event(cursor, event)
            cursor.execute(
                """
                INSERT INTO simulation.applied_events (event_id, source_system, event_type, business_date)
                VALUES (%s, %s, %s, %s)
                """,
                [event.event_id, event.source_system, event.event_type, event.business_date],
            )
            applied += 1
        return applied

    @staticmethod
    def _event_priority(event: SourceEvent) -> int:
        return {"order_created": 0, "shipment_created": 1, "invoice_created": 2}.get(
            event.event_type, 99
        )

    def _apply_event(self, cursor: Cursor, event: SourceEvent) -> None:
        if event.event_type == "order_created":
            self._apply_order(cursor, event)
        elif event.event_type == "shipment_created":
            self._apply_shipment(cursor, event)
        elif event.event_type == "invoice_created":
            self._apply_invoice(cursor, event)
        else:
            raise ValueError(f"unsupported Ops event type: {event.event_type}")

    def _apply_order(self, cursor: Cursor, event: SourceEvent) -> None:
        payload = event.payload
        customer_id = int(payload["customer_id"])
        order_id = int(payload["order_id"])
        order_date = event.business_date
        cursor.execute("SELECT 1 FROM ops.customers WHERE customer_id = %s", [customer_id])
        if cursor.fetchone() is None:
            cursor.execute(
                """
                INSERT INTO ops.customers (
                    customer_id, account_number, customer_name, segment, industry, region,
                    company_code, country, employee_count, credit_limit_usd,
                    payment_terms_days, is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, 'SMB', 'Technology', 'NA-WEST', 'US01',
                          'United States', 100, 50000, 30, true, %s, %s)
                """,
                [
                    customer_id,
                    f"ACC-{customer_id:06d}",
                    f"CRM customer {customer_id}",
                    f"{order_date.isoformat()} 08:00:00",
                    f"{order_date.isoformat()} 08:00:00",
                ],
            )
        cursor.execute("SELECT opportunity_ref FROM ops.orders WHERE order_id = %s", [order_id])
        existing = cursor.fetchone()
        if existing is not None:
            if existing[0] != payload["opportunity_ref"]:
                raise RuntimeError(f"order ID collision for {order_id}")
            return
        cursor.execute(
            """
            INSERT INTO ops.orders (
                order_id, customer_id, opportunity_ref, rep_id, order_date,
                requested_delivery_date, currency_code, status, sales_channel,
                order_discount_pct, po_number, created_at, updated_at
            ) VALUES (%s, %s, %s, NULL, %s, %s, 'USD', 'PENDING', 'DIRECT',
                      0, NULL, %s, %s)
            """,
            [
                order_id,
                customer_id,
                payload["opportunity_ref"],
                order_date,
                order_date + timedelta(days=7),
                f"{order_date.isoformat()} 08:00:00",
                f"{order_date.isoformat()} 08:00:00",
            ],
        )
        transition_time = f"{order_date.isoformat()} 08:00:00"
        cursor.execute(
            """
            INSERT INTO ops.order_status_history (
                order_id, previous_status, new_status, occurred_at, recorded_at,
                source_event_id, sla_due_at, sla_status, anomaly_type
            ) VALUES (%s, NULL, 'PENDING', %s, %s, %s, NULL, 'ON_TIME', %s)
            """,
            [
                order_id,
                transition_time,
                transition_time,
                event.event_id,
                event.anomaly_type,
            ],
        )
        cursor.execute(
            "SELECT product_id, list_price_usd, standard_cost_usd FROM ops.products ORDER BY product_id LIMIT 1"
        )
        product = cursor.fetchone()
        if product is None:
            raise RuntimeError("Ops order cannot be created without a product catalog")
        cursor.execute("SELECT coalesce(max(order_line_id), 0) + 1 FROM ops.order_lines")
        line_id = cursor.fetchone()[0]
        cursor.execute(
            """
            INSERT INTO ops.order_lines (
                order_line_id, order_id, product_id, quantity, unit_price, discount_pct,
                line_amount, unit_cost_usd, created_at, updated_at
            ) VALUES (%s, %s, %s, 1, %s, 0, %s, %s, %s, %s)
            """,
            [
                line_id,
                order_id,
                product[0],
                product[1],
                product[1],
                product[2],
                f"{order_date.isoformat()} 08:00:00",
                f"{order_date.isoformat()} 08:00:00",
            ],
        )

    def _apply_shipment(self, cursor: Cursor, event: SourceEvent) -> None:
        payload = event.payload
        shipment_id = int(payload["shipment_id"])
        order_id = int(payload["order_id"])
        cursor.execute("SELECT order_id FROM ops.shipments WHERE shipment_id = %s", [shipment_id])
        existing = cursor.fetchone()
        if existing is not None:
            if existing[0] != order_id:
                raise RuntimeError(f"shipment ID collision for {shipment_id}")
            return
        cursor.execute("SELECT status FROM ops.orders WHERE order_id = %s", [order_id])
        order = cursor.fetchone()
        if order is None:
            raise RuntimeError(f"shipment requires existing order {order_id}")
        cursor.execute("SELECT warehouse_code FROM ops.warehouses ORDER BY warehouse_code LIMIT 1")
        warehouse = cursor.fetchone()
        cursor.execute("SELECT carrier_code FROM ops.carriers ORDER BY carrier_code LIMIT 1")
        carrier = cursor.fetchone()
        if warehouse is None or carrier is None:
            raise RuntimeError("shipment requires a warehouse and carrier master record")
        ship_date = event.business_date
        cursor.execute(
            """
            INSERT INTO ops.shipments (
                shipment_id, order_id, warehouse_code, carrier_code, service_level, ship_date,
                promised_delivery_date, delivered_date, package_count, gross_weight_kg,
                distance_km, freight_cost_usd, tracking_number, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, 'STANDARD', %s, %s, %s, 1, 5, 100, 20, %s, %s, %s)
            """,
            [shipment_id, order_id, warehouse[0], carrier[0], ship_date, ship_date + timedelta(days=5),
             ship_date + timedelta(days=4), f"SIM-{shipment_id:012d}",
             f"{ship_date.isoformat()} 08:00:00", f"{ship_date.isoformat()} 17:00:00"],
        )
        if order[0] == "PENDING":
            cursor.execute(
                "UPDATE ops.orders SET status = 'SHIPPED', updated_at = %s WHERE order_id = %s",
                [f"{ship_date.isoformat()} 08:00:00", order_id],
            )

    def _apply_invoice(self, cursor: Cursor, event: SourceEvent) -> None:
        payload = event.payload
        order_id = int(payload["order_id"])
        shipment_id = int(payload["shipment_id"])
        cursor.execute("SELECT invoice_id FROM ops.invoices WHERE order_id = %s", [order_id])
        if cursor.fetchone() is not None:
            return
        cursor.execute("SELECT status, currency_code FROM ops.orders WHERE order_id = %s", [order_id])
        order = cursor.fetchone()
        if order is None:
            raise RuntimeError(f"invoice requires existing order {order_id}")
        cursor.execute("SELECT 1 FROM ops.shipments WHERE shipment_id = %s AND order_id = %s", [shipment_id, order_id])
        if cursor.fetchone() is None:
            raise RuntimeError(f"invoice requires shipment {shipment_id} for order {order_id}")
        cursor.execute("SELECT coalesce(sum(line_amount), 0) FROM ops.order_lines WHERE order_id = %s", [order_id])
        amount = cursor.fetchone()[0]
        if amount <= 0:
            raise RuntimeError(f"invoice requires positive order amount for {order_id}")
        cursor.execute("SELECT coalesce(max(invoice_id), 0) + 1 FROM ops.invoices")
        invoice_id = cursor.fetchone()[0]
        invoice_date = event.business_date
        cursor.execute(
            """
            INSERT INTO ops.invoices (
                invoice_id, invoice_number, order_id, invoice_date, currency_code, amount, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'ISSUED', %s, %s)
            """,
            [invoice_id, f"OPS-{order_id:012d}", order_id, invoice_date, order[1], amount,
             f"{invoice_date.isoformat()} 08:00:00", f"{invoice_date.isoformat()} 08:00:00"],
        )
        if order[0] == "SHIPPED":
            cursor.execute(
                "UPDATE ops.orders SET status = 'INVOICED', updated_at = %s WHERE order_id = %s",
                [f"{invoice_date.isoformat()} 08:00:00", order_id],
            )
