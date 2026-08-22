"""Opt-in, bounded framework for inferred baseline lifecycle history.

This command never updates current-state records. Table-specific inference is
added as explicit targets; ``--dry-run`` is safe against production sources.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import psycopg

OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")


@dataclass(frozen=True)
class HistoryTarget:
    name: str
    table: str
    key: str


TARGETS = (
    HistoryTarget("inferred_order_status_history", "ops.orders", "order_id"),
    HistoryTarget("inferred_shipment_status_history", "ops.shipments", "shipment_id"),
    HistoryTarget("inferred_invoice_status_history", "ops.invoices", "invoice_id"),
    HistoryTarget("inferred_support_case_status_history", "ops.support_cases", "case_id"),
)


def inferred_event_id(history_name: str, entity_id: object, transition: str) -> str:
    return hashlib.sha256(f"inferred_baseline:{history_name}:{entity_id}:{transition}".encode()).hexdigest()


def _business_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


class HistoryBackfill:
    def __init__(self, connection, batch_size: int = 10_000) -> None:
        if not 1 <= batch_size <= 100_000:
            raise ValueError("batch_size must be between 1 and 100000")
        self.connection = connection
        self.batch_size = batch_size

    def status(self) -> list[dict[str, object]]:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT backfill_name, last_key, rows_scanned, rows_updated, completed FROM simulation.audit_backfill_progress WHERE backfill_name LIKE 'inferred_%%' ORDER BY backfill_name")
            columns = [item.name for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def dry_run(self, target: HistoryTarget) -> dict[str, object]:
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT {target.key} FROM {target.table} ORDER BY {target.key} LIMIT %s", [self.batch_size])
            keys = cursor.fetchall()
        return {"name": target.name, "scanned": len(keys), "would_write": 0, "dry_run": True}

    def run_orders(self, max_batches: int = 1) -> list[dict[str, object]]:
        target = TARGETS[0]
        results = []
        for _ in range(max_batches):
            with self.connection.cursor() as cursor:
                cursor.execute("INSERT INTO simulation.audit_backfill_progress (backfill_name) VALUES (%s) ON CONFLICT DO NOTHING", [target.name])
                cursor.execute("SELECT last_key, completed FROM simulation.audit_backfill_progress WHERE backfill_name=%s", [target.name])
                last_key, completed = cursor.fetchone()
                if completed:
                    self.connection.commit(); return results + [{"name": target.name, "scanned": 0, "inserted": 0, "completed": True}]
                cursor.execute("""
                    SELECT o.order_id, o.status, o.created_at, o.requested_delivery_date,
                           s.ship_date, i.created_at
                    FROM ops.orders o LEFT JOIN ops.shipments s USING (order_id)
                    LEFT JOIN ops.invoices i USING (order_id)
                    WHERE (%s IS NULL OR o.order_id > %s::BIGINT)
                      AND NOT EXISTS (SELECT 1 FROM ops.order_status_history h WHERE h.order_id=o.order_id)
                    ORDER BY o.order_id LIMIT %s
                    """, [last_key, last_key, self.batch_size])
                rows = cursor.fetchall()
                if not rows:
                    cursor.execute("UPDATE simulation.audit_backfill_progress SET completed=true, completed_at=current_timestamp WHERE backfill_name=%s", [target.name]); self.connection.commit()
                    return results + [{"name": target.name, "scanned": 0, "inserted": 0, "completed": True}]
                inserts=[]
                for order_id,status,created,due,ship,invoice in rows:
                    inserts.append((order_id,None,"PENDING",created,created,inferred_event_id(target.name,order_id,"PENDING"),due,"ON_TIME","inferred_baseline"))
                    if status in ("SHIPPED","INVOICED") and ship:
                        inserts.append((order_id,"PENDING","SHIPPED",ship,ship,inferred_event_id(target.name,order_id,"SHIPPED"),due,"BREACHED" if _business_date(ship)>due else "ON_TIME","inferred_baseline"))
                    if status == "INVOICED" and ship and invoice:
                        invoice_due = _business_date(ship) + timedelta(days=3)
                        inserts.append((order_id,"SHIPPED","INVOICED",invoice,invoice,inferred_event_id(target.name,order_id,"INVOICED"),invoice_due,"BREACHED" if _business_date(invoice)>invoice_due else "ON_TIME","inferred_baseline"))
                cursor.executemany("INSERT INTO ops.order_status_history (order_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING", inserts)
                cursor.execute("UPDATE simulation.audit_backfill_progress SET last_key=%s, rows_scanned=rows_scanned+%s, rows_updated=rows_updated+%s, updated_at=current_timestamp WHERE backfill_name=%s", [str(rows[-1][0]),len(rows),len(inserts),target.name])
            self.connection.commit(); results.append({"name":target.name,"scanned":len(rows),"inserted":len(inserts),"completed":False})
        return results

    def run_shipments(self, max_batches: int = 1) -> list[dict[str, object]]:
        target = TARGETS[1]; results = []
        for _ in range(max_batches):
            with self.connection.cursor() as cursor:
                cursor.execute("INSERT INTO simulation.audit_backfill_progress (backfill_name) VALUES (%s) ON CONFLICT DO NOTHING", [target.name])
                cursor.execute("SELECT last_key, completed FROM simulation.audit_backfill_progress WHERE backfill_name=%s", [target.name])
                last_key, completed = cursor.fetchone()
                if completed:
                    self.connection.commit(); return results + [{"name":target.name,"scanned":0,"inserted":0,"completed":True}]
                cursor.execute("""SELECT shipment_id, ship_date, delivered_date, promised_delivery_date FROM ops.shipments s
                    WHERE (%s IS NULL OR shipment_id > %s::BIGINT) AND NOT EXISTS
                    (SELECT 1 FROM ops.shipment_status_history h WHERE h.shipment_id=s.shipment_id)
                    ORDER BY shipment_id LIMIT %s""", [last_key,last_key,self.batch_size])
                rows=cursor.fetchall()
                if not rows:
                    cursor.execute("UPDATE simulation.audit_backfill_progress SET completed=true, completed_at=current_timestamp WHERE backfill_name=%s", [target.name]); self.connection.commit()
                    return results + [{"name":target.name,"scanned":0,"inserted":0,"completed":True}]
                inserts=[]
                for shipment_id,ship,delivered,promised in rows:
                    inserts.append((shipment_id,None,"SHIPPED",ship,ship,inferred_event_id(target.name,shipment_id,"SHIPPED"),promised,"ON_TIME","inferred_baseline"))
                    if delivered:
                        inserts.append((shipment_id,"SHIPPED","DELIVERED",delivered,delivered,inferred_event_id(target.name,shipment_id,"DELIVERED"),promised,"BREACHED" if _business_date(delivered)>promised else "ON_TIME","inferred_baseline"))
                cursor.executemany("INSERT INTO ops.shipment_status_history (shipment_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING", inserts)
                cursor.execute("UPDATE simulation.audit_backfill_progress SET last_key=%s, rows_scanned=rows_scanned+%s, rows_updated=rows_updated+%s, updated_at=current_timestamp WHERE backfill_name=%s", [str(rows[-1][0]),len(rows),len(inserts),target.name])
            self.connection.commit(); results.append({"name":target.name,"scanned":len(rows),"inserted":len(inserts),"completed":False})
        return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--target", choices=[target.name for target in TARGETS], default=TARGETS[0].name)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-batches", type=int, default=1)
    args = parser.parse_args(argv)
    with psycopg.connect(args.ops_dsn, autocommit=False) as connection:
        runner = HistoryBackfill(connection, args.batch_size)
        if args.status:
            result = runner.status()
        elif args.dry_run:
            result = runner.dry_run(next(target for target in TARGETS if target.name == args.target))
        elif args.apply and args.target == TARGETS[0].name:
            result = runner.run_orders(args.max_batches)
        elif args.apply and args.target == TARGETS[1].name:
            result = runner.run_shipments(args.max_batches)
        else:
            parser.error("choose --dry-run or --status; writes require a table-specific implementation")
        print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
