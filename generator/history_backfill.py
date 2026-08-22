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
from typing import Any

import psycopg

OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")


@dataclass(frozen=True)
class HistoryTarget:
    name: str
    table: str
    key: str


@dataclass(frozen=True)
class InferenceBatch:
    inserts: list[tuple[Any, ...]]
    invalid_sources: int


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


def _is_date(value: object) -> bool:
    return isinstance(value, (date, datetime))


def _sla_status(occurred_at: date | datetime, due_at: date | datetime | None) -> str:
    if due_at is None:
        return "ON_TIME"
    return "BREACHED" if _business_date(occurred_at) > _business_date(due_at) else "ON_TIME"


def _is_not_before(later: date | datetime, earlier: date | datetime) -> bool:
    """Compare timestamps precisely when available, while accepting DATE source fields."""
    if isinstance(later, datetime) and isinstance(earlier, datetime):
        return later >= earlier
    return _business_date(later) >= _business_date(earlier)


class HistoryBackfill:
    def __init__(self, connection, batch_size: int = 10_000) -> None:
        if not 1 <= batch_size <= 100_000:
            raise ValueError("batch_size must be between 1 and 100000")
        self.connection = connection
        self.batch_size = batch_size

    def status(self) -> list[dict[str, object]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT backfill_name, last_key, rows_scanned, rows_updated, completed "
                "FROM simulation.audit_backfill_progress "
                "WHERE backfill_name LIKE 'inferred_%%' ORDER BY backfill_name"
            )
            columns = [item.name for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def _fetch_candidates(self, cursor, target: HistoryTarget, last_key: str | None) -> list[tuple[Any, ...]]:
        queries = {
            TARGETS[0].name: """
                SELECT o.order_id, o.status, o.created_at, o.requested_delivery_date,
                       s.ship_date, i.created_at
                FROM ops.orders o
                LEFT JOIN ops.shipments s USING (order_id)
                LEFT JOIN ops.invoices i USING (order_id)
                WHERE (%s IS NULL OR o.order_id > %s::BIGINT)
                  AND NOT EXISTS (
                    SELECT 1 FROM ops.order_status_history h WHERE h.order_id = o.order_id
                  )
                ORDER BY o.order_id LIMIT %s
            """,
            TARGETS[1].name: """
                SELECT shipment_id, ship_date, delivered_date, promised_delivery_date
                FROM ops.shipments s
                WHERE (%s IS NULL OR shipment_id > %s::BIGINT)
                  AND NOT EXISTS (
                    SELECT 1 FROM ops.shipment_status_history h WHERE h.shipment_id = s.shipment_id
                  )
                ORDER BY shipment_id LIMIT %s
            """,
            TARGETS[2].name: """
                SELECT invoice_id, status, created_at, updated_at
                FROM ops.invoices i
                WHERE (%s IS NULL OR invoice_id > %s::BIGINT)
                  AND NOT EXISTS (
                    SELECT 1 FROM ops.invoice_status_history h WHERE h.invoice_id = i.invoice_id
                  )
                ORDER BY invoice_id LIMIT %s
            """,
            TARGETS[3].name: """
                SELECT case_id, status, opened_at, resolution_hours, updated_at
                FROM ops.support_cases c
                WHERE (%s IS NULL OR case_id > %s::BIGINT)
                  AND NOT EXISTS (
                    SELECT 1 FROM ops.support_case_status_history h WHERE h.case_id = c.case_id
                  )
                ORDER BY case_id LIMIT %s
            """,
        }
        cursor.execute(queries[target.name], [last_key, last_key, self.batch_size])
        return cursor.fetchall()

    def _infer(self, target: HistoryTarget, row: tuple[Any, ...]) -> tuple[list[tuple[Any, ...]], bool]:
        if target.name == TARGETS[0].name:
            return self._infer_order(target, row)
        if target.name == TARGETS[1].name:
            return self._infer_shipment(target, row)
        if target.name == TARGETS[2].name:
            return self._infer_invoice(target, row)
        return self._infer_support_case(target, row)

    @staticmethod
    def _infer_order(target: HistoryTarget, row: tuple[Any, ...]) -> tuple[list[tuple[Any, ...]], bool]:
        order_id, status, created, due, ship, invoice = row
        if not _is_date(created):
            return [], True
        due_at = due if _is_date(due) else None
        events = [
            (order_id, None, "PENDING", created, created, inferred_event_id(target.name, order_id, "PENDING"), due_at, "ON_TIME", "inferred_baseline")
        ]
        invalid = False
        if status in ("SHIPPED", "INVOICED"):
            if not _is_date(ship) or not _is_not_before(ship, created):
                return events, True
            events.append(
                (order_id, "PENDING", "SHIPPED", ship, ship, inferred_event_id(target.name, order_id, "SHIPPED"), due_at, _sla_status(ship, due_at), "inferred_baseline")
            )
        if status == "INVOICED":
            if not _is_date(invoice) or not _is_date(ship) or not _is_not_before(invoice, ship):
                invalid = True
            else:
                invoice_due = _business_date(ship) + timedelta(days=3)
                events.append(
                    (order_id, "SHIPPED", "INVOICED", invoice, invoice, inferred_event_id(target.name, order_id, "INVOICED"), invoice_due, _sla_status(invoice, invoice_due), "inferred_baseline")
                )
        return events, invalid

    @staticmethod
    def _infer_shipment(target: HistoryTarget, row: tuple[Any, ...]) -> tuple[list[tuple[Any, ...]], bool]:
        shipment_id, ship, delivered, promised = row
        if not _is_date(ship):
            return [], True
        promised_at = promised if _is_date(promised) else None
        events = [
            (shipment_id, None, "SHIPPED", ship, ship, inferred_event_id(target.name, shipment_id, "SHIPPED"), promised_at, _sla_status(ship, promised_at), "inferred_baseline")
        ]
        if delivered is None:
            return events, False
        if not _is_date(delivered) or not _is_not_before(delivered, ship):
            return events, True
        events.append(
            (shipment_id, "SHIPPED", "DELIVERED", delivered, delivered, inferred_event_id(target.name, shipment_id, "DELIVERED"), promised_at, _sla_status(delivered, promised_at), "inferred_baseline")
        )
        return events, False

    @staticmethod
    def _infer_invoice(target: HistoryTarget, row: tuple[Any, ...]) -> tuple[list[tuple[Any, ...]], bool]:
        invoice_id, status, created, updated = row
        if not _is_date(created):
            return [], True
        events = [
            (invoice_id, None, "ISSUED", created, created, inferred_event_id(target.name, invoice_id, "ISSUED"), None, "ON_TIME", "inferred_baseline")
        ]
        if status != "VOID":
            return events, False
        if not _is_date(updated) or not _is_not_before(updated, created):
            return events, True
        events.append(
            (invoice_id, "ISSUED", "VOID", updated, updated, inferred_event_id(target.name, invoice_id, "VOID"), None, "ON_TIME", "inferred_baseline")
        )
        return events, False

    @staticmethod
    def _infer_support_case(target: HistoryTarget, row: tuple[Any, ...]) -> tuple[list[tuple[Any, ...]], bool]:
        case_id, status, opened, resolution_hours, updated = row
        if not _is_date(opened):
            return [], True
        events = [
            (case_id, None, "Open", opened, opened, inferred_event_id(target.name, case_id, "OPEN"), None, "ON_TIME", "inferred_baseline")
        ]
        if status not in ("Resolved", "Closed"):
            return events, False
        try:
            resolved = opened + timedelta(hours=float(resolution_hours))
        except (TypeError, ValueError):
            return events, True
        if not _is_not_before(resolved, opened) or (updated is not None and (not _is_date(updated) or not _is_not_before(updated, resolved))):
            return events, True
        due_at = opened + timedelta(days=3)
        events.append(
            (case_id, "Open", "Resolved", resolved, resolved, inferred_event_id(target.name, case_id, "RESOLVED"), due_at, _sla_status(resolved, due_at), "inferred_baseline")
        )
        if status != "Closed":
            return events, False
        if not _is_date(updated) or not _is_not_before(updated, resolved):
            return events, True
        events.append(
            (case_id, "Resolved", "Closed", updated, updated, inferred_event_id(target.name, case_id, "CLOSED"), None, "ON_TIME", "inferred_baseline")
        )
        return events, False

    def _build_batch(self, target: HistoryTarget, rows: list[tuple[Any, ...]]) -> InferenceBatch:
        inserts: list[tuple[Any, ...]] = []
        invalid_sources = 0
        for row in rows:
            events, invalid = self._infer(target, row)
            inserts.extend(events)
            invalid_sources += int(invalid)
        return InferenceBatch(inserts=inserts, invalid_sources=invalid_sources)

    @staticmethod
    def _result(target: HistoryTarget, rows: list[tuple[Any, ...]], batch: InferenceBatch, *, dry_run: bool, completed: bool) -> dict[str, object]:
        return {
            "name": target.name,
            "scanned": len(rows),
            "eligible": len(rows),
            "inferred": len(batch.inserts),
            "skipped_invalid": batch.invalid_sources,
            "would_write" if dry_run else "inserted": len(batch.inserts),
            "dry_run": dry_run,
            "completed": completed,
        }

    def dry_run(self, target: HistoryTarget, max_batches: int = 1) -> list[dict[str, object]]:
        """Infer bounded candidate batches without writing history or checkpoints."""
        if max_batches < 1:
            raise ValueError("max_batches must be at least 1")
        results: list[dict[str, object]] = []
        last_key: str | None = None
        with self.connection.cursor() as cursor:
            for _ in range(max_batches):
                rows = self._fetch_candidates(cursor, target, last_key)
                if not rows:
                    results.append(self._result(target, rows, InferenceBatch([], 0), dry_run=True, completed=True))
                    break
                batch = self._build_batch(target, rows)
                results.append(self._result(target, rows, batch, dry_run=True, completed=False))
                last_key = str(rows[-1][0])
        return results

    def run(self, target: HistoryTarget, max_batches: int = 1) -> list[dict[str, object]]:
        """Append inferred history using the same candidate and inference path as dry-run."""
        if max_batches < 1:
            raise ValueError("max_batches must be at least 1")
        results: list[dict[str, object]] = []
        for _ in range(max_batches):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO simulation.audit_backfill_progress (backfill_name) VALUES (%s) ON CONFLICT DO NOTHING",
                    [target.name],
                )
                cursor.execute(
                    "SELECT last_key, completed FROM simulation.audit_backfill_progress WHERE backfill_name=%s",
                    [target.name],
                )
                last_key, completed = cursor.fetchone()
                if completed:
                    self.connection.commit()
                    return results + [self._result(target, [], InferenceBatch([], 0), dry_run=False, completed=True)]
                rows = self._fetch_candidates(cursor, target, last_key)
                if not rows:
                    cursor.execute(
                        "UPDATE simulation.audit_backfill_progress SET completed=true, completed_at=current_timestamp WHERE backfill_name=%s",
                        [target.name],
                    )
                    self.connection.commit()
                    return results + [self._result(target, rows, InferenceBatch([], 0), dry_run=False, completed=True)]
                batch = self._build_batch(target, rows)
                if batch.inserts:
                    cursor.executemany(self._history_insert_sql(target), batch.inserts)
                cursor.execute(
                    "UPDATE simulation.audit_backfill_progress "
                    "SET last_key=%s, rows_scanned=rows_scanned+%s, rows_updated=rows_updated+%s, updated_at=current_timestamp "
                    "WHERE backfill_name=%s",
                    [str(rows[-1][0]), len(rows), len(batch.inserts), target.name],
                )
            self.connection.commit()
            results.append(self._result(target, rows, batch, dry_run=False, completed=False))
        return results

    @staticmethod
    def _history_insert_sql(target: HistoryTarget) -> str:
        statements = {
            TARGETS[0].name: "INSERT INTO ops.order_status_history (order_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING",
            TARGETS[1].name: "INSERT INTO ops.shipment_status_history (shipment_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING",
            TARGETS[2].name: "INSERT INTO ops.invoice_status_history (invoice_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING",
            TARGETS[3].name: "INSERT INTO ops.support_case_status_history (case_id,previous_status,new_status,occurred_at,recorded_at,source_event_id,sla_due_at,sla_status,anomaly_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (source_event_id) DO NOTHING",
        }
        return statements[target.name]

    def run_orders(self, max_batches: int = 1) -> list[dict[str, object]]:
        return self.run(TARGETS[0], max_batches)

    def run_shipments(self, max_batches: int = 1) -> list[dict[str, object]]:
        return self.run(TARGETS[1], max_batches)

    def run_invoices(self, max_batches: int = 1) -> list[dict[str, object]]:
        return self.run(TARGETS[2], max_batches)

    def run_support_cases(self, max_batches: int = 1) -> list[dict[str, object]]:
        return self.run(TARGETS[3], max_batches)


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
        target = next(target for target in TARGETS if target.name == args.target)
        if args.status:
            result = runner.status()
        elif args.dry_run:
            result = runner.dry_run(target, args.max_batches)
        elif args.apply:
            result = runner.run(target, args.max_batches)
        else:
            parser.error("choose --dry-run or --status; writes require --apply")
        print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
