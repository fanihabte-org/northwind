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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--target", choices=[target.name for target in TARGETS], default=TARGETS[0].name)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    with psycopg.connect(args.ops_dsn, autocommit=False) as connection:
        runner = HistoryBackfill(connection, args.batch_size)
        if args.status:
            result = runner.status()
        elif args.dry_run:
            result = runner.dry_run(next(target for target in TARGETS if target.name == args.target))
        else:
            parser.error("choose --dry-run or --status; writes require a table-specific implementation")
        print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
