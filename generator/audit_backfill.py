"""Backfill source-system audit timestamps in bounded, restart-safe batches.

This command is intentionally separate from deployment and daily simulation.
It uses the durable ``simulation.audit_backfill_progress`` ledger created by
the source system's audit-backfill migration, so an interrupted run resumes
after its last committed key.

    python -m generator.audit_backfill --system ops --status
    python -m generator.audit_backfill --system erp --batch-size 10000 --max-batches 1
    python -m generator.audit_backfill --system erp --until-complete
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover - deployment image supplies psycopg
    psycopg = None


OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")
ERP_DSN = os.getenv("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp")


@dataclass(frozen=True)
class BackfillTarget:
    name: str
    table: str
    key: str
    key_type: str
    update_sql: str


TARGETS: tuple[BackfillTarget, ...] = (
    BackfillTarget(
        "products", "ops.products", "product_id", "BIGINT",
        """
        UPDATE ops.products
        SET created_at = COALESCE(created_at, launch_date::timestamp + time '02:00'),
            updated_at = COALESCE(updated_at, launch_date::timestamp + time '02:00')
        WHERE product_id = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "warehouses", "ops.warehouses", "warehouse_code", "VARCHAR",
        """
        UPDATE ops.warehouses
        SET created_at = COALESCE(created_at, timestamp '2022-01-03 08:00:00'),
            updated_at = COALESCE(updated_at, timestamp '2022-01-03 08:00:00')
        WHERE warehouse_code = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "carriers", "ops.carriers", "carrier_code", "VARCHAR",
        """
        UPDATE ops.carriers
        SET created_at = COALESCE(created_at, timestamp '2022-01-03 08:00:00'),
            updated_at = COALESCE(updated_at, timestamp '2022-01-03 08:00:00')
        WHERE carrier_code = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "order_lines", "ops.order_lines", "order_line_id", "BIGINT",
        """
        UPDATE ops.order_lines AS line
        SET created_at = COALESCE(line.created_at, order_header.created_at),
            updated_at = COALESCE(line.updated_at, order_header.updated_at)
        FROM ops.orders AS order_header
        WHERE line.order_id = order_header.order_id
          AND line.order_line_id = ANY(%s)
          AND (line.created_at IS NULL OR line.updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "shipments", "ops.shipments", "shipment_id", "BIGINT",
        """
        UPDATE ops.shipments
        SET created_at = COALESCE(created_at, ship_date::timestamp + time '08:00'),
            updated_at = COALESCE(
                updated_at,
                COALESCE(delivered_date, ship_date)::timestamp +
                    CASE WHEN delivered_date IS NULL THEN time '08:00' ELSE time '17:00' END
            )
        WHERE shipment_id = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "invoices", "ops.invoices", "invoice_id", "BIGINT",
        """
        UPDATE ops.invoices
        SET updated_at = COALESCE(updated_at, created_at)
        WHERE invoice_id = ANY(%s) AND updated_at IS NULL
        """,
    ),
    BackfillTarget(
        "support_cases", "ops.support_cases", "case_id", "BIGINT",
        """
        UPDATE ops.support_cases
        SET created_at = COALESCE(created_at, opened_at),
            updated_at = COALESCE(
                updated_at,
                opened_at + COALESCE(resolution_hours, 0) * interval '1 hour'
            )
        WHERE case_id = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
)


ERP_TARGETS: tuple[BackfillTarget, ...] = (
    BackfillTarget(
        "companies", "erp.companies", "company_code", "VARCHAR",
        """
        UPDATE erp.companies
        SET created_at = COALESCE(created_at, timestamp '2022-01-03 08:00:00'),
            updated_at = COALESCE(updated_at, timestamp '2022-01-03 08:00:00')
        WHERE company_code = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "cost_centers", "erp.cost_centers", "cost_center_code", "VARCHAR",
        """
        UPDATE erp.cost_centers
        SET created_at = COALESCE(created_at, valid_from::timestamp + time '08:00'),
            updated_at = COALESCE(
                updated_at,
                COALESCE(valid_to, valid_from)::timestamp +
                    CASE WHEN valid_to IS NULL THEN time '08:00' ELSE time '17:00' END
            )
        WHERE cost_center_code = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "gl_accounts", "erp.gl_accounts", "gl_account", "VARCHAR",
        """
        UPDATE erp.gl_accounts
        SET created_at = COALESCE(created_at, timestamp '2022-01-03 08:00:00'),
            updated_at = COALESCE(updated_at, timestamp '2022-01-03 08:00:00')
        WHERE gl_account = ANY(%s) AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "fx_rates", "erp.fx_rates",
        "concat_ws(chr(31), from_currency, to_currency, rate_date::text, rate_type)", "TEXT",
        """
        UPDATE erp.fx_rates
        SET created_at = COALESCE(created_at, loaded_at),
            updated_at = COALESCE(updated_at, loaded_at)
        WHERE concat_ws(chr(31), from_currency, to_currency, rate_date::text, rate_type) = ANY(%s)
          AND (created_at IS NULL OR updated_at IS NULL)
        """,
    ),
    BackfillTarget(
        "revenue_postings", "erp.revenue_postings", "posting_id", "BIGINT",
        """
        UPDATE erp.revenue_postings
        SET created_at = COALESCE(created_at, posted_at)
        WHERE posting_id = ANY(%s) AND created_at IS NULL
        """,
    ),
)


class AuditBackfillError(RuntimeError):
    pass


class AuditBackfill:
    targets: tuple[BackfillTarget, ...] = ()
    lock_name = "northwind:audit-backfill"

    def __init__(self, connection: Any, batch_size: int) -> None:
        if batch_size < 1 or batch_size > 100_000:
            raise ValueError("batch_size must be between 1 and 100000")
        self.connection = connection
        self.batch_size = batch_size

    def status(self) -> list[dict[str, object]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT backfill_name, last_key, rows_scanned, rows_updated, completed,
                       started_at, updated_at, completed_at
                FROM simulation.audit_backfill_progress
                ORDER BY backfill_name
                """
            )
            columns = [item.name for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def run(self, max_batches: int, until_complete: bool = False) -> list[dict[str, object]]:
        if max_batches < 1:
            raise ValueError("max_batches must be positive")
        self._lock()
        try:
            results: list[dict[str, object]] = []
            for target in self.targets:
                batches = 0
                while batches < max_batches or until_complete:
                    result = self._run_batch(target)
                    results.append(result)
                    batches += 1
                    if result["completed"]:
                        break
            return results
        finally:
            self._unlock()

    def _lock(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", [self.lock_name])

    def _unlock(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", [self.lock_name])

    def _run_batch(self, target: BackfillTarget) -> dict[str, object]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO simulation.audit_backfill_progress (backfill_name)
                VALUES (%s) ON CONFLICT (backfill_name) DO NOTHING
                """,
                [target.name],
            )
            cursor.execute(
                "SELECT last_key, completed FROM simulation.audit_backfill_progress WHERE backfill_name = %s",
                [target.name],
            )
            last_key, completed = cursor.fetchone()
            if completed:
                self.connection.commit()
                return {"name": target.name, "scanned": 0, "updated": 0, "completed": True}
            keys = self._keys(cursor, target, last_key)
            if not keys:
                cursor.execute(
                    """
                    UPDATE simulation.audit_backfill_progress
                    SET completed = true, completed_at = current_timestamp, updated_at = current_timestamp
                    WHERE backfill_name = %s
                    """,
                    [target.name],
                )
                self.connection.commit()
                return {"name": target.name, "scanned": 0, "updated": 0, "completed": True}
            cursor.execute(target.update_sql, [keys])
            updated = cursor.rowcount
            cursor.execute(
                """
                UPDATE simulation.audit_backfill_progress
                SET last_key = %s, rows_scanned = rows_scanned + %s,
                    rows_updated = rows_updated + %s, updated_at = current_timestamp
                WHERE backfill_name = %s
                """,
                [str(keys[-1]), len(keys), updated, target.name],
            )
        self.connection.commit()
        return {"name": target.name, "scanned": len(keys), "updated": updated, "completed": False}

    def _keys(self, cursor: Any, target: BackfillTarget, last_key: str | None) -> list[object]:
        if last_key is None:
            cursor.execute(
                f"SELECT {target.key} FROM {target.table} ORDER BY {target.key} LIMIT %s",
                [self.batch_size],
            )
        else:
            cursor.execute(
                f"SELECT {target.key} FROM {target.table} WHERE {target.key} > %s::{target.key_type} "
                f"ORDER BY {target.key} LIMIT %s",
                [last_key, self.batch_size],
            )
        return [row[0] for row in cursor.fetchall()]


class OpsAuditBackfill(AuditBackfill):
    targets = TARGETS
    lock_name = "northwind:ops:audit-backfill"


class ErpAuditBackfill(AuditBackfill):
    targets = ERP_TARGETS
    lock_name = "northwind:erp:audit-backfill"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("ops", "erp"), default="ops")
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--erp-dsn", default=ERP_DSN)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--max-batches", type=int, default=1)
    parser.add_argument("--until-complete", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    if psycopg is None:
        raise AuditBackfillError("pip install 'psycopg[binary]' first")
    runner_type = OpsAuditBackfill if args.system == "ops" else ErpAuditBackfill
    dsn = args.ops_dsn if args.system == "ops" else args.erp_dsn
    with psycopg.connect(dsn, autocommit=False) as connection:
        runner = runner_type(connection, args.batch_size)
        if args.status:
            print(json.dumps(runner.status(), default=str, indent=2, sort_keys=True))
        else:
            print(json.dumps(runner.run(args.max_batches, args.until_complete), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
