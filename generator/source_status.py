"""Print low-cost operational status for the independent Ops and ERP sources.

The command uses catalog statistics rather than ``COUNT(*)`` so it is safe to
run against the large ledger while the daily simulator is active.

    python -m generator.source_status
    python -m generator.source_status --system erp
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from generator.migrate import discover_migrations

try:
    import psycopg
except ImportError:  # pragma: no cover - deployment image supplies psycopg
    psycopg = None


OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")
ERP_DSN = os.getenv("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp")

SOURCE_TABLES = {
    "ops": (
        "ops.products", "ops.warehouses", "ops.carriers", "ops.customers",
        "ops.orders", "ops.order_lines", "ops.shipments", "ops.invoices", "ops.support_cases",
    ),
    "erp": (
        "erp.companies", "erp.cost_centers", "erp.gl_accounts", "erp.fx_rates",
        "erp.revenue_postings",
    ),
}


def _migration_status(cursor: Any, system: str) -> dict[str, object]:
    expected = discover_migrations(system)[-1].version
    cursor.execute("SELECT coalesce(max(version), 0) FROM simulation.schema_migrations")
    applied = int(cursor.fetchone()[0])
    return {"expected_version": expected, "applied_version": applied, "current": applied == expected}


def _table_status(cursor: Any, tables: tuple[str, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for table in tables:
        cursor.execute(
            """
            SELECT greatest(c.reltuples::BIGINT, 0), pg_total_relation_size(c.oid)
            FROM pg_class AS c
            WHERE c.oid = %s::regclass
            """,
            [table],
        )
        estimated_rows, total_bytes = cursor.fetchone()
        rows.append({"table": table, "estimated_rows": estimated_rows, "total_bytes": total_bytes})
    return rows


def _event_status(cursor: Any, system: str) -> dict[str, object]:
    cursor.execute(
        """
        SELECT count(*), max(business_date), max(applied_at)
        FROM simulation.applied_events
        WHERE source_system = %s
        """,
        [system],
    )
    count, latest_business_date, latest_applied_at = cursor.fetchone()
    return {
        "applied_events": count,
        "latest_business_date": latest_business_date,
        "latest_applied_at": latest_applied_at,
    }


def _backfill_status(cursor: Any) -> list[dict[str, object]]:
    cursor.execute("SELECT to_regclass('simulation.audit_backfill_progress')")
    if cursor.fetchone()[0] is None:
        return []
    cursor.execute(
        """
        SELECT backfill_name, rows_scanned, rows_updated, completed, updated_at
        FROM simulation.audit_backfill_progress
        ORDER BY backfill_name
        """
    )
    columns = [item.name for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def source_status(connection: Any, system: str) -> dict[str, object]:
    """Return a bounded operational snapshot for one independently owned DB."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_database_size(current_database())")
        database_bytes = cursor.fetchone()[0]
        return {
            "system": system,
            "database_bytes": database_bytes,
            "migrations": _migration_status(cursor, system),
            "tables": _table_status(cursor, SOURCE_TABLES[system]),
            "events": _event_status(cursor, system),
            "audit_backfill": _backfill_status(cursor),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("ops", "erp", "all"), default="all")
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--erp-dsn", default=ERP_DSN)
    args = parser.parse_args(argv)
    if psycopg is None:
        raise RuntimeError("pip install 'psycopg[binary]' first")

    dsns = {"ops": args.ops_dsn, "erp": args.erp_dsn}
    systems = ("ops", "erp") if args.system == "all" else (args.system,)
    snapshots: list[dict[str, object]] = []
    for system in systems:
        with psycopg.connect(dsns[system], autocommit=True) as connection:
            snapshots.append(source_status(connection, system))
    print(json.dumps(snapshots, default=str, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
