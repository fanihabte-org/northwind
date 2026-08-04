"""Capture versioned Ops and ERP source schemas in the analytics registry.

The command reads PostgreSQL's information schema only; it never reads source
business rows. A table gains a new registry version only when its normalized
column contract changes.

    python -m generator.metadata_registry --dry-run
    python -m generator.metadata_registry
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover - deployment image supplies psycopg
    psycopg = None


OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")
ERP_DSN = os.getenv("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp")
ANALYTICS_DSN = os.getenv(
    "ANALYTICS_PG_DSN", "postgresql://analytics:analytics@localhost:5435/rev_engine_pipeline"
)
SOURCE_SCHEMAS = {"ops": "ops", "erp": "erp"}


def read_columns(connection: Any, schema: str) -> dict[str, list[dict[str, object]]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name, ordinal_position, column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
            """,
            [schema],
        )
        columns = [item.name for item in cursor.description]
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in cursor.fetchall():
            value = dict(zip(columns, row, strict=True))
            value["is_nullable"] = value["is_nullable"] == "YES"
            grouped[str(value.pop("table_name"))].append(value)
    return dict(grouped)


def fingerprint(columns: list[dict[str, object]]) -> str:
    payload = json.dumps(columns, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def record_table(
    connection: Any,
    source_system: str,
    source_schema: str,
    table_name: str,
    columns: list[dict[str, object]],
    dry_run: bool,
) -> dict[str, object]:
    contract_hash = fingerprint(columns)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_version_id, version
            FROM metadata.table_versions
            WHERE source_system = %s AND source_schema = %s AND table_name = %s
              AND schema_fingerprint = %s
            """,
            [source_system, source_schema, table_name, contract_hash],
        )
        current = cursor.fetchone()
        if current is not None:
            return {"table": table_name, "version": current[1], "changed": False}
        cursor.execute(
            """
            SELECT coalesce(max(version), 0) + 1
            FROM metadata.table_versions
            WHERE source_system = %s AND source_schema = %s AND table_name = %s
            """,
            [source_system, source_schema, table_name],
        )
        version = int(cursor.fetchone()[0])
        if dry_run:
            return {"table": table_name, "version": version, "changed": True}
        cursor.execute(
            """
            UPDATE metadata.table_versions SET retired_at = current_timestamp
            WHERE source_system = %s AND source_schema = %s AND table_name = %s
              AND retired_at IS NULL
            """,
            [source_system, source_schema, table_name],
        )
        cursor.execute(
            """
            INSERT INTO metadata.table_versions
                (source_system, source_schema, table_name, version, schema_fingerprint)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING table_version_id
            """,
            [source_system, source_schema, table_name, version, contract_hash],
        )
        version_id = cursor.fetchone()[0]
        cursor.executemany(
            """
            INSERT INTO metadata.column_versions (
                table_version_id, ordinal_position, column_name, data_type, is_nullable,
                character_maximum_length, numeric_precision, numeric_scale
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                [
                    version_id, item["ordinal_position"], item["column_name"], item["data_type"],
                    item["is_nullable"], item["character_maximum_length"],
                    item["numeric_precision"], item["numeric_scale"],
                ]
                for item in columns
            ],
        )
    return {"table": table_name, "version": version, "changed": True}


def sync_system(
    source_connection: Any, analytics_connection: Any, system: str, dry_run: bool = False
) -> list[dict[str, object]]:
    schema = SOURCE_SCHEMAS[system]
    tables = read_columns(source_connection, schema)
    return [
        record_table(analytics_connection, system, schema, name, columns, dry_run)
        for name, columns in tables.items()
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("ops", "erp", "all"), default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--erp-dsn", default=ERP_DSN)
    parser.add_argument("--analytics-dsn", default=ANALYTICS_DSN)
    args = parser.parse_args(argv)
    if psycopg is None:
        raise RuntimeError("pip install 'psycopg[binary]' first")

    dsns = {"ops": args.ops_dsn, "erp": args.erp_dsn}
    systems = ("ops", "erp") if args.system == "all" else (args.system,)
    report: dict[str, list[dict[str, object]]] = {}
    with psycopg.connect(args.analytics_dsn, autocommit=False) as analytics_connection:
        for system in systems:
            with psycopg.connect(dsns[system], autocommit=True) as source_connection:
                report[system] = sync_system(source_connection, analytics_connection, system, args.dry_run)
        if args.dry_run:
            analytics_connection.rollback()
        else:
            analytics_connection.commit()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
