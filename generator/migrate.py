"""Apply ordered, checksummed schema migrations to an independently owned source DB.

Each source has its own ``simulation.schema_migrations`` ledger inside its own
Postgres database.  The runner never drops a schema or truncates business data;
an altered or missing already-applied migration fails loudly instead of guessing.

    python generator/migrate.py                  # Ops and ERP
    python generator/migrate.py --system ops
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:
    import psycopg
except ImportError:  # pragma: no cover - exercised in deployment image
    psycopg = None


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_ROOT = ROOT / "sql" / "migrations"
OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")
ERP_DSN = os.getenv("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp")
SYSTEM_DSN_DEFAULTS = {"ops": OPS_DSN, "erp": ERP_DSN}
_FILENAME = re.compile(r"(?P<version>[0-9]+)_(?P<name>[a-z][a-z0-9_]*)\.sql$")


class MigrationError(RuntimeError):
    """A migration set or its durable ledger is inconsistent."""


@dataclass(frozen=True)
class Migration:
    version: int
    filename: str
    checksum: str
    sql: str


def discover_migrations(system: str, migrations_root: Path = MIGRATIONS_ROOT) -> list[Migration]:
    """Return one contiguous, numerically ordered migration chain for ``system``."""
    directory = migrations_root / system
    if not directory.is_dir():
        raise MigrationError(f"migration directory is missing for {system}: {directory}")

    migrations: list[Migration] = []
    seen_versions: set[int] = set()
    for path in sorted(directory.glob("*.sql")):
        match = _FILENAME.fullmatch(path.name)
        if match is None:
            raise MigrationError(f"invalid migration filename for {system}: {path.name}")
        version = int(match.group("version"))
        if version in seen_versions:
            raise MigrationError(f"duplicate migration version {version} for {system}")
        seen_versions.add(version)
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=version,
                filename=path.name,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
            )
        )

    if not migrations:
        raise MigrationError(f"no migrations found for {system}: {directory}")
    versions = [migration.version for migration in migrations]
    expected = list(range(1, versions[-1] + 1))
    if versions != expected:
        raise MigrationError(f"migration versions for {system} must be contiguous from 1; found {versions}")
    return migrations


def _prepare_ledger(cursor: Any, system: str) -> None:
    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", [f"northwind:{system}:migrations"])
    cursor.execute("CREATE SCHEMA IF NOT EXISTS simulation")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS simulation.schema_migrations (
            version INTEGER PRIMARY KEY,
            filename VARCHAR(255) NOT NULL UNIQUE,
            checksum CHAR(64) NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
        )
        """
    )


def _validate_applied(system: str, migrations: Iterable[Migration], rows: Iterable[tuple[Any, ...]]) -> set[int]:
    available = {migration.version: migration for migration in migrations}
    applied: set[int] = set()
    for version_raw, filename, checksum in rows:
        version = int(version_raw)
        migration = available.get(version)
        if migration is None:
            raise MigrationError(f"{system} has applied migration {version} that is absent from this checkout")
        if migration.filename != filename or migration.checksum != checksum:
            raise MigrationError(
                f"{system} migration {version} does not match its recorded filename or checksum; "
                "create a new migration instead of editing an applied one"
            )
        applied.add(version)
    if applied and applied != set(range(1, max(applied) + 1)):
        raise MigrationError(f"{system} migration ledger has a gap: {sorted(applied)}")
    return applied


def apply_migrations(
    connection: Any,
    system: str,
    migrations_root: Path = MIGRATIONS_ROOT,
) -> list[Migration]:
    """Apply pending migrations inside the caller's transaction and return them.

    No commit happens here.  That lets the initial loader apply DDL and COPY its
    seed atomically, while the Compose migration service commits only its schema
    ledger work.
    """
    migrations = discover_migrations(system, migrations_root)
    cursor = connection.cursor()
    _prepare_ledger(cursor, system)
    cursor.execute(
        "SELECT version, filename, checksum FROM simulation.schema_migrations ORDER BY version"
    )
    applied = _validate_applied(system, migrations, cursor.fetchall())
    pending = [migration for migration in migrations if migration.version not in applied]
    for migration in pending:
        cursor.execute(migration.sql)
        cursor.execute(
            """
            INSERT INTO simulation.schema_migrations (version, filename, checksum)
            VALUES (%s, %s, %s)
            """,
            [migration.version, migration.filename, migration.checksum],
        )
    return pending


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply non-destructive source schema migrations")
    parser.add_argument("--system", choices=("ops", "erp", "all"), default="all")
    parser.add_argument("--ops-dsn", default=OPS_DSN)
    parser.add_argument("--erp-dsn", default=ERP_DSN)
    args = parser.parse_args(argv)
    if psycopg is None:
        raise RuntimeError("pip install 'psycopg[binary]' first")

    selected = ("ops", "erp") if args.system == "all" else (args.system,)
    dsns = {"ops": args.ops_dsn, "erp": args.erp_dsn}
    for system in selected:
        with psycopg.connect(dsns[system], autocommit=False) as connection:
            applied = apply_migrations(connection, system)
            connection.commit()
        if applied:
            print(f"{system.upper()}: applied " + ", ".join(migration.filename for migration in applied))
        else:
            print(f"{system.upper()}: schema is already current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
