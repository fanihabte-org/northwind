"""Readiness gate for the one-time source baseline bootstrap."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


OPS_TABLES = (
    "customers",
    "products",
    "warehouses",
    "carriers",
    "orders",
    "order_lines",
    "shipments",
    "invoices",
    "support_cases",
)
ERP_TABLES = ("companies", "cost_centers", "gl_accounts", "fx_rates", "revenue_postings")
CRM_SEED_FILES = ("crm_accounts.parquet", "crm_opportunities.parquet")


@dataclass(frozen=True)
class BootstrapReport:
    ready: bool
    problems: tuple[str, ...]

    def message(self) -> str:
        return "; ".join(self.problems)


class SourceBootstrapProbe:
    """Check that the CRM files and both source database baselines exist.

    Database health only confirms that Postgres accepts connections.  The
    simulator needs the generated source schema and rows as well, so a fresh
    Compose deployment must wait until ``generator/load.py`` commits.
    """

    def __init__(
        self,
        seed_directory: Path,
        ops_connection_factory: Callable[[], Any],
        erp_connection_factory: Callable[[], Any],
    ) -> None:
        self.seed_directory = seed_directory
        self.ops_connection_factory = ops_connection_factory
        self.erp_connection_factory = erp_connection_factory

    @classmethod
    def from_dsns(
        cls, seed_directory: Path, ops_dsn: str, erp_dsn: str
    ) -> "SourceBootstrapProbe":
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment image includes psycopg
            raise RuntimeError("install psycopg[binary] to check source bootstrap readiness") from exc
        return cls(
            seed_directory,
            lambda: psycopg.connect(ops_dsn, autocommit=True),
            lambda: psycopg.connect(erp_dsn, autocommit=True),
        )

    def check(self) -> BootstrapReport:
        problems = self._crm_seed_problems()
        problems.extend(self._database_problems("Ops", "ops", OPS_TABLES, self.ops_connection_factory))
        problems.extend(self._database_problems("ERP", "erp", ERP_TABLES, self.erp_connection_factory))
        return BootstrapReport(ready=not problems, problems=tuple(problems))

    def _crm_seed_problems(self) -> list[str]:
        problems: list[str] = []
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - deployment image includes pyarrow
            return [f"CRM Parquet reader unavailable: {exc}"]
        for filename in CRM_SEED_FILES:
            path = self.seed_directory / filename
            if not path.is_file():
                problems.append(f"CRM seed file is missing: {path}")
                continue
            try:
                if pq.ParquetFile(path).metadata.num_rows < 1:
                    problems.append(f"CRM seed file has no rows: {path}")
            except Exception as exc:
                problems.append(f"CRM seed file cannot be read: {path} ({exc})")
        return problems

    @staticmethod
    def _database_problems(
        label: str,
        schema: str,
        required_tables: tuple[str, ...],
        connection_factory: Callable[[], Any],
    ) -> list[str]:
        connection = None
        try:
            connection = connection_factory()
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name = ANY(%s)
                """,
                [schema, list(required_tables)],
            )
            available = {str(row[0]) for row in cursor.fetchall()}
            missing = sorted(set(required_tables) - available)
            if missing:
                return [f"{label} baseline tables are missing: {', '.join(missing)}"]
            empty: list[str] = []
            for table in required_tables:
                cursor.execute(f"SELECT 1 FROM {schema}.{table} LIMIT 1")
                if cursor.fetchone() is None:
                    empty.append(table)
            if empty:
                return [f"{label} baseline tables have no rows: {', '.join(empty)}"]
            return []
        except Exception as exc:
            return [f"{label} baseline cannot be checked: {exc}"]
        finally:
            if connection is not None:
                connection.close()


def wait_for_source_bootstrap(
    probe: SourceBootstrapProbe,
    retry_seconds: int,
    *,
    sleep: Callable[[float], None] = time.sleep,
    emit: Callable[[str], None] = print,
) -> None:
    """Wait without crashing until manual first-run source loading completes."""
    if retry_seconds <= 0:
        raise ValueError("SIMULATION_BOOTSTRAP_RETRY_SECONDS must be greater than zero")
    while True:
        report = probe.check()
        if report.ready:
            emit("Simulator source baseline is ready.")
            return
        emit(
            "Simulator is waiting for source bootstrap: "
            f"{report.message()}. Run generator/load.py once, then it will continue automatically."
        )
        sleep(retry_seconds)
