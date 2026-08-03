from __future__ import annotations

from pathlib import Path

import pytest

from generator.migrate import MigrationError, apply_migrations, discover_migrations


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection
        self.results: list[tuple[object, ...]] = []
        self.statements: list[str] = []

    def execute(self, query: str, parameters=None) -> None:
        self.statements.append(query)
        if "SELECT version, filename, checksum" in query:
            self.results = list(self.connection.applied)
        elif "INSERT INTO simulation.schema_migrations" in query:
            self.connection.applied.append(tuple(parameters))

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.results


class Connection:
    def __init__(self, applied: list[tuple[object, ...]] | None = None) -> None:
        self.applied = [] if applied is None else applied
        self.cursor_value = Cursor(self)

    def cursor(self) -> Cursor:
        return self.cursor_value


def _migration(directory: Path, filename: str, body: str = "SELECT 1;") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / filename).write_text(body, encoding="utf-8")


def test_discovery_reads_the_independent_versioned_source_chains() -> None:
    ops = discover_migrations("ops")
    erp = discover_migrations("erp")

    assert [migration.filename for migration in ops] == [
        "001_initial_schema.sql",
        "002_add_audit_metadata.sql",
        "003_add_audit_backfill_state.sql",
        "004_enforce_audit_metadata.sql",
    ]
    assert [migration.filename for migration in erp] == [
        "001_initial_schema.sql",
        "002_add_audit_metadata.sql",
        "003_add_audit_backfill_state.sql",
        "004_enforce_audit_metadata.sql",
    ]
    assert "CREATE TABLE IF NOT EXISTS ops.orders" in ops[0].sql
    assert "ADD COLUMN IF NOT EXISTS created_at" in ops[1].sql
    assert "simulation.audit_backfill_progress" in ops[2].sql
    assert "ALTER COLUMN created_at SET NOT NULL" in ops[3].sql
    assert "CREATE TABLE IF NOT EXISTS erp.revenue_postings" in erp[0].sql
    assert "ADD COLUMN IF NOT EXISTS created_at" in erp[1].sql
    assert "simulation.audit_backfill_progress" in erp[2].sql
    assert "ALTER COLUMN created_at SET NOT NULL" in erp[3].sql
    assert "ck_revenue_postings_created_at" in erp[3].sql


def test_apply_records_pending_migrations_and_is_idempotent(tmp_path: Path) -> None:
    _migration(tmp_path / "ops", "001_create_table.sql")
    _migration(tmp_path / "ops", "002_add_index.sql")
    connection = Connection()

    applied = apply_migrations(connection, "ops", tmp_path)

    assert [migration.version for migration in applied] == [1, 2]
    assert [(row[0], row[1]) for row in connection.applied] == [
        (1, "001_create_table.sql"),
        (2, "002_add_index.sql"),
    ]
    assert apply_migrations(connection, "ops", tmp_path) == []


def test_apply_rejects_an_edited_applied_migration(tmp_path: Path) -> None:
    _migration(tmp_path / "ops", "001_create_table.sql", "SELECT 2;")
    connection = Connection([(1, "001_create_table.sql", "0" * 64)])

    with pytest.raises(MigrationError, match="does not match"):
        apply_migrations(connection, "ops", tmp_path)


def test_discovery_rejects_a_gap_in_migration_versions(tmp_path: Path) -> None:
    _migration(tmp_path / "ops", "001_create_table.sql")
    _migration(tmp_path / "ops", "003_add_index.sql")

    with pytest.raises(MigrationError, match="contiguous"):
        discover_migrations("ops", tmp_path)
