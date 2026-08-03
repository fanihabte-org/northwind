from __future__ import annotations

from generator import source_status as status


class Description:
    def __init__(self, *names: str) -> None:
        self.names = names

    @property
    def name(self) -> str:
        return self.names[0]


class Cursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, list[object] | None]] = []
        self.description = []

    def execute(self, query: str, parameters=None) -> None:
        self.queries.append((query, parameters))
        if "FROM simulation.audit_backfill_progress" in query:
            self.description = [Description("backfill_name"), Description("completed")]

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def fetchone(self):
        query = self.queries[-1][0]
        if "pg_database_size" in query:
            return (1024,)
        if "max(version)" in query:
            return (5,)
        if "FROM pg_class" in query:
            return (25, 4096)
        if "FROM simulation.applied_events" in query:
            return (7, "2026-08-03", "2026-08-03 08:00:00")
        if "to_regclass" in query:
            return ("simulation.audit_backfill_progress",)
        raise AssertionError(query)

    def fetchall(self):
        return [("revenue_postings", True)]


class Connection:
    def __init__(self) -> None:
        self.cursor_value = Cursor()

    def cursor(self) -> Cursor:
        return self.cursor_value


def test_source_status_reports_bounded_catalog_and_checkpoint_data(monkeypatch) -> None:
    monkeypatch.setattr(status, "discover_migrations", lambda system: [type("M", (), {"version": 5})()])
    connection = Connection()

    snapshot = status.source_status(connection, "erp")

    assert snapshot["database_bytes"] == 1024
    assert snapshot["migrations"] == {"expected_version": 5, "applied_version": 5, "current": True}
    assert len(snapshot["tables"]) == len(status.SOURCE_TABLES["erp"])
    assert snapshot["tables"][-1]["table"] == "erp.revenue_postings"
    assert snapshot["events"]["applied_events"] == 7
    assert snapshot["audit_backfill"] == [{"backfill_name": "revenue_postings", "completed": True}]


def test_source_status_uses_catalog_estimates_not_large_table_counts() -> None:
    assert all("COUNT(*)" not in table for table in status.SOURCE_TABLES.values())
