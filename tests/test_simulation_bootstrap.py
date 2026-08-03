from pathlib import Path

import pandas as pd

from simulator.bootstrap import ERP_TABLES, OPS_TABLES, SourceBootstrapProbe, wait_for_source_bootstrap


class Cursor:
    def __init__(self, tables: set[str], populated: set[str]) -> None:
        self.tables = tables
        self.populated = populated
        self.result: list[tuple[str]] = []

    def execute(self, query: str, parameters=None) -> None:
        if "information_schema.tables" in query:
            self.result = [(table,) for table in self.tables]
            return
        table = query.split("FROM ", 1)[1].split(" LIMIT", 1)[0].split(".", 1)[1]
        self.result = [("row",)] if table in self.populated else []

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None


class Connection:
    def __init__(self, tables: set[str], populated: set[str]) -> None:
        self.cursor_value = Cursor(tables, populated)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


def _seed(directory: Path) -> None:
    pd.DataFrame([{"Id": "001000000000000001"}]).to_parquet(directory / "crm_accounts.parquet")
    pd.DataFrame([{"Id": "006000000000000001"}]).to_parquet(directory / "crm_opportunities.parquet")


def test_bootstrap_probe_requires_seed_and_populated_source_tables(tmp_path: Path) -> None:
    _seed(tmp_path)
    ops = Connection(set(OPS_TABLES), set(OPS_TABLES))
    erp = Connection(set(ERP_TABLES), set(ERP_TABLES))

    report = SourceBootstrapProbe(tmp_path, lambda: ops, lambda: erp).check()

    assert report.ready
    assert ops.closed and erp.closed


def test_bootstrap_probe_reports_missing_or_empty_prerequisites(tmp_path: Path) -> None:
    ops = Connection(set(OPS_TABLES) - {"orders"}, set())
    erp = Connection(set(ERP_TABLES), set())

    report = SourceBootstrapProbe(tmp_path, lambda: ops, lambda: erp).check()

    assert not report.ready
    assert "CRM seed file is missing" in report.message()
    assert "Ops baseline tables are missing: orders" in report.message()
    assert "ERP baseline tables have no rows" in report.message()


def test_bootstrap_waits_and_then_returns_when_the_load_finishes() -> None:
    reports = iter([False, True])

    class Probe:
        def check(self):
            ready = next(reports)
            from simulator.bootstrap import BootstrapReport

            return BootstrapReport(ready, () if ready else ("Ops baseline tables are missing: orders",))

    delays: list[float] = []
    messages: list[str] = []
    wait_for_source_bootstrap(Probe(), 15, sleep=delays.append, emit=messages.append)  # type: ignore[arg-type]

    assert delays == [15]
    assert "waiting for source bootstrap" in messages[0]
    assert messages[-1] == "Simulator source baseline is ready."
