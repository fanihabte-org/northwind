from datetime import date, timedelta

from simulator.erp import ErpDailyPlanner
from simulator.erp_apply import ErpEventApplier
from simulator.erp_run import ErpRun
from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateStore


class Cursor:
    def __init__(self, one_rows=(), many_rows=()) -> None:
        self.one_rows = iter(one_rows)
        self.many_rows = iter(many_rows)
        self.queries: list[str] = []

    def execute(self, statement, parameters=None):
        self.queries.append(statement)

    def fetchone(self):
        return next(self.one_rows)

    def fetchall(self):
        return next(self.many_rows)


class Connection:
    def __init__(self, one_rows=(), many_rows=()) -> None:
        self.cursor_instance = Cursor(one_rows, many_rows)
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        raise AssertionError("should not roll back")

    def close(self):
        self.closed = True


def test_erp_run_reads_ops_then_posts_and_completes_day(tmp_path) -> None:
    policy = SimulationPolicy()
    run_date = next(
        date(2026, 7, 10) + timedelta(days=offset)
        for offset in range(7)
        if not policy.is_weekly_sla_breach_date(date(2026, 7, 10) + timedelta(days=offset), 1)
    )
    store = SimulationStateStore(tmp_path / "simulation.duckdb", policy)
    store.initialize(run_date - timedelta(days=1))
    store.claim_run(run_date, seed=1)
    store.mark_source_applied(run_date, "crm", 0, 0)
    store.mark_source_applied(run_date, "ops", 0, 0)

    ops = Connection(many_rows=[[(1, "OPS-000000000001", 100, run_date - timedelta(days=10), "US01", "USD", 125.5)]])
    snapshot = Connection(one_rows=[(900,)], many_rows=[[], [("US01", "USD")]])
    apply = Connection(one_rows=[None, None, ("USD",), ("4000",), ("CC-US001",)])
    erp_connections = iter([snapshot, apply])
    reconciled_before_completion = []

    def reconcile(day):
        reconciled_before_completion.append((day, store.last_completed_date()))

    result = ErpRun(
        store,
        ErpDailyPlanner(policy),
        ErpEventApplier(),
        lambda: ops,
        lambda: next(erp_connections),
        reconcile,
    ).run(run_date, seed=1)

    assert result.records == result.applied == 1
    assert store.last_completed_date() == run_date
    assert reconciled_before_completion == [(run_date, run_date - timedelta(days=1))]
    assert apply.committed
    assert ops.closed and snapshot.closed and apply.closed
