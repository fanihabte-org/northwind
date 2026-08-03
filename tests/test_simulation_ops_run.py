from datetime import date

from simulator.events import SourceEvent
from simulator.ops import OpsDailyPlanner
from simulator.ops_apply import OpsEventApplier
from simulator.ops_run import OpsRun
from simulator.fulfillment import FulfillmentPlanner
from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateStore


class Cursor:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.statements = []

    def execute(self, statement, parameters=None):
        self.statements.append(statement)

    def fetchone(self):
        return next(self.responses)

    def fetchall(self):
        return next(self.responses)


class Connection:
    def __init__(self, responses):
        self.cursor_instance = Cursor(responses)
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


def test_ops_run_plans_and_applies_due_order_after_crm(tmp_path) -> None:
    policy = SimulationPolicy()
    run_date = date(2026, 7, 29)
    store = SimulationStateStore(tmp_path / "simulation.duckdb", policy)
    store.initialize(date(2026, 7, 25))
    crm_date = date(2026, 7, 26)
    store.claim_run(crm_date, seed=1)
    store.record_events([
        SourceEvent.create(
            business_date=crm_date,
            source_system="crm",
            event_type="opportunity_stage_changed",
            entity_id="00610",
            payload={"AccountId": "001000010", "StageName": "Closed Won"},
        )
    ])
    store.mark_source_applied(crm_date, "crm", 1, 1)
    store.mark_source_applied(crm_date, "ops", 0, 0)
    store.mark_source_applied(crm_date, "erp", 0, 0)
    store.complete_run(crm_date)
    for intervening_date in (date(2026, 7, 27), date(2026, 7, 28)):
        store.claim_run(intervening_date, seed=1)
        for source in ("crm", "ops", "erp"):
            store.mark_source_applied(intervening_date, source, 0, 0)
        store.complete_run(intervening_date)
    store.claim_run(run_date, seed=1)
    store.mark_source_applied(run_date, "crm", 0, 0)

    snapshot_connection = Connection([(100,), []])
    apply_connection = Connection([None, None, None, (1, 100.0, 55.0), (900,)])
    fulfillment_snapshot_connection = Connection([(500,), [], []])
    fulfillment_apply_connection = Connection([])
    connections = iter([
        snapshot_connection,
        apply_connection,
        fulfillment_snapshot_connection,
        fulfillment_apply_connection,
    ])
    result = OpsRun(
        store, OpsDailyPlanner(policy), FulfillmentPlanner(policy), OpsEventApplier(), lambda: next(connections)
    ).run(run_date, seed=1)

    assert result.records == result.applied == 1
    assert store.source_run_state(run_date, "ops") == "applied"
    assert apply_connection.committed
    assert all(
        connection.closed
        for connection in (snapshot_connection, apply_connection, fulfillment_snapshot_connection, fulfillment_apply_connection)
    )
