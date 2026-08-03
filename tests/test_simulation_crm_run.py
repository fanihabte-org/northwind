from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from simulator.crm import CrmDailyPlanner
from simulator.crm_run import CrmRun
from simulator.crm_snapshot import CrmSnapshotReader
from simulator.crm_storage import CrmDeltaPublisher
from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateStore


def test_crm_run_reuses_ledger_events_after_restart(tmp_path) -> None:
    account_schema = pa.schema([("Id", pa.string()), ("OwnerId", pa.string()), ("LastModifiedDate", pa.string()), ("IsDeleted", pa.bool_())])
    opportunity_schema = pa.schema([("Id", pa.string()), ("AccountId", pa.string()), ("OwnerId", pa.string()), ("StageName", pa.string()), ("LastModifiedDate", pa.string()), ("IsDeleted", pa.bool_())])
    accounts = tmp_path / "accounts.parquet"
    opportunities = tmp_path / "opportunities.parquet"
    pq.write_table(pa.Table.from_pylist([{"Id": "001000000000000001", "OwnerId": "REP-0001", "LastModifiedDate": "2026-07-24T08:00:00.000+0000", "IsDeleted": False}], schema=account_schema), accounts)
    pq.write_table(pa.Table.from_pylist([{"Id": "006000000000000001", "AccountId": "001000000000000001", "OwnerId": "REP-0001", "StageName": "Prospecting", "LastModifiedDate": "2026-07-24T08:00:00.000+0000", "IsDeleted": False}], schema=opportunity_schema), opportunities)
    run_date = date(2026, 7, 25)
    policy = SimulationPolicy()
    delta_root = tmp_path / "deltas"
    state = SimulationStateStore(tmp_path / "simulator.duckdb", policy)
    state.initialize(run_date - timedelta(days=1))
    run = CrmRun(
        state,
        CrmDailyPlanner(policy),
        CrmSnapshotReader(policy, delta_root),
        CrmDeltaPublisher(delta_root, {"accounts": accounts, "opportunities": opportunities}),
        accounts,
        opportunities,
    )

    first = run.run(run_date, seed=42)
    second = run.run(run_date, seed=42)

    assert first.records == second.records
    assert first.parts == second.parts
    assert state.source_run_state(run_date, "crm") == "applied"
