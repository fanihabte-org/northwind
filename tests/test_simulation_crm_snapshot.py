from pathlib import Path

import pandas as pd

from simulator.crm import CrmDailyPlanner
from simulator.config import SimulatorDuckDBSettings
from simulator.crm_snapshot import CrmSnapshotReader
from simulator.policy import SimulationPolicy


def test_snapshot_reader_uses_metadata_and_bounded_samples(tmp_path: Path) -> None:
    accounts_path = tmp_path / "accounts.parquet"
    opportunities_path = tmp_path / "opportunities.parquet"
    pd.DataFrame(
        [
                {"Id": f"001{i:015d}", "OwnerId": "REP-0001", "LastModifiedDate": "2026-07-24T08:00:00.000+0000"}
            for i in range(1, 251)
        ]
    ).to_parquet(accounts_path, index=False)
    pd.DataFrame(
        [
                {"Id": f"006{i:015d}", "AccountId": f"001{i:015d}", "OwnerId": "REP-0001", "StageName": "Prospecting", "LastModifiedDate": "2026-07-24T08:00:00.000+0000"}
            for i in range(1, 401)
        ]
    ).to_parquet(opportunities_path, index=False)

    policy = SimulationPolicy(annual_growth_rate=0.08)
    settings = SimulatorDuckDBSettings.from_env(
        tmp_path / "state",
        {
            "SIMULATION_DUCKDB_MEMORY_LIMIT": "128MB",
            "SIMULATION_DUCKDB_MAX_TEMP_SIZE": "1GB",
            "SIMULATION_DUCKDB_THREADS": "1",
        },
    )
    reader = CrmSnapshotReader(policy, duckdb_settings=settings)
    with reader._connect() as connection:
        assert connection.execute("SELECT current_setting('memory_limit')").fetchone()[0] == "122.0 MiB"
        assert connection.execute("SELECT current_setting('threads')").fetchone()[0] == 1
        assert connection.execute("SELECT current_setting('max_temp_directory_size')").fetchone()[0] == "953.6 MiB"

    snapshot = reader.read(
        accounts_path, opportunities_path, seed=42
    )
    events = CrmDailyPlanner(policy).plan(pd.Timestamp("2026-07-25").date(), 42, snapshot)

    assert snapshot.account_population == 250
    assert snapshot.open_opportunity_population == 400
    assert snapshot.account_next_sequence == 251
    assert snapshot.opportunity_next_sequence == 401
    assert len(snapshot.opportunities) == 1
    assert any(event.event_type == "opportunity_stalled" for event in events)
    assert settings.temp_directory.exists()


def test_snapshot_reader_resolves_small_deltas_without_windowing_the_base(tmp_path: Path) -> None:
    accounts_path = tmp_path / "accounts.parquet"
    opportunities_path = tmp_path / "opportunities.parquet"
    pd.DataFrame(
        [
            {"Id": f"001{i:015d}", "OwnerId": "REP-OLD", "LastModifiedDate": "2026-07-24T08:00:00.000+0000"}
            for i in range(1, 251)
        ]
    ).to_parquet(accounts_path, index=False)
    pd.DataFrame(
        [
            {"Id": f"006{i:015d}", "AccountId": f"001{i:015d}", "OwnerId": "REP-0001", "StageName": "Prospecting", "LastModifiedDate": "2026-07-24T08:00:00.000+0000"}
            for i in range(1, 251)
        ]
    ).to_parquet(opportunities_path, index=False)
    delta_root = tmp_path / "state" / "crm"
    account_delta = delta_root / "accounts" / "business_date=2026-07-25" / "delta.parquet"
    account_delta.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {"Id": "001000000000000001", "OwnerId": "REP-NEW", "LastModifiedDate": "2026-07-25T08:00:00.000+0000"},
            {"Id": "001000000000000251", "OwnerId": "REP-NEW", "LastModifiedDate": "2026-07-25T08:00:00.000+0000"},
        ]
    ).to_parquet(account_delta, index=False)

    reader = CrmSnapshotReader(SimulationPolicy(), delta_root=delta_root)
    snapshot = reader.read(accounts_path, opportunities_path, seed=42)
    with reader._connect() as connection:
        current_rows = connection.execute(
            f'''SELECT "Id", "OwnerId" FROM {reader._relation(accounts_path, "accounts")}
            WHERE "Id" IN (?, ?)''',
            ["001000000000000001", "001000000000000251"],
        ).fetchall()

    assert snapshot.account_population == 251
    owners_by_id = {str(record_id): owner_id for record_id, owner_id in current_rows}
    assert owners_by_id["001000000000000001"] == "REP-NEW"
    assert owners_by_id["001000000000000251"] == "REP-NEW"
