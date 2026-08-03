"""Bounded DuckDB reads of the current CRM state for daily event planning."""

from __future__ import annotations

from pathlib import Path

import duckdb

from simulator.config import SimulatorDuckDBSettings
from simulator.crm import CrmSnapshot, OPEN_STAGES
from simulator.policy import SimulationPolicy


class CrmSnapshotReader:
    def __init__(
        self,
        policy: SimulationPolicy,
        delta_root: Path | None = None,
        duckdb_settings: SimulatorDuckDBSettings | None = None,
    ) -> None:
        self.policy = policy
        self.delta_root = delta_root
        self.duckdb_settings = duckdb_settings

    def read(self, accounts_path: Path, opportunities_path: Path, seed: int) -> CrmSnapshot:
        with self._connect() as conn:
            accounts_relation = self._relation(accounts_path, "accounts")
            opportunities_relation = self._relation(opportunities_path, "opportunities")
            account_population, account_next_sequence = self._population_and_next_id(
                conn, accounts_relation
            )
            open_population, opportunity_next_sequence = self._open_population_and_next_id(
                conn, opportunities_relation
            )
            account_sample_size = min(
                account_population,
                max(100, self.policy.proportional_daily_volume(account_population) * 2),
            )
            transition_sample_size = min(
                open_population, self.policy.proportional_daily_volume(open_population)
            )
            accounts = self._rows(
                conn,
                f'SELECT "Id", "OwnerId" FROM {accounts_relation} ORDER BY hash("Id", ?) LIMIT ?',
                [seed, account_sample_size],
            )
            opportunities = self._rows(
                conn,
                f"""
                SELECT "Id", "AccountId", "OwnerId", "StageName"
                FROM {opportunities_relation}
                WHERE "StageName" IN (?, ?, ?, ?)
                ORDER BY hash("Id", ?) LIMIT ?
                """,
                [*OPEN_STAGES, seed, transition_sample_size],
            )
            owners = self._rows(
                conn,
                f'SELECT DISTINCT "OwnerId" FROM {accounts_relation} WHERE "OwnerId" IS NOT NULL LIMIT 1024',
                [],
            )
        return CrmSnapshot(
            accounts=tuple(accounts),
            opportunities=tuple(opportunities),
            account_population=account_population,
            open_opportunity_population=open_population,
            account_next_sequence=account_next_sequence,
            opportunity_next_sequence=opportunity_next_sequence,
            owner_ids=tuple(row["OwnerId"] for row in owners),
        )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self.duckdb_settings is None:
            return duckdb.connect()
        self.duckdb_settings.temp_directory.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(config=self.duckdb_settings.duckdb_config())

    @staticmethod
    def _population_and_next_id(conn: duckdb.DuckDBPyConnection, relation: str) -> tuple[int, int]:
        count, next_id = conn.execute(
            f'SELECT count(*), coalesce(max(try_cast(substr("Id", 4) AS BIGINT)), 0) + 1 FROM {relation}',
        ).fetchone()
        return int(count), int(next_id)

    @staticmethod
    def _open_population_and_next_id(conn: duckdb.DuckDBPyConnection, relation: str) -> tuple[int, int]:
        count = conn.execute(
            f"""
            SELECT count(*)
            FROM {relation} WHERE "StageName" IN (?, ?, ?, ?)
            """,
            [*OPEN_STAGES],
        ).fetchone()[0]
        next_id = conn.execute(
            f'SELECT coalesce(max(try_cast(substr("Id", 4) AS BIGINT)), 0) + 1 FROM {relation}',
        ).fetchone()[0]
        return int(count), int(next_id)

    @staticmethod
    def _rows(
        conn: duckdb.DuckDBPyConnection, sql: str, parameters: list[object]
    ) -> list[dict[str, object]]:
        cursor = conn.execute(sql, parameters)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def _relation(self, base_path: Path, object_name: str) -> str:
        """Return the latest view without windowing the full base data set.

        The immutable seed already has one row per Id. Only daily delta files
        can contain competing versions, so rank that bounded data and exclude
        overridden base rows with an anti-join. The former implementation
        ranked the base plus every delta, which could create a large DuckDB
        window working set during high-scale catch-up.
        """
        delta_paths: list[Path] = []
        if self.delta_root is not None:
            delta_paths = sorted((self.delta_root / object_name).glob("**/*.parquet"))
        base_reader = f"read_parquet({self._quote(base_path)})"
        if not delta_paths:
            return base_reader
        literals = ", ".join(self._quote(path) for path in delta_paths)
        # Delta files live in business_date=... directories. Do not expose that
        # storage partition as a CRM column when unioning with the base file.
        delta_reader = (
            f"read_parquet([{literals}], union_by_name = true, hive_partitioning = false)"
        )
        return (
            "(WITH latest_delta AS ("
            "SELECT * EXCLUDE (__sim_version_rank) FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY \"Id\" ORDER BY \"LastModifiedDate\" DESC) "
            f"AS __sim_version_rank FROM {delta_reader}) WHERE __sim_version_rank = 1"
            ") "
            f"SELECT base.* FROM {base_reader} AS base "
            "WHERE NOT EXISTS (SELECT 1 FROM latest_delta AS delta "
            "WHERE delta.\"Id\" = base.\"Id\") "
            "UNION ALL SELECT * FROM latest_delta)"
        )

    @staticmethod
    def _quote(path: Path) -> str:
        return "'" + str(path).replace("'", "''") + "'"
