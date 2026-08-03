"""Durable, ordered control state for the daily source-system simulator."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Iterator, Literal

import duckdb

from simulator.policy import SimulationPolicy
from simulator.events import SourceEvent


SourceSystem = Literal["crm", "ops", "erp"]
SOURCES: tuple[SourceSystem, ...] = ("crm", "ops", "erp")


class SimulationStateError(RuntimeError):
    pass


class SimulationStateStore:
    """Tracks completed business dates separately from partial source application.

    A date advances only after CRM, Ops, and ERP have all acknowledged their
    work.  This is the boundary that lets a restart replay a partial day safely.
    """

    def __init__(self, database_path: Path, policy: SimulationPolicy) -> None:
        self.database_path = database_path
        self.policy = policy

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.database_path))
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self, baseline_completed_date: date) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_metadata (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_runs (
                    run_date DATE PRIMARY KEY,
                    state VARCHAR NOT NULL,
                    weekly_sla_breach BOOLEAN NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    failure_message VARCHAR
                );
                CREATE TABLE IF NOT EXISTS simulation_source_runs (
                    run_date DATE NOT NULL,
                    source_system VARCHAR NOT NULL,
                    state VARCHAR NOT NULL,
                    records_generated BIGINT NOT NULL DEFAULT 0,
                    records_applied BIGINT NOT NULL DEFAULT 0,
                    applied_at TIMESTAMP,
                    PRIMARY KEY (run_date, source_system),
                    CHECK (source_system IN ('crm', 'ops', 'erp'))
                );
                CREATE TABLE IF NOT EXISTS simulation_events (
                    event_id VARCHAR PRIMARY KEY,
                    business_date DATE NOT NULL,
                    source_system VARCHAR NOT NULL,
                    event_type VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    payload_json VARCHAR NOT NULL,
                    available_on DATE NOT NULL,
                    anomaly_type VARCHAR,
                    applied_at TIMESTAMP,
                    CHECK (source_system IN ('crm', 'ops', 'erp'))
                );
                """
            )
            existing = conn.execute(
                "SELECT value FROM simulation_metadata WHERE key = 'last_completed_date'"
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO simulation_metadata VALUES ('last_completed_date', ?)",
                    [baseline_completed_date.isoformat()],
                )
            elif existing[0] != baseline_completed_date.isoformat():
                raise SimulationStateError(
                    "simulation state already belongs to a different baseline date"
                )

    def last_completed_date(self) -> date:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT value FROM simulation_metadata WHERE key = 'last_completed_date'"
            ).fetchone()
        if row is None:
            raise SimulationStateError("simulation state has not been initialized")
        return date.fromisoformat(row[0])

    def due_dates(self, through_date: date) -> list[date]:
        current = self.last_completed_date()
        if through_date <= current:
            return []
        return [date.fromordinal(day) for day in range(current.toordinal() + 1, through_date.toordinal() + 1)]

    def claim_run(self, run_date: date, seed: int) -> bool:
        """Create or reclaim an incomplete date without advancing the watermark."""
        with self.connection() as conn:
            last_completed = date.fromisoformat(
                conn.execute(
                    "SELECT value FROM simulation_metadata WHERE key = 'last_completed_date'"
                ).fetchone()[0]
            )
            if run_date > last_completed.fromordinal(last_completed.toordinal() + 1):
                raise SimulationStateError("simulation dates must be claimed in order")
            row = conn.execute(
                "SELECT state FROM simulation_runs WHERE run_date = ?", [run_date]
            ).fetchone()
            if row is not None and row[0] == "completed":
                return False
            if row is None:
                conn.execute(
                    "INSERT INTO simulation_runs VALUES (?, 'in_progress', ?, current_timestamp, NULL, NULL)",
                    [run_date, self.policy.is_weekly_sla_breach_date(run_date, seed)],
                )
                for source in SOURCES:
                    conn.execute(
                        "INSERT INTO simulation_source_runs (run_date, source_system, state) VALUES (?, ?, 'pending')",
                        [run_date, source],
                    )
            else:
                conn.execute(
                    "UPDATE simulation_runs SET state = 'in_progress', failure_message = NULL WHERE run_date = ?",
                    [run_date],
                )
            return True

    def mark_source_applied(
        self, run_date: date, source: SourceSystem, records_generated: int, records_applied: int
    ) -> None:
        if records_generated < 0 or records_applied < 0:
            raise ValueError("record counts cannot be negative")
        with self.connection() as conn:
            row = conn.execute(
                "SELECT state FROM simulation_source_runs WHERE run_date = ? AND source_system = ?",
                [run_date, source],
            ).fetchone()
            if row is None:
                raise SimulationStateError("source run has not been claimed")
            source_index = SOURCES.index(source)
            if source_index:
                preceding = conn.execute(
                    """
                    SELECT count(*) FROM simulation_source_runs
                    WHERE run_date = ? AND source_system IN ({}) AND state != 'applied'
                    """.format(", ".join("?" for _ in SOURCES[:source_index])),
                    [run_date, *SOURCES[:source_index]],
                ).fetchone()[0]
                if preceding:
                    raise SimulationStateError("source systems must apply in CRM, Ops, ERP order")
            conn.execute(
                """
                UPDATE simulation_source_runs
                SET state = 'applied', records_generated = ?, records_applied = ?, applied_at = current_timestamp
                WHERE run_date = ? AND source_system = ?
                """,
                [records_generated, records_applied, run_date, source],
            )

    def complete_run(self, run_date: date) -> None:
        with self.connection() as conn:
            last_completed = date.fromisoformat(
                conn.execute(
                    "SELECT value FROM simulation_metadata WHERE key = 'last_completed_date'"
                ).fetchone()[0]
            )
            if run_date != date.fromordinal(last_completed.toordinal() + 1):
                raise SimulationStateError("simulation dates must complete in order")
            pending = conn.execute(
                "SELECT count(*) FROM simulation_source_runs WHERE run_date = ? AND state != 'applied'",
                [run_date],
            ).fetchone()[0]
            if pending:
                raise SimulationStateError("all source systems must apply before completion")
            conn.execute(
                "UPDATE simulation_runs SET state = 'completed', completed_at = current_timestamp WHERE run_date = ?",
                [run_date],
            )
            conn.execute(
                "UPDATE simulation_metadata SET value = ? WHERE key = 'last_completed_date'",
                [run_date.isoformat()],
            )

    def source_run_state(self, run_date: date, source: SourceSystem) -> str:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT state FROM simulation_source_runs WHERE run_date = ? AND source_system = ?",
                [run_date, source],
            ).fetchone()
        if row is None:
            raise SimulationStateError("source run has not been claimed")
        return row[0]

    def record_events(self, events: list[SourceEvent]) -> None:
        """Persist deterministic planned events; retries do not duplicate them."""
        with self.connection() as conn:
            for event in events:
                claimed = conn.execute(
                    "SELECT 1 FROM simulation_source_runs WHERE run_date = ? AND source_system = ?",
                    [event.business_date, event.source_system],
                ).fetchone()
                if claimed is None:
                    raise SimulationStateError("events may be recorded only for a claimed source run")
                conn.execute(
                    """
                    INSERT INTO simulation_events (
                        event_id, business_date, source_system, event_type, entity_id,
                        payload_json, available_on, anomaly_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    [
                        event.event_id,
                        event.business_date,
                        event.source_system,
                        event.event_type,
                        event.entity_id,
                        event.payload_json(),
                        event.available_on,
                        event.anomaly_type,
                    ],
                )

    def event_count(self, run_date: date, source: SourceSystem) -> int:
        with self.connection() as conn:
            return conn.execute(
                "SELECT count(*) FROM simulation_events WHERE business_date = ? AND source_system = ?",
                [run_date, source],
            ).fetchone()[0]

    def events_for(self, run_date: date, source: SourceSystem) -> list[SourceEvent]:
        return self.events_through(run_date, source, exact_date=True)

    def events_through(
        self, through_date: date, source: SourceSystem, *, exact_date: bool = False
    ) -> list[SourceEvent]:
        """Return deterministic events for one source through a business date."""
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT event_id, business_date, source_system, event_type, entity_id,
                       payload_json, available_on, anomaly_type
                FROM simulation_events
                WHERE business_date {} ? AND source_system = ?
                ORDER BY event_id
                """.format("=" if exact_date else "<="),
                [through_date, source],
            ).fetchall()
        return [
            SourceEvent(
                event_id=row[0],
                business_date=row[1],
                source_system=row[2],
                event_type=row[3],
                entity_id=row[4],
                payload=json.loads(row[5]),
                available_on=row[6],
                anomaly_type=row[7],
            )
            for row in rows
        ]
