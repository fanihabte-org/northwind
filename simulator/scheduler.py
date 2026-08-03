"""Sequential catch-up runner and command line entry point for daily simulation."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol, Sequence
from zoneinfo import ZoneInfo

from simulator.crm import CrmDailyPlanner
from simulator.crm_run import CrmRun
from simulator.crm_snapshot import CrmSnapshotReader
from simulator.crm_storage import CrmDeltaPublisher
from simulator.erp import ErpDailyPlanner
from simulator.erp_apply import ErpEventApplier
from simulator.erp_run import ErpRun
from simulator.fulfillment import FulfillmentPlanner
from simulator.ops import OpsDailyPlanner
from simulator.ops_apply import OpsEventApplier
from simulator.ops_run import OpsRun
from simulator.policy import SimulationPolicy
from simulator.state import SimulationStateError, SimulationStateStore


class DailySourceRun(Protocol):
    def run(self, run_date: date, seed: int) -> Any: ...


@dataclass(frozen=True)
class DailySimulationRunner:
    """Advance every missing date in strict CRM → Ops → ERP order."""

    state: SimulationStateStore
    crm: DailySourceRun
    ops: DailySourceRun
    erp: DailySourceRun

    def run_through(self, through_date: date, seed: int) -> list[date]:
        completed: list[date] = []
        for run_date in self.state.due_dates(through_date):
            self.crm.run(run_date, seed)
            self.ops.run(run_date, seed)
            self.erp.run(run_date, seed)
            if self.state.last_completed_date() != run_date:
                raise SimulationStateError("ERP did not complete the simulation date")
            completed.append(run_date)
        return completed


def build_runner(
    *,
    state_directory: Path,
    seed_directory: Path,
    ops_dsn: str,
    erp_dsn: str,
    policy: SimulationPolicy,
) -> DailySimulationRunner:
    """Assemble production source runners; connections are opened only per step."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised by deployment environment
        raise RuntimeError("install psycopg[binary] to run the daily simulator") from exc

    def connection_factory(dsn: str):
        return lambda: psycopg.connect(dsn, autocommit=False)

    delta_root = state_directory / "simulation" / "crm"
    state = SimulationStateStore(state_directory / "simulation" / "simulation.duckdb", policy)
    crm = CrmRun(
        state,
        CrmDailyPlanner(policy),
        CrmSnapshotReader(policy, delta_root),
        CrmDeltaPublisher(
            delta_root,
            {
                "accounts": seed_directory / "crm_accounts.parquet",
                "opportunities": seed_directory / "crm_opportunities.parquet",
            },
        ),
        seed_directory / "crm_accounts.parquet",
        seed_directory / "crm_opportunities.parquet",
    )
    ops_factory = connection_factory(ops_dsn)
    ops = OpsRun(
        state,
        OpsDailyPlanner(policy),
        FulfillmentPlanner(policy),
        OpsEventApplier(),
        ops_factory,
    )
    erp = ErpRun(state, ErpDailyPlanner(policy), ErpEventApplier(), ops_factory, connection_factory(erp_dsn))
    return DailySimulationRunner(state, crm, ops, erp)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must be YYYY-MM-DD") from exc


def main(argv: Sequence[str] | None = None) -> int:
    env = os.environ
    policy = SimulationPolicy()
    default_state = Path(env.get("FAKEFORCE_STATE_DIR", Path(__file__).resolve().parents[1] / "state"))
    default_seed = Path(env.get("FAKEFORCE_SEED_DIR", Path(__file__).resolve().parents[1] / "seed"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=_parse_date, default=env.get("SIMULATION_BASELINE_DATE"))
    parser.add_argument("--through", type=_parse_date)
    parser.add_argument("--seed", type=int, default=int(env.get("SIMULATION_SEED", "42")))
    parser.add_argument("--state-directory", type=Path, default=default_state)
    parser.add_argument("--seed-directory", type=Path, default=default_seed)
    parser.add_argument("--ops-dsn", default=env.get("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops"))
    parser.add_argument("--erp-dsn", default=env.get("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp"))
    args = parser.parse_args(argv)
    if args.baseline is None:
        parser.error("--baseline or SIMULATION_BASELINE_DATE is required for first initialization")
    if isinstance(args.baseline, str):  # argparse does not apply type conversion to string defaults.
        args.baseline = _parse_date(args.baseline)
    through_date = args.through or datetime.now(ZoneInfo(policy.timezone)).date()
    runner = build_runner(
        state_directory=args.state_directory,
        seed_directory=args.seed_directory,
        ops_dsn=args.ops_dsn,
        erp_dsn=args.erp_dsn,
        policy=policy,
    )
    runner.state.initialize(args.baseline)
    completed = runner.run_through(through_date, args.seed)
    print(f"completed {len(completed)} simulation date(s) through {through_date.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
