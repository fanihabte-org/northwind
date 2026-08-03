"""Long-running midnight scheduler for the CRM, Ops, and ERP simulator."""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

from simulator.policy import SimulationPolicy
from simulator.scheduler import DailySimulationRunner, _parse_date, build_runner


def seconds_until_next_midnight(now: datetime, timezone: str) -> float:
    """Return the real elapsed seconds to the next local midnight, including DST."""
    local_now = now.astimezone(ZoneInfo(timezone))
    tomorrow = local_now.date() + timedelta(days=1)
    next_midnight = datetime.combine(tomorrow, clock_time.min, tzinfo=ZoneInfo(timezone))
    return max(1.0, next_midnight.timestamp() - local_now.timestamp())


def run_forever(
    runner: DailySimulationRunner,
    *,
    seed: int,
    policy: SimulationPolicy,
    now: Callable[[ZoneInfo], datetime] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Run immediately for catch-up, then once at every local midnight."""
    timezone = ZoneInfo(policy.timezone)
    clock = now or (lambda tz: datetime.now(tz))
    while True:
        current = clock(timezone)
        runner.run_through(current.date(), seed)
        sleep(seconds_until_next_midnight(current, policy.timezone))


def main(argv: Sequence[str] | None = None) -> int:
    env = os.environ
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run catch-up once, then exit")
    parser.add_argument("--baseline", default=env.get("SIMULATION_BASELINE_DATE"))
    parser.add_argument("--seed", type=int, default=int(env.get("SIMULATION_SEED", "42")))
    parser.add_argument(
        "--state-directory",
        type=Path,
        default=Path(env.get("FAKEFORCE_STATE_DIR", Path(__file__).resolve().parents[1] / "state")),
    )
    parser.add_argument(
        "--seed-directory",
        type=Path,
        default=Path(env.get("FAKEFORCE_SEED_DIR", Path(__file__).resolve().parents[1] / "seed")),
    )
    parser.add_argument("--ops-dsn", default=env.get("OPS_PG_DSN", "postgresql://ops:ops@ops:5432/ops"))
    parser.add_argument("--erp-dsn", default=env.get("ERP_PG_DSN", "postgresql://erp:erp@erp:5432/erp"))
    args = parser.parse_args(argv)
    if not args.baseline:
        parser.error("SIMULATION_BASELINE_DATE or --baseline is required")
    policy = SimulationPolicy()
    runner = build_runner(
        state_directory=args.state_directory,
        seed_directory=args.seed_directory,
        ops_dsn=args.ops_dsn,
        erp_dsn=args.erp_dsn,
        policy=policy,
    )
    runner.state.initialize(_parse_date(args.baseline))
    if args.once:
        runner.run_through(datetime.now(ZoneInfo(policy.timezone)).date(), args.seed)
        return 0
    run_forever(runner, seed=args.seed, policy=policy)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
