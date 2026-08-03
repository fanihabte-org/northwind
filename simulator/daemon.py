"""Long-running midnight scheduler for the CRM, Ops, and ERP simulator."""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

from simulator.bootstrap import SourceBootstrapProbe, wait_for_source_bootstrap
from simulator.policy import SimulationPolicy
from simulator.scheduler import DailySimulationRunner, _parse_date, build_runner


def seconds_until_next_midnight(now: datetime, timezone: str) -> float:
    """Return the real elapsed seconds to the next local midnight, including DST."""
    local_now = now.astimezone(ZoneInfo(timezone))
    tomorrow = local_now.date() + timedelta(days=1)
    next_midnight = datetime.combine(tomorrow, clock_time.min, tzinfo=ZoneInfo(timezone))
    return max(1.0, next_midnight.timestamp() - local_now.timestamp())


def _positive_seconds(value: int, name: str) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def run_forever(
    runner: DailySimulationRunner,
    *,
    seed: int,
    policy: SimulationPolicy,
    retry_initial_seconds: int = 60,
    retry_max_seconds: int = 3600,
    now: Callable[[ZoneInfo], datetime] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    emit: Callable[[str], None] = print,
) -> None:
    """Run catch-up at midnight, retrying transient source failures with backoff."""
    _positive_seconds(retry_initial_seconds, "SIMULATION_RETRY_INITIAL_SECONDS")
    _positive_seconds(retry_max_seconds, "SIMULATION_RETRY_MAX_SECONDS")
    if retry_max_seconds < retry_initial_seconds:
        raise ValueError("SIMULATION_RETRY_MAX_SECONDS cannot be below the initial retry delay")
    timezone = ZoneInfo(policy.timezone)
    clock = now or (lambda tz: datetime.now(tz))
    consecutive_failures = 0
    while True:
        current = clock(timezone)
        try:
            runner.run_through(current.date(), seed)
        except Exception as exc:
            delay = min(retry_initial_seconds * (2**consecutive_failures), retry_max_seconds)
            consecutive_failures += 1
            emit(
                f"Simulator run failed for {current.date().isoformat()} "
                f"({type(exc).__name__}: {exc}); retrying in {delay} seconds."
            )
            sleep(delay)
            continue
        consecutive_failures = 0
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
    parser.add_argument(
        "--bootstrap-retry-seconds",
        type=int,
        default=int(env.get("SIMULATION_BOOTSTRAP_RETRY_SECONDS", "60")),
        help="seconds to wait between baseline readiness checks",
    )
    parser.add_argument(
        "--retry-initial-seconds",
        type=int,
        default=int(env.get("SIMULATION_RETRY_INITIAL_SECONDS", "60")),
        help="initial retry delay after a failed daily run",
    )
    parser.add_argument(
        "--retry-max-seconds",
        type=int,
        default=int(env.get("SIMULATION_RETRY_MAX_SECONDS", "3600")),
        help="maximum retry delay after repeated failed daily runs",
    )
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
    probe = SourceBootstrapProbe.from_dsns(args.seed_directory, args.ops_dsn, args.erp_dsn)
    wait_for_source_bootstrap(probe, args.bootstrap_retry_seconds)
    runner.state.initialize(_parse_date(args.baseline))
    if args.once:
        runner.run_through(datetime.now(ZoneInfo(policy.timezone)).date(), args.seed)
        return 0
    run_forever(
        runner,
        seed=args.seed,
        policy=policy,
        retry_initial_seconds=args.retry_initial_seconds,
        retry_max_seconds=args.retry_max_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
