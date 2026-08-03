from datetime import datetime
from zoneinfo import ZoneInfo

from simulator.daemon import seconds_until_next_midnight
from simulator.daemon import run_forever


def test_daemon_waits_until_next_local_midnight() -> None:
    now = datetime(2026, 7, 1, 23, 59, 30, tzinfo=ZoneInfo("America/Los_Angeles"))

    assert seconds_until_next_midnight(now, "America/Los_Angeles") == 30


def test_daemon_accounts_for_spring_daylight_saving_transition() -> None:
    midnight_before_spring_forward = datetime(
        2026, 3, 8, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )

    assert seconds_until_next_midnight(midnight_before_spring_forward, "America/Los_Angeles") == 23 * 60 * 60


def test_daemon_retries_a_transient_failure_then_resets_to_midnight_schedule() -> None:
    class Runner:
        def __init__(self) -> None:
            self.calls = 0

        def run_through(self, run_date, seed) -> None:
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("Ops is restarting")

    class StopLoop(Exception):
        pass

    delays = []
    messages = []

    def sleep(seconds: float) -> None:
        delays.append(seconds)
        if len(delays) == 2:
            raise StopLoop

    now = datetime(2026, 7, 1, 23, 59, 30, tzinfo=ZoneInfo("America/Los_Angeles"))
    try:
        run_forever(
            Runner(),
            seed=42,
            policy=type("Policy", (), {"timezone": "America/Los_Angeles"})(),
            retry_initial_seconds=60,
            retry_max_seconds=300,
            now=lambda _: now,
            sleep=sleep,
            emit=messages.append,
        )
    except StopLoop:
        pass

    assert delays == [60, 30]
    assert "ConnectionError: Ops is restarting" in messages[0]
