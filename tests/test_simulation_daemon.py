from datetime import datetime
from zoneinfo import ZoneInfo

from simulator.daemon import seconds_until_next_midnight


def test_daemon_waits_until_next_local_midnight() -> None:
    now = datetime(2026, 7, 1, 23, 59, 30, tzinfo=ZoneInfo("America/Los_Angeles"))

    assert seconds_until_next_midnight(now, "America/Los_Angeles") == 30


def test_daemon_accounts_for_spring_daylight_saving_transition() -> None:
    midnight_before_spring_forward = datetime(
        2026, 3, 8, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )

    assert seconds_until_next_midnight(midnight_before_spring_forward, "America/Los_Angeles") == 23 * 60 * 60
