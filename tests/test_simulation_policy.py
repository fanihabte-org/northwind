from datetime import date, timedelta

import pytest

from simulator.policy import AnomalyRates, DayWindow, SimulationPolicy


def test_policy_generates_proportional_daily_volume() -> None:
    policy = SimulationPolicy(annual_growth_rate=0.073)

    assert policy.proportional_daily_volume(0) == 1
    assert policy.proportional_daily_volume(100_000) == 20


def test_policy_selects_one_repeatable_breach_day_per_week() -> None:
    policy = SimulationPolicy()
    monday = date(2026, 8, 3)
    week = [monday + timedelta(days=offset) for offset in range(7)]

    selected = [day for day in week if policy.is_weekly_sla_breach_date(day, seed=42)]

    assert len(selected) == 1
    assert selected == [day for day in week if policy.is_weekly_sla_breach_date(day, seed=42)]


@pytest.mark.parametrize("minimum, maximum", [(-1, 1), (2, 1)])
def test_policy_rejects_invalid_day_windows(minimum: int, maximum: int) -> None:
    with pytest.raises(ValueError, match="day windows"):
        DayWindow(minimum, maximum)


def test_policy_rejects_anomaly_rates_that_would_spoil_the_dataset() -> None:
    with pytest.raises(ValueError, match="anomaly rates"):
        AnomalyRates(stalled_shipment=0.051)
