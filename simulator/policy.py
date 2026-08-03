"""Business timing and controlled-anomaly contract for daily increments."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, time


@dataclass(frozen=True)
class DayWindow:
    """Inclusive calendar-day window with a validated natural ordering."""

    minimum: int
    maximum: int

    def __post_init__(self) -> None:
        if self.minimum < 0 or self.maximum < self.minimum:
            raise ValueError("day windows must be non-negative and ordered")


@dataclass(frozen=True)
class AnomalyRates:
    """Small, traceable exception rates; normal records remain the majority."""

    stalled_opportunity: float = 0.006
    delayed_crm_to_ops: float = 0.004
    stalled_shipment: float = 0.008
    delayed_erp_posting: float = 0.004

    def __post_init__(self) -> None:
        for value in self.__dict__.values():
            if not 0 <= value <= 0.05:
                raise ValueError("anomaly rates must be between 0 and 0.05")


@dataclass(frozen=True)
class SimulationPolicy:
    """The data contract used by the scheduler and all source-specific appliers."""

    timezone: str = "America/Los_Angeles"
    run_at: time = time(0, 0)
    annual_growth_rate: float = 0.08
    crm_to_ops: DayWindow = DayWindow(1, 3)
    order_to_shipment: DayWindow = DayWindow(1, 5)
    shipment_to_invoice: DayWindow = DayWindow(0, 3)
    invoice_to_erp: DayWindow = DayWindow(1, 2)
    weekly_sla_breach_days: int = 1
    anomalies: AnomalyRates = AnomalyRates()

    def __post_init__(self) -> None:
        if not 0 < self.annual_growth_rate <= 1:
            raise ValueError("annual growth rate must be between 0 and 1")
        if self.weekly_sla_breach_days < 1:
            raise ValueError("weekly SLA breach duration must be positive")

    def proportional_daily_volume(self, current_population: int) -> int:
        """Return a bounded daily increment proportional to current population."""
        if current_population < 0:
            raise ValueError("current population cannot be negative")
        return max(1, round(current_population * self.annual_growth_rate / 365))

    def is_weekly_sla_breach_date(self, run_date: date, seed: int) -> bool:
        """Choose exactly one deterministic random weekday in every ISO week."""
        iso_year, iso_week, _ = run_date.isocalendar()
        digest = hashlib.sha256(f"{seed}:{iso_year}:{iso_week}".encode()).digest()
        selected_weekday = digest[0] % 7 + 1  # ISO weekday, Monday=1
        return run_date.isoweekday() == selected_weekday
