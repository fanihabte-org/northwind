from datetime import date, timedelta

from simulator.events import SourceEvent
from simulator.ops import OpsDailyPlanner, OpsSnapshot
from simulator.policy import SimulationPolicy


def closed_won(day: date) -> SourceEvent:
    return SourceEvent.create(
        business_date=day,
        source_system="crm",
        event_type="opportunity_stage_changed",
        entity_id="006000000000000010",
        payload={"StageName": "Closed Won", "AccountId": "001000000000000010"},
    )


def test_ops_planner_never_creates_an_order_on_the_crm_close_date() -> None:
    event = closed_won(date(2026, 7, 25))
    planner = OpsDailyPlanner(SimulationPolicy())

    orders = planner.plan(date(2026, 7, 25), [event], set(), OpsSnapshot(next_order_id=100))

    assert orders == []


def test_ops_planner_creates_one_traceable_order_after_the_sla_window() -> None:
    event = closed_won(date(2026, 7, 25))
    planner = OpsDailyPlanner(SimulationPolicy())
    orders = planner.plan(date(2026, 7, 29), [event], set(), OpsSnapshot(next_order_id=100))

    assert len(orders) == 1
    assert orders[0].payload["customer_id"] == 10
    assert orders[0].payload["opportunity_ref"] == event.entity_id
    assert orders[0].payload["crm_to_ops_delay_days"] >= 1
    assert orders[0].anomaly_type in {None, "delayed_crm_to_ops"}


def test_ops_planner_does_not_recreate_an_already_converted_opportunity() -> None:
    event = closed_won(date(2026, 7, 25))
    orders = OpsDailyPlanner(SimulationPolicy()).plan(
        date(2026, 7, 30), [event], {event.entity_id}, OpsSnapshot(next_order_id=100)
    )

    assert orders == []
