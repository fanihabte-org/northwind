from datetime import date

from simulator.crm import CrmDailyPlanner, CrmSnapshot
from simulator.policy import AnomalyRates, SimulationPolicy


def snapshot() -> CrmSnapshot:
    return CrmSnapshot.from_rows(
        [{"Id": "001000000000000010", "OwnerId": "REP-0001"}],
        [
            {"Id": "006000000000000010", "OwnerId": "REP-0001", "StageName": "Prospecting"},
            {"Id": "006000000000000011", "OwnerId": "REP-0001", "StageName": "Qualification"},
        ],
    )


def test_crm_planner_is_repeatable_and_generates_complete_new_records() -> None:
    planner = CrmDailyPlanner(SimulationPolicy(annual_growth_rate=0.08))

    first = planner.plan(date(2026, 7, 25), 42, snapshot())
    second = planner.plan(date(2026, 7, 25), 42, snapshot())
    accounts = [event for event in first if event.event_type == "account_created"]
    opportunities = [event for event in first if event.event_type == "opportunity_created"]

    assert [event.event_id for event in first] == [event.event_id for event in second]
    assert accounts[0].payload["Id"] == "001000000000000011"
    assert accounts[0].payload["AccountNumber"] == "ACC-000011"
    assert opportunities[0].payload["AccountId"]
    assert {"LastModifiedDate", "IsDeleted"} <= accounts[0].payload.keys()
    assert {"StageName", "LastModifiedDate", "IsDeleted"} <= opportunities[0].payload.keys()


def test_crm_planner_marks_long_open_opportunity_without_auto_closing_it() -> None:
    policy = SimulationPolicy(anomalies=AnomalyRates(stalled_opportunity=0.05))
    events = CrmDailyPlanner(policy).plan(date(2026, 7, 25), 42, snapshot())
    stalled = [event for event in events if event.event_type == "opportunity_stalled"]

    assert stalled
    assert stalled[0].anomaly_type == "long_open_opportunity"
    assert stalled[0].payload["StageName"] in {"Prospecting", "Qualification"}
