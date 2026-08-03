from datetime import date, timedelta

from simulator.erp import ErpDailyPlanner, ErpSnapshot, OpsInvoice
from simulator.policy import AnomalyRates, DayWindow, SimulationPolicy


def _invoice(invoice_id: int = 100, invoice_date: date = date(2026, 7, 1)) -> OpsInvoice:
    return OpsInvoice(
        invoice_id=invoice_id,
        invoice_number=f"OPS-{invoice_id:012d}",
        order_id=invoice_id,
        invoice_date=invoice_date,
        company_code="US01",
        company_currency="USD",
        document_currency="USD",
        amount_doc=125.5,
    )


def test_erp_planner_creates_traceable_posting_after_invoice_sla() -> None:
    policy = SimulationPolicy()
    planner = ErpDailyPlanner(policy)
    run_date = next(
        date(2026, 7, 10) + timedelta(days=offset)
        for offset in range(7)
        if not policy.is_weekly_sla_breach_date(date(2026, 7, 10) + timedelta(days=offset), 1)
    )

    postings = planner.plan(run_date, 1, [_invoice()], set(), ErpSnapshot(900))

    assert len(postings) == 1
    posting = postings[0]
    assert posting.payload["posting_id"] == 900
    assert posting.payload["order_ref"] == 100
    assert posting.payload["posting_date"] == "2026-07-01"
    assert posting.payload["invoice_to_erp_delay_days"] >= 1
    assert posting.business_date > date.fromisoformat(posting.payload["posting_date"])


def test_erp_planner_does_not_repost_an_order() -> None:
    postings = ErpDailyPlanner(SimulationPolicy()).plan(
        date(2026, 7, 10), 1, [_invoice()], {100}, ErpSnapshot(900)
    )

    assert postings == []


def test_weekly_breach_defers_then_self_heals_next_day() -> None:
    policy = SimulationPolicy(invoice_to_erp=DayWindow(1, 1))
    planner = ErpDailyPlanner(policy)
    start = date(2026, 7, 6)
    breach_date = next(
        start + timedelta(days=offset)
        for offset in range(7)
        if policy.is_weekly_sla_breach_date(start + timedelta(days=offset), 42)
    )
    invoice = _invoice(invoice_date=breach_date - timedelta(days=1))

    assert planner.plan(breach_date, 42, [invoice], set(), ErpSnapshot(900)) == []
    assert len(planner.plan(breach_date + timedelta(days=1), 42, [invoice], set(), ErpSnapshot(900))) == 1


def test_late_erp_anomaly_is_deterministic_and_bounded() -> None:
    policy = SimulationPolicy(anomalies=AnomalyRates(delayed_erp_posting=0.05))
    run_date = next(
        date(2026, 7, 20) + timedelta(days=offset)
        for offset in range(7)
        if not policy.is_weekly_sla_breach_date(date(2026, 7, 20) + timedelta(days=offset), 1)
    )
    postings = ErpDailyPlanner(policy).plan(
        run_date, 1, [_invoice(invoice_id) for invoice_id in range(1, 400)], set(), ErpSnapshot(1)
    )

    late = [posting for posting in postings if posting.anomaly_type == "late_erp_posting"]
    assert late
    assert all(posting.payload["invoice_to_erp_delay_days"] > policy.invoice_to_erp.maximum for posting in late)
