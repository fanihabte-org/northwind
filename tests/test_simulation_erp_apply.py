from datetime import date

from simulator.erp_apply import ErpEventApplier
from simulator.events import SourceEvent


class RecordingCursor:
    def __init__(self, responses) -> None:
        self.responses = iter(responses)
        self.queries: list[str] = []

    def execute(self, query, parameters=None):
        self.queries.append(query)

    def fetchone(self):
        return next(self.responses)


def _event() -> SourceEvent:
    return SourceEvent.create(
        business_date=date(2026, 7, 10),
        source_system="erp",
        event_type="revenue_posting_created",
        entity_id="900",
        payload={
            "posting_id": 900,
            "document_number": "US01-000000000100",
            "order_ref": 100,
            "company_code": "US01",
            "company_currency": "USD",
            "document_currency": "USD",
            "amount_doc": 125.5,
            "posting_date": "2026-07-01",
        },
    )


def test_erp_applier_validates_masters_and_inserts_posting_and_marker() -> None:
    cursor = RecordingCursor([None, None, ("USD",), ("4000",), ("CC-US001",)])

    assert ErpEventApplier().apply(cursor, [_event()]) == 1

    statements = "\n".join(cursor.queries)
    assert "INSERT INTO erp.revenue_postings" in statements
    assert "INSERT INTO simulation.applied_events" in statements
    assert "SELECT functional_currency FROM erp.companies" in statements


def test_erp_applier_uses_finance_fx_rates_for_cross_currency_invoice() -> None:
    event = _event()
    event = SourceEvent.create(
        business_date=event.business_date,
        source_system=event.source_system,
        event_type=event.event_type,
        entity_id=event.entity_id,
        payload={**event.payload, "company_currency": "EUR", "document_currency": "GBP"},
    )
    cursor = RecordingCursor([None, None, ("EUR",), ("4000",), ("CC-DE001",), (1.25,), (1.10,)])

    assert ErpEventApplier().apply(cursor, [event]) == 1
    assert sum("SELECT rate FROM erp.fx_rates" in query for query in cursor.queries) == 2
