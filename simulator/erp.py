"""Plan Finance revenue postings from the durable Ops invoice hand-off."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from simulator.events import SourceEvent
from simulator.policy import SimulationPolicy


@dataclass(frozen=True)
class OpsInvoice:
    """The cross-system invoice facts Finance receives from Operations."""

    invoice_id: int
    invoice_number: str
    order_id: int
    invoice_date: date
    company_code: str
    company_currency: str
    document_currency: str
    amount_doc: float


@dataclass(frozen=True)
class ErpSnapshot:
    next_posting_id: int


class ErpDailyPlanner:
    """Create deterministic ledger-posting events without mutating the ERP DB."""

    def __init__(self, policy: SimulationPolicy) -> None:
        self.policy = policy

    def plan(
        self,
        run_date: date,
        seed: int,
        invoices: Iterable[OpsInvoice],
        posted_order_ids: set[int],
        snapshot: ErpSnapshot,
    ) -> list[SourceEvent]:
        breach_today = self.policy.is_weekly_sla_breach_date(run_date, seed)
        eligible: list[tuple[OpsInvoice, int, str | None]] = []
        for invoice in invoices:
            if invoice.order_id in posted_order_ids:
                continue
            delay, anomaly = self._posting_delay(invoice)
            due_date = invoice.invoice_date + timedelta(days=delay)
            # Hold only data that becomes due on the selected breach date.  Any
            # pre-existing backlog still publishes, guaranteeing a one-day
            # breach even when two consecutive weeks select adjacent dates.
            if due_date <= run_date and not (breach_today and due_date == run_date):
                eligible.append((invoice, delay, anomaly))

        postings: list[SourceEvent] = []
        for offset, (invoice, delay, anomaly) in enumerate(
            sorted(eligible, key=lambda item: item[0].invoice_id)
        ):
            posting_id = snapshot.next_posting_id + offset
            postings.append(
                SourceEvent.create(
                    business_date=run_date,
                    source_system="erp",
                    event_type="revenue_posting_created",
                    entity_id=str(posting_id),
                    payload={
                        "posting_id": posting_id,
                        "document_number": f"{invoice.company_code}-{invoice.invoice_id:012d}",
                        "invoice_id": invoice.invoice_id,
                        "invoice_number": invoice.invoice_number,
                        "order_ref": invoice.order_id,
                        "company_code": invoice.company_code,
                        "company_currency": invoice.company_currency,
                        "document_currency": invoice.document_currency,
                        "amount_doc": invoice.amount_doc,
                        "posting_date": invoice.invoice_date.isoformat(),
                        "invoice_to_erp_delay_days": delay,
                    },
                    anomaly_type=anomaly,
                )
            )
        return postings

    def _posting_delay(self, invoice: OpsInvoice) -> tuple[int, str | None]:
        digest = hashlib.sha256(f"erp:{invoice.invoice_id}".encode()).digest()
        span = self.policy.invoice_to_erp.maximum - self.policy.invoice_to_erp.minimum + 1
        delay = self.policy.invoice_to_erp.minimum + digest[0] % span
        late = digest[1] / 255 < self.policy.anomalies.delayed_erp_posting
        if late:
            return self.policy.invoice_to_erp.maximum + 1, "late_erp_posting"
        return delay, None
