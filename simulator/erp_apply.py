"""Transactional, idempotent application of planned ERP revenue postings."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Protocol

from simulator.events import SourceEvent


class Cursor(Protocol):
    def execute(self, query: str, parameters: Iterable[Any] | None = None) -> Any: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...


class ErpEventApplier:
    """Caller owns the ERP transaction; event marker and posting commit together."""

    def apply(self, cursor: Cursor, events: Iterable[SourceEvent]) -> int:
        applied = 0
        for event in events:
            if event.source_system != "erp" or event.event_type != "revenue_posting_created":
                raise ValueError("ERP applier supports only planned revenue postings")
            cursor.execute("SELECT 1 FROM simulation.applied_events WHERE event_id = %s", [event.event_id])
            if cursor.fetchone() is not None:
                continue
            self._apply_posting(cursor, event)
            cursor.execute(
                """
                INSERT INTO simulation.applied_events (event_id, source_system, event_type, business_date)
                VALUES (%s, %s, %s, %s)
                """,
                [event.event_id, event.source_system, event.event_type, event.business_date],
            )
            applied += 1
        return applied

    def _apply_posting(self, cursor: Cursor, event: SourceEvent) -> None:
        payload = event.payload
        posting_id = int(payload["posting_id"])
        order_ref = int(payload["order_ref"])
        cursor.execute("SELECT order_ref FROM erp.revenue_postings WHERE posting_id = %s", [posting_id])
        existing = cursor.fetchone()
        if existing is not None:
            if existing[0] != order_ref:
                raise RuntimeError(f"ERP posting ID collision for {posting_id}")
            return

        company_code = str(payload["company_code"])
        cursor.execute("SELECT functional_currency FROM erp.companies WHERE company_code = %s", [company_code])
        company = cursor.fetchone()
        if company is None:
            raise RuntimeError(f"unknown ERP company {company_code}")
        company_currency = str(payload["company_currency"])
        if company[0] != company_currency:
            raise RuntimeError(f"company currency changed for {company_code}")

        posting_date = date.fromisoformat(str(payload["posting_date"]))
        cursor.execute(
            """
            SELECT gl_account FROM erp.gl_accounts
            WHERE account_type = 'REVENUE' AND is_postable
            ORDER BY gl_account LIMIT 1
            """
        )
        gl_account = cursor.fetchone()
        cursor.execute(
            """
            SELECT cost_center_code FROM erp.cost_centers
            WHERE company_code = %s AND is_active AND valid_from <= %s
              AND (valid_to IS NULL OR valid_to > %s)
            ORDER BY cost_center_code LIMIT 1
            """,
            [company_code, posting_date, posting_date],
        )
        cost_center = cursor.fetchone()
        if gl_account is None or cost_center is None:
            raise RuntimeError("ERP posting requires an active revenue account and cost center")

        amount_doc = Decimal(str(payload["amount_doc"]))
        document_currency = str(payload["document_currency"])
        amount_company = self._company_amount(
            cursor, amount_doc, document_currency, company_currency, posting_date
        )
        cursor.execute(
            """
            INSERT INTO erp.revenue_postings (
                posting_id, document_number, document_type, company_code, order_ref, gl_account,
                cost_center_code, posting_date, fiscal_period, document_currency, amount_doc,
                company_currency, amount_company, reverses_posting_id, posted_at
            ) VALUES (%s, %s, 'INV', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
            """,
            [
                posting_id,
                payload["document_number"],
                company_code,
                order_ref,
                gl_account[0],
                cost_center[0],
                posting_date,
                posting_date.strftime("%Y-%m"),
                document_currency,
                amount_doc,
                company_currency,
                amount_company,
                f"{event.business_date.isoformat()} 08:00:00",
            ],
        )

    def _company_amount(
        self,
        cursor: Cursor,
        amount_doc: Decimal,
        document_currency: str,
        company_currency: str,
        posting_date: date,
    ) -> Decimal:
        if document_currency == company_currency:
            return amount_doc.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        usd_amount = amount_doc * self._usd_rate(cursor, document_currency, posting_date)
        if company_currency != "USD":
            usd_amount /= self._usd_rate(cursor, company_currency, posting_date)
        return usd_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def _usd_rate(cursor: Cursor, currency: str, posting_date: date) -> Decimal:
        if currency == "USD":
            return Decimal("1")
        cursor.execute(
            """
            SELECT rate FROM erp.fx_rates
            WHERE from_currency = %s AND to_currency = 'USD' AND rate_type = 'SPOT'
              AND rate_date <= %s
            ORDER BY rate_date DESC LIMIT 1
            """,
            [currency, posting_date],
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"missing FX rate for {currency} on or before {posting_date}")
        return Decimal(str(row[0]))
