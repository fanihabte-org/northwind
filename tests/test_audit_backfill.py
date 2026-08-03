from __future__ import annotations

import pytest

from generator.audit_backfill import ERP_TARGETS, ErpAuditBackfill, OpsAuditBackfill, TARGETS


class Cursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, list[object] | None]] = []

    def execute(self, query: str, parameters=None) -> None:
        self.queries.append((query, parameters))

    def fetchall(self) -> list[tuple[object, ...]]:
        return [(101,), (102,)]


class Connection:
    def cursor(self) -> Cursor:
        return Cursor()


def test_backfill_targets_cover_every_ops_table_with_missing_audit_fields() -> None:
    assert [target.name for target in TARGETS] == [
        "products", "warehouses", "carriers", "order_lines", "shipments", "invoices", "support_cases",
    ]
    assert all("COALESCE" in target.update_sql for target in TARGETS)


def test_backfill_key_resume_casts_the_durable_text_checkpoint_to_key_type() -> None:
    cursor = Cursor()
    runner = OpsAuditBackfill(Connection(), batch_size=100)

    assert runner._keys(cursor, TARGETS[0], "100") == [101, 102]

    query, parameters = cursor.queries[-1]
    assert "product_id > %s::BIGINT" in query
    assert parameters == ["100", 100]


def test_backfill_rejects_an_unbounded_batch_size() -> None:
    with pytest.raises(ValueError, match="between 1 and 100000"):
        OpsAuditBackfill(Connection(), batch_size=100_001)


def test_erp_backfill_covers_master_feed_and_large_ledger_audit_fields() -> None:
    assert [target.name for target in ERP_TARGETS] == [
        "companies", "cost_centers", "gl_accounts", "fx_rates", "revenue_postings",
    ]
    assert "loaded_at" in ERP_TARGETS[3].update_sql
    assert "posted_at" in ERP_TARGETS[4].update_sql


def test_erp_ledger_resume_uses_the_numeric_posting_identifier() -> None:
    cursor = Cursor()
    runner = ErpAuditBackfill(Connection(), batch_size=100)

    assert runner._keys(cursor, ERP_TARGETS[-1], "100") == [101, 102]

    query, parameters = cursor.queries[-1]
    assert "posting_id > %s::BIGINT" in query
    assert parameters == ["100", 100]
