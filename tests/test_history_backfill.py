from datetime import date, datetime

import pytest

from generator.history_backfill import HistoryBackfill, TARGETS, _business_date, inferred_event_id


def test_inferred_event_ids_are_deterministic_and_targeted() -> None:
    assert inferred_event_id(TARGETS[0].name, 10, "PENDING") == inferred_event_id(TARGETS[0].name, 10, "PENDING")
    assert inferred_event_id(TARGETS[0].name, 10, "PENDING") != inferred_event_id(TARGETS[0].name, 10, "SHIPPED")


def test_history_runner_rejects_unbounded_batches() -> None:
    with pytest.raises(ValueError, match="between 1 and 100000"):
        HistoryBackfill(object(), 100_001)


def test_business_date_accepts_date_and_timestamp_values() -> None:
    assert _business_date(date(2026, 8, 1)) == date(2026, 8, 1)
    assert _business_date(datetime(2026, 8, 1, 17, 0)) == date(2026, 8, 1)


class _DryRunCursor:
    def __init__(self, rows_by_target: dict[str, list[tuple[object, ...]]]) -> None:
        self.rows_by_target = rows_by_target
        self.executed: list[str] = []
        self.executemany_called = False
        self._rows: list[tuple[object, ...]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query: str, _params=None) -> None:
        self.executed.append(query)
        for target, rows in self.rows_by_target.items():
            if target in query:
                self._rows = rows
                return
        self._rows = []

    def fetchall(self):
        return self._rows

    def executemany(self, *_args) -> None:
        self.executemany_called = True


class _DryRunConnection:
    def __init__(self, rows_by_target: dict[str, list[tuple[object, ...]]]) -> None:
        self.cursor_instance = _DryRunCursor(rows_by_target)

    def cursor(self):
        return self.cursor_instance


def test_dry_run_infers_uncovered_orders_without_writing() -> None:
    connection = _DryRunConnection(
        {
            "ops.orders o": [
                (
                    17,
                    "INVOICED",
                    datetime(2026, 8, 1, 9),
                    date(2026, 8, 5),
                    datetime(2026, 8, 2, 10),
                    datetime(2026, 8, 3, 10),
                )
            ]
        }
    )

    result = HistoryBackfill(connection, batch_size=10).dry_run(TARGETS[0])

    assert result == [
        {
            "name": "inferred_order_status_history",
            "scanned": 1,
            "eligible": 1,
            "inferred": 3,
            "skipped_invalid": 0,
            "would_write": 3,
            "dry_run": True,
            "completed": False,
        }
    ]
    assert not connection.cursor_instance.executemany_called
    assert all("audit_backfill_progress" not in query for query in connection.cursor_instance.executed)


def test_dry_run_reports_invalid_order_chain_without_writing() -> None:
    connection = _DryRunConnection(
        {"ops.orders o": [(18, "INVOICED", datetime(2026, 8, 3), date(2026, 8, 5), None, None)]}
    )

    result = HistoryBackfill(connection, batch_size=10).dry_run(TARGETS[0])[0]

    assert result["eligible"] == 1
    assert result["would_write"] == 1
    assert result["inferred"] == 1
    assert result["skipped_invalid"] == 1
    assert not connection.cursor_instance.executemany_called


@pytest.mark.parametrize(
    ("target", "query_marker", "row", "would_write"),
    [
        (TARGETS[1], "ops.shipments s", (21, datetime(2026, 8, 1), datetime(2026, 8, 2), date(2026, 8, 3)), 2),
        (TARGETS[2], "ops.invoices i", (22, "VOID", datetime(2026, 8, 1), datetime(2026, 8, 2)), 2),
        (TARGETS[3], "ops.support_cases c", (23, "Closed", datetime(2026, 8, 1), 24, datetime(2026, 8, 3)), 3),
    ],
)
def test_dry_run_uses_target_inference_for_all_history_targets(target, query_marker, row, would_write) -> None:
    connection = _DryRunConnection({query_marker: [row]})

    result = HistoryBackfill(connection, batch_size=10).dry_run(target)[0]

    assert result["eligible"] == 1
    assert result["inferred"] == would_write
    assert result["would_write"] == would_write
    assert result["skipped_invalid"] == 0
    assert not connection.cursor_instance.executemany_called
