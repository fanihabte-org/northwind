import pytest

from generator.history_backfill import HistoryBackfill, TARGETS, inferred_event_id


def test_inferred_event_ids_are_deterministic_and_targeted() -> None:
    assert inferred_event_id(TARGETS[0].name, 10, "PENDING") == inferred_event_id(TARGETS[0].name, 10, "PENDING")
    assert inferred_event_id(TARGETS[0].name, 10, "PENDING") != inferred_event_id(TARGETS[0].name, 10, "SHIPPED")


def test_history_runner_rejects_unbounded_batches() -> None:
    with pytest.raises(ValueError, match="between 1 and 100000"):
        HistoryBackfill(object(), 100_001)
