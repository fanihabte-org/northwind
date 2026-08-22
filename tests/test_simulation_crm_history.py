from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

import pyarrow as pa
import pyarrow.parquet as pq

from simulator.crm_history import validate_opportunity_history_partition
from simulator.crm_storage import OPPORTUNITY_HISTORY_SCHEMA
from simulator.events import SourceEvent
from simulator.reconciliation import DailySourceReconciler, ReconciliationError


def _event(run_date: date) -> SourceEvent:
    return SourceEvent.create(
        business_date=run_date,
        source_system="crm",
        event_type="opportunity_stage_changed",
        entity_id="006000000000000001",
        payload={
            "StageName": "Closed Won",
            "LastModifiedById": "REP-0001",
            "LastModifiedDate": "2026-07-25T08:00:00.000+0000",
            "SystemModstamp": "2026-07-25T08:00:00.000+0000",
        },
    )


def _publish(root: Path, event: SourceEvent, row: dict[str, object] | None = None) -> None:
    target = root / "opportunity_history" / f"business_date={event.business_date.isoformat()}" / "delta.parquet"
    target.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([row or {
            "Id": event.event_id,
            "OpportunityId": event.entity_id,
            "PreviousStageName": "Negotiation",
            "StageName": "Closed Won",
            "CreatedDate": "2026-07-25T08:00:00.000+0000",
            "CreatedById": "REP-0001",
            "SystemModstamp": "2026-07-25T08:00:00.000+0000",
        }], schema=OPPORTUNITY_HISTORY_SCHEMA),
        target,
    )
    manifest = root / "manifests" / f"{event.business_date.isoformat()}.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({
        "events": {"opportunities": [event.event_id]},
        "parts": {"opportunity_history": str(target)},
    }))


def test_history_partition_validation_accepts_published_immutable_event(tmp_path: Path) -> None:
    run_date = date(2026, 7, 25)
    event = _event(run_date)
    _publish(tmp_path, event)

    report = validate_opportunity_history_partition(tmp_path, run_date, [event])

    assert report.expected_events == 1
    assert report.actual_rows == 1
    assert report.problems == ()


def test_history_partition_validation_reports_missing_or_corrupt_artifacts(tmp_path: Path) -> None:
    run_date = date(2026, 7, 25)
    event = _event(run_date)

    missing = validate_opportunity_history_partition(tmp_path, run_date, [event])
    assert "CRM publication manifest is missing" in missing.problems
    assert "OpportunityHistory partition is missing" in missing.problems

    _publish(tmp_path, event, {"Id": event.event_id, "OpportunityId": event.entity_id,
                                "PreviousStageName": "Closed Won", "StageName": "Closed Won",
                                "CreatedDate": "2026-07-25T08:00:00.000+0000", "CreatedById": "REP-0001",
                                "SystemModstamp": "2026-07-25T08:00:00.000+0000"})
    invalid = validate_opportunity_history_partition(tmp_path, run_date, [event])

    assert f"OpportunityHistory {event.event_id} has an invalid stage transition" in invalid.problems


class _State:
    def __init__(self, event: SourceEvent) -> None:
        self.event = event

    def events_for(self, run_date: date, source: str) -> list[SourceEvent]:
        return [self.event] if source == "crm" else []


class _Connection:
    def cursor(self):
        return object()

    def close(self) -> None:
        pass


def test_daily_reconciliation_blocks_completion_when_history_partition_is_invalid(
    tmp_path: Path,
) -> None:
    run_date = date(2026, 7, 25)
    event = _event(run_date)
    _publish(tmp_path, event, {
        "Id": event.event_id,
        "OpportunityId": event.entity_id,
        "PreviousStageName": "Closed Won",
        "StageName": "Closed Won",
        "CreatedDate": "2026-07-25T08:00:00.000+0000",
        "CreatedById": "REP-0001",
        "SystemModstamp": "2026-07-25T08:00:00.000+0000",
    })

    reconciler = DailySourceReconciler(
        _State(event), _Connection, _Connection, crm_delta_root=tmp_path
    )

    with pytest.raises(ReconciliationError, match="invalid stage transition"):
        reconciler.reconcile(run_date)
