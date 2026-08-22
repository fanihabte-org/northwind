"""Read-only validation of immutable CRM OpportunityHistory partitions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from simulator.crm_storage import OPPORTUNITY_HISTORY_SCHEMA
from simulator.events import SourceEvent

HISTORY_EVENT_TYPES = frozenset({"opportunity_created", "opportunity_stage_changed"})


@dataclass(frozen=True)
class OpportunityHistoryValidationReport:
    run_date: date
    expected_events: int
    actual_rows: int
    problems: tuple[str, ...]


def validate_opportunity_history_partition(
    root: Path, run_date: date, events: Iterable[SourceEvent]
) -> OpportunityHistoryValidationReport:
    """Validate one immutable daily history artifact without changing it."""
    expected = [
        event
        for event in events
        if event.source_system == "crm" and event.event_type in HISTORY_EVENT_TYPES
    ]
    target = root / "opportunity_history" / f"business_date={run_date.isoformat()}" / "delta.parquet"
    if not expected:
        problems = ("unexpected OpportunityHistory partition",) if target.is_file() else ()
        return OpportunityHistoryValidationReport(run_date, 0, 0, problems)

    problems: list[str] = []
    manifest_path = root / "manifests" / f"{run_date.isoformat()}.json"
    manifest = _read_manifest(manifest_path, problems)
    _validate_manifest(manifest, target, expected, problems)
    if not target.is_file():
        problems.append("OpportunityHistory partition is missing")
        return OpportunityHistoryValidationReport(run_date, len(expected), 0, tuple(problems))

    try:
        parquet = pq.ParquetFile(target)
        if not parquet.schema_arrow.equals(OPPORTUNITY_HISTORY_SCHEMA, check_metadata=False):
            problems.append("OpportunityHistory partition has an incompatible schema")
        rows = parquet.read().to_pylist()
    except Exception as error:
        problems.append(f"OpportunityHistory partition cannot be read: {error}")
        return OpportunityHistoryValidationReport(run_date, len(expected), 0, tuple(problems))

    _validate_rows(rows, expected, run_date, problems)
    return OpportunityHistoryValidationReport(run_date, len(expected), len(rows), tuple(problems))


def _read_manifest(path: Path, problems: list[str]) -> dict[str, object] | None:
    if not path.is_file():
        problems.append("CRM publication manifest is missing")
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        problems.append(f"CRM publication manifest cannot be read: {error}")
        return None
    if not isinstance(value, dict):
        problems.append("CRM publication manifest is not an object")
        return None
    return value


def _validate_manifest(
    manifest: dict[str, object] | None,
    target: Path,
    expected: list[SourceEvent],
    problems: list[str],
) -> None:
    if manifest is None:
        return
    parts = manifest.get("parts")
    if not isinstance(parts, dict) or parts.get("opportunity_history") != str(target):
        problems.append("CRM manifest does not reference the OpportunityHistory partition")
    event_groups = manifest.get("events")
    opportunity_ids = event_groups.get("opportunities") if isinstance(event_groups, dict) else None
    if not isinstance(opportunity_ids, list):
        problems.append("CRM manifest has no opportunity event IDs")
        return
    absent = sorted({event.event_id for event in expected} - {str(value) for value in opportunity_ids})
    if absent:
        problems.append("CRM manifest is missing OpportunityHistory event IDs: " + ", ".join(absent))


def _validate_rows(
    rows: list[dict[str, object]],
    expected: list[SourceEvent],
    run_date: date,
    problems: list[str],
) -> None:
    expected_by_id = {event.event_id: event for event in expected}
    actual_ids = [str(row.get("Id")) for row in rows]
    duplicates = sorted({value for value in actual_ids if actual_ids.count(value) > 1})
    if duplicates:
        problems.append("OpportunityHistory partition has duplicate Id values: " + ", ".join(duplicates))
    missing = sorted(set(expected_by_id) - set(actual_ids))
    unexpected = sorted(set(actual_ids) - set(expected_by_id))
    if missing:
        problems.append("OpportunityHistory rows missing for event IDs: " + ", ".join(missing))
    if unexpected:
        problems.append("OpportunityHistory rows not backed by CRM events: " + ", ".join(unexpected))

    for row in rows:
        event = expected_by_id.get(str(row.get("Id")))
        if event is None:
            continue
        if row.get("OpportunityId") != event.entity_id:
            problems.append(f"OpportunityHistory {event.event_id} has the wrong OpportunityId")
        payload = event.payload
        if row.get("StageName") != payload.get("StageName"):
            problems.append(f"OpportunityHistory {event.event_id} has the wrong StageName")
        if str(row.get("CreatedDate")) != str(payload.get("LastModifiedDate")):
            problems.append(f"OpportunityHistory {event.event_id} has the wrong CreatedDate")
        expected_stamp = payload.get("SystemModstamp", payload.get("LastModifiedDate"))
        if str(row.get("SystemModstamp")) != str(expected_stamp):
            problems.append(f"OpportunityHistory {event.event_id} has the wrong SystemModstamp")
        if event.event_type == "opportunity_created" and row.get("PreviousStageName") is not None:
            problems.append(f"OpportunityHistory {event.event_id} must begin with a null previous stage")
        if event.event_type == "opportunity_stage_changed":
            previous = row.get("PreviousStageName")
            if previous is None or previous == row.get("StageName"):
                problems.append(f"OpportunityHistory {event.event_id} has an invalid stage transition")
        if not str(row.get("CreatedDate", "")).startswith(run_date.isoformat()):
            problems.append(f"OpportunityHistory {event.event_id} is outside its business-date partition")
