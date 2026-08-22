from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from simulator.crm_storage import CrmDeltaPublisher
from simulator.events import SourceEvent


def test_delta_publisher_writes_complete_records_and_is_idempotent(tmp_path: Path) -> None:
    account_schema = pa.schema([("Id", pa.string()), ("Name", pa.string()), ("LastModifiedDate", pa.string()), ("IsDeleted", pa.bool_())])
    opportunity_schema = pa.schema([("Id", pa.string()), ("StageName", pa.string()), ("LastModifiedDate", pa.string()), ("IsDeleted", pa.bool_())])
    accounts = tmp_path / "accounts.parquet"
    opportunities = tmp_path / "opportunities.parquet"
    pq.write_table(pa.Table.from_pylist([{"Id": "0011", "Name": "Old", "LastModifiedDate": "2026-07-24T08:00:00.000+0000", "IsDeleted": False}], schema=account_schema), accounts)
    pq.write_table(pa.Table.from_pylist([{"Id": "0061", "StageName": "Proposal", "LastModifiedDate": "2026-07-24T08:00:00.000+0000", "IsDeleted": False}], schema=opportunity_schema), opportunities)
    publisher = CrmDeltaPublisher(tmp_path / "deltas", {"accounts": accounts, "opportunities": opportunities})
    run_date = date(2026, 7, 25)
    events = [
        SourceEvent.create(business_date=run_date, source_system="crm", event_type="account_created", entity_id="0012", payload={"Id": "0012", "Name": "New", "LastModifiedDate": "2026-07-25T08:00:00.000+0000", "IsDeleted": False}),
        SourceEvent.create(business_date=run_date, source_system="crm", event_type="opportunity_stage_changed", entity_id="0061", payload={"StageName": "Closed Won", "LastModifiedDate": "2026-07-25T08:00:00.000+0000"}),
    ]

    first = publisher.publish(run_date, events)
    second = publisher.publish(run_date, events)

    assert first == second
    assert pq.read_table(first["accounts"]).to_pylist()[0]["Name"] == "New"
    changed_opportunity = pq.read_table(first["opportunities"]).to_pylist()[0]
    assert changed_opportunity["StageName"] == "Closed Won"
    assert changed_opportunity["IsDeleted"] is False
    history = pq.read_table(first["opportunity_history"]).to_pylist()[0]
    assert history["OpportunityId"] == "0061"
    assert history["PreviousStageName"] == "Proposal"
    assert history["StageName"] == "Closed Won"
