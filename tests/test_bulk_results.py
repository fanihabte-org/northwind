from __future__ import annotations

import json

import pytest

from fakeforce.bulk.results import BulkQueryResultStore, InvalidBulkLocator


def test_bulk_result_store_pages_and_streams_csv_parts(tmp_path) -> None:
    job_dir = tmp_path / "job-1"
    job_dir.mkdir()
    (job_dir / "part-00000.csv").write_text("Id\n001\n")
    (job_dir / "part-00001.csv").write_text("Id\n002\n")
    (job_dir / "manifest.json").write_text(
        json.dumps({"parts": [
            {"path": "part-00000.csv", "record_count": 1},
            {"path": "part-00001.csv", "record_count": 1},
        ]})
    )
    store = BulkQueryResultStore(tmp_path)

    first = store.page("job-1", None)
    second = store.page("job-1", first.next_locator)

    assert b"".join(store.stream(first.path)) == b"Id\n001\n"
    assert second.next_locator is None
    with pytest.raises(InvalidBulkLocator):
        store.page("job-1", "invalid")


def test_bulk_result_store_streams_replayable_max_records_window(tmp_path) -> None:
    job = tmp_path / "job-1"; job.mkdir()
    (job / "part-00000.csv").write_text("Id\n001\n002\n")
    (job / "part-00001.csv").write_text("Id\n003\n")
    (job / "manifest.json").write_text(json.dumps({"parts": [{"path": "part-00000.csv", "record_count": 2}, {"path": "part-00001.csv", "record_count": 1}]}))
    store = BulkQueryResultStore(tmp_path)
    page = store.window("job-1", None, 2)
    assert page.next_locator == "r:2"
    assert b"".join(store.stream_window("job-1", None, page.record_count)) == b"Id\r\n001\r\n002\r\n"
