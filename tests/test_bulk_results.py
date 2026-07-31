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
