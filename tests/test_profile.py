from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from generator.profile import profile_dataset


def test_profile_recommends_date_partitioning_without_full_file_loading(tmp_path) -> None:
    path = tmp_path / "orders.parquet"
    pq.write_table(pa.table({"order_id": [1, 2, 3], "order_date": ["2026-07-01", "2026-07-02", "2026-07-02"], "note": [None, "x", None]}), path)
    report = profile_dataset(path, target_partition_rows=2)
    assert report["row_count"] == 3
    assert report["columns"]["note"]["null_count"] == 2
    assert report["partition_recommendation"]["strategy"] == "date"
    assert report["partition_recommendation"]["column"] == "order_date"
