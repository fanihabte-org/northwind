"""Source resolution is memoized, but must never go stale.

FakeForce is long-running and the simulator publishes a new delta partition
every night, so a cache that misses a publication would serve yesterday's CRM
for as long as the process lives.  These tests pin both halves of that: the
work is skipped when nothing moved, and it is redone when something did.
"""

from __future__ import annotations

import json
import os
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.catalog import DatasetCatalog


SCHEMA = pa.schema(
    [
        pa.field("Id", pa.string()),
        pa.field("IsDeleted", pa.bool_()),
        pa.field("Name", pa.string()),
        pa.field("LastModifiedDate", pa.string()),
    ]
)


def _write(path, ids, modified):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "Id": list(ids),
                "IsDeleted": [False] * len(ids),
                "Name": [f"n{identifier}" for identifier in ids],
                "LastModifiedDate": [modified] * len(ids),
            },
            schema=SCHEMA,
        ),
        path,
    )


@pytest.fixture()
def catalog(tmp_path) -> DatasetCatalog:
    data_root = tmp_path / "data"
    _write(data_root / "crm_opportunities.parquet", ["001", "002"], "2026-07-24T00:00:00Z")
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [
                    {
                        "name": "Opportunity",
                        "sources": ["crm_opportunities.parquet"],
                        "delta_patterns": ["opportunities/**/*.parquet"],
                        "version_field": "LastModifiedDate",
                        "id_field": "Id",
                        "soft_delete_field": "IsDeleted",
                    }
                ],
            }
        )
    )
    return DatasetCatalog.from_file(catalog_path, (data_root,))


def _delta(catalog: DatasetCatalog, business_date: str, ids, modified):
    spec = catalog.get("Opportunity")
    root = spec.data_roots[0] / "opportunities" / f"business_date={business_date}"
    _write(root / "delta.parquet", ids, modified)
    return root / "delta.parquet"


def test_repeated_resolution_does_not_rewalk_the_filesystem(catalog, monkeypatch) -> None:
    calls = []
    original = DatasetCatalog._expand_source

    def counted(pattern, roots):
        calls.append(pattern)
        return original(pattern, roots)

    monkeypatch.setattr(DatasetCatalog, "_expand_source", staticmethod(counted))
    spec = catalog.get("Opportunity")

    for _ in range(25):
        spec.current_sources()

    assert len(calls) == 1, "the recursive glob must run once, not once per call"


def test_repeated_snapshot_reads_do_not_restat_every_source(catalog, monkeypatch) -> None:
    spec = catalog.get("Opportunity")
    _delta(catalog, "2026-07-25", ["003"], "2026-07-25T00:00:00Z")
    catalog.refresh()

    computed = []
    original = DatasetCatalog._compute_snapshot_id

    def counted(self):
        computed.append(1)
        return original(self)

    monkeypatch.setattr(DatasetCatalog, "_compute_snapshot_id", counted)

    identifiers = {catalog.snapshot_id for _ in range(25)}

    assert len(identifiers) == 1
    assert len(computed) == 1, "snapshot_id is read once per page of every extract"


def test_a_newly_published_partition_is_picked_up(catalog) -> None:
    spec = catalog.get("Opportunity")
    assert len(spec.current_sources()) == 1
    before = catalog.snapshot_id

    _delta(catalog, "2026-07-25", ["003"], "2026-07-25T00:00:00Z")

    assert len(spec.current_sources()) == 2, "a nightly publication must be visible"
    assert catalog.snapshot_id != before, "cursors must be invalidated by new data"


def test_a_partition_rewritten_in_place_is_picked_up(catalog) -> None:
    spec = catalog.get("Opportunity")
    partition = _delta(catalog, "2026-07-25", ["003"], "2026-07-25T00:00:00Z")
    assert len(spec.current_sources()) == 2
    before = catalog.snapshot_id

    _write(partition, ["003", "004"], "2026-07-26T00:00:00Z")
    stat = partition.stat()
    os.utime(partition, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert catalog.snapshot_id != before


def test_a_regenerated_base_file_is_picked_up(catalog) -> None:
    spec = catalog.get("Opportunity")
    base = spec.sources[0]
    before = catalog.snapshot_id

    _write(base, ["001", "002", "003"], "2026-07-26T00:00:00Z")
    stat = base.stat()
    os.utime(base, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert catalog.snapshot_id != before


def test_refresh_forces_the_next_resolution_to_reread(catalog, monkeypatch) -> None:
    spec = catalog.get("Opportunity")
    spec.current_sources()

    calls = []
    original = DatasetCatalog._expand_source

    def counted(pattern, roots):
        calls.append(pattern)
        return original(pattern, roots)

    monkeypatch.setattr(DatasetCatalog, "_expand_source", staticmethod(counted))
    catalog.refresh()
    spec.current_sources()

    assert len(calls) == 1


def test_delta_roots_stop_at_the_first_wildcard_segment(catalog) -> None:
    spec = catalog.get("Opportunity")

    roots = spec.delta_roots()

    assert roots == (spec.data_roots[0] / "opportunities",)


def test_resolution_fingerprint_tolerates_a_missing_delta_directory(catalog) -> None:
    """A deployment that has never run the simulator has no delta root yet."""
    spec = catalog.get("Opportunity")

    assert not (spec.data_roots[0] / "opportunities").exists()
    assert spec.current_sources() == spec.sources
    assert isinstance(catalog.snapshot_id, str)


def test_concurrent_resolution_returns_one_consistent_answer(catalog) -> None:
    spec = catalog.get("Opportunity")
    _delta(catalog, "2026-07-25", ["003"], "2026-07-25T00:00:00Z")
    results: list[tuple] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(8)

    def resolve() -> None:
        try:
            barrier.wait(timeout=10)
            results.append((spec.current_sources(), catalog.snapshot_id))
        except BaseException as error:  # pragma: no cover - surfaced by the assert
            errors.append(error)

    threads = [threading.Thread(target=resolve) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert not errors
    assert len(set(results)) == 1, "concurrent readers must agree"


def test_specs_compare_without_regard_to_their_caches(catalog) -> None:
    """The cache is an implementation detail and must not leak into equality."""
    spec = catalog.get("Opportunity")
    twin = DatasetCatalog.from_file(
        spec.data_roots[0].parent / "catalog.json", spec.data_roots
    ).get("Opportunity")
    spec.current_sources()

    assert spec == twin
