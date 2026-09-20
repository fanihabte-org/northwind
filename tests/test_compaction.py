"""Compaction must bound the file count without ever changing an answer.

It runs against a store the simulator is still writing to and that FakeForce is
still reading from, so the ordering guarantees matter as much as the merge
itself: publish the merged file first, remove its sources second, and never
touch a partition that appeared after the plan was taken.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.catalog import DatasetCatalog
from fakeforce.compaction import (
    COMPACTED_FILENAME,
    CompactionError,
    DeltaCompactor,
    main,
)
from fakeforce.config import Settings
from fakeforce.engine import DuckDBEngine

SCHEMA = pa.schema(
    [
        pa.field("Id", pa.string()),
        pa.field("IsDeleted", pa.bool_()),
        pa.field("Name", pa.string()),
        pa.field("LastModifiedDate", pa.string()),
    ]
)


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "Id": [r[0] for r in rows],
                "IsDeleted": [False] * len(rows),
                "Name": [r[1] for r in rows],
                "LastModifiedDate": [r[2] for r in rows],
            },
            schema=SCHEMA,
        ),
        path,
    )


@pytest.fixture()
def world(tmp_path):
    data = tmp_path / "data"
    _write(
        data / "crm_opportunities.parquet",
        [("006a", "base-a", "2026-07-24T00:00:00Z"), ("006b", "base-b", "2026-07-24T00:00:00Z")],
    )
    (tmp_path / "catalog.json").write_text(
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
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data),
            "FAKEFORCE_DATA_ROOTS": str(data),
            "FAKEFORCE_CATALOG_PATH": str(tmp_path / "catalog.json"),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
            "FAKEFORCE_MEMORY_LIMIT": "256MB",
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    return settings, catalog, data


def _publish(data, day, rows):
    _write(data / "opportunities" / f"business_date=2026-07-{day}" / "delta.parquet", rows)


def _visible(settings, catalog):
    with DuckDBEngine(settings, catalog).connection(objects=("Opportunity",)) as conn:
        return {
            row[0]: row[1]
            for row in conn.execute(
                'SELECT "Id", "Name" FROM "ff_source_Opportunity"'
            ).fetchall()
        }


def test_compaction_collapses_partitions_without_changing_the_answer(world) -> None:
    settings, catalog, data = world
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006a", "day26", "2026-07-26T00:00:00Z"), ("006c", "new-c", "2026-07-26T00:00:00Z")])
    _publish(data, "27", [("006b", "day27", "2026-07-27T00:00:00Z")])
    before = _visible(settings, catalog)
    assert before == {"006a": "day26", "006b": "day27", "006c": "new-c"}

    result = DeltaCompactor(settings, catalog).compact("Opportunity")

    assert result.partitions_merged == 3
    assert result.rows_written == 3
    assert result.removed == 3
    assert _visible(settings, catalog) == before, "compaction must be answer-preserving"


def test_compaction_leaves_no_deltas_at_all(world) -> None:
    """The point of merging the base in is to reach the no-ranking fast path."""
    settings, catalog, data = world
    for day in ("25", "26", "27"):
        _publish(data, day, [("006a", f"day{day}", f"2026-07-{day}T00:00:00Z")])

    DeltaCompactor(settings, catalog).compact("Opportunity")

    spec = catalog.get("Opportunity")
    assert spec.published_deltas() == ()
    assert spec.current_sources() == (spec.compacted_base(),)
    assert spec.compacted_base().name == COMPACTED_FILENAME

    from fakeforce.engine import DuckDBEngine

    assert "row_number()" not in DuckDBEngine(settings, catalog)._latest_version_sql(spec)


def test_compaction_is_idempotent(world) -> None:
    settings, catalog, data = world
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006a", "day26", "2026-07-26T00:00:00Z")])
    compactor = DeltaCompactor(settings, catalog)
    compactor.compact("Opportunity")
    answer = _visible(settings, catalog)

    second = compactor.compact("Opportunity")

    assert second.partitions_merged == 0, "one delta file is already compacted"
    assert _visible(settings, catalog) == answer


def test_a_partition_published_after_compaction_still_wins(world) -> None:
    settings, catalog, data = world
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006a", "day26", "2026-07-26T00:00:00Z")])
    DeltaCompactor(settings, catalog).compact("Opportunity")

    _publish(data, "28", [("006a", "day28", "2026-07-28T00:00:00Z")])

    assert _visible(settings, catalog)["006a"] == "day28"


def test_a_later_compaction_folds_into_the_previous_one(world) -> None:
    settings, catalog, data = world
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006b", "day26", "2026-07-26T00:00:00Z")])
    compactor = DeltaCompactor(settings, catalog)
    compactor.compact("Opportunity")
    _publish(data, "27", [("006a", "day27", "2026-07-27T00:00:00Z")])

    result = compactor.compact("Opportunity")

    assert result.partitions_merged == 1, "only the new partition is outstanding"
    assert _visible(settings, catalog) == {"006a": "day27", "006b": "day26"}
    remaining = sorted((data / "opportunities").rglob("*.parquet"))
    assert [path.name for path in remaining] == [COMPACTED_FILENAME]


def test_the_seed_base_is_superseded_not_rewritten(world) -> None:
    """The seed is read-only where FakeForce runs and must survive untouched."""
    settings, catalog, data = world
    seed = data / "crm_opportunities.parquet"
    before = seed.read_bytes()
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])

    DeltaCompactor(settings, catalog).compact("Opportunity")

    assert seed.read_bytes() == before
    assert catalog.get("Opportunity").compacted_base() != seed
    assert _visible(settings, catalog) == {"006a": "day25", "006b": "base-b"}


def test_compaction_removes_the_emptied_partition_directories(world) -> None:
    settings, catalog, data = world
    for day in ("25", "26"):
        _publish(data, day, [("006a", f"d{day}", f"2026-07-{day}T00:00:00Z")])

    DeltaCompactor(settings, catalog).compact("Opportunity")

    assert not (data / "opportunities" / "business_date=2026-07-25").exists()
    assert not (data / "opportunities" / "business_date=2026-07-26").exists()


def test_nothing_to_do_when_no_partition_was_published(world) -> None:
    settings, catalog, _ = world

    result = DeltaCompactor(settings, catalog).compact("Opportunity")

    assert result.partitions_merged == 0
    assert result.removed == 0


def test_compaction_refuses_an_object_it_cannot_resolve(world) -> None:
    settings, catalog, _ = world
    compactor = DeltaCompactor(settings, catalog)

    with pytest.raises(CompactionError, match="not configured"):
        compactor.compact("Contact")


def test_compaction_refuses_an_unversioned_object(tmp_path) -> None:
    data = tmp_path / "data"
    _write(data / "crm_accounts.parquet", [("001", "a", "2026-07-24T00:00:00Z")])
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {"version": 1, "objects": [{"name": "Account", "sources": ["crm_accounts.parquet"]}]}
        )
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data),
            "FAKEFORCE_DATA_ROOTS": str(data),
            "FAKEFORCE_CATALOG_PATH": str(tmp_path / "catalog.json"),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)

    with pytest.raises(CompactionError, match="no version field"):
        DeltaCompactor(settings, catalog).compact("Account")


def test_a_failed_merge_leaves_every_partition_in_place(world, monkeypatch) -> None:
    """A crash mid-merge must never lose a published partition."""
    settings, catalog, data = world
    for day in ("25", "26"):
        _publish(data, day, [("006a", f"d{day}", f"2026-07-{day}T00:00:00Z")])
    compactor = DeltaCompactor(settings, catalog)

    def explode(*args, **kwargs):
        raise RuntimeError("disk fell over")

    monkeypatch.setattr(DeltaCompactor, "_write_merged", explode)
    with pytest.raises(RuntimeError, match="disk fell over"):
        compactor.compact("Opportunity")

    assert len(sorted((data / "opportunities").rglob("*.parquet"))) == 2
    assert _visible(settings, catalog)["006a"] == "d26"
    monkeypatch.undo()
    assert compactor.compact("Opportunity").partitions_merged == 2


def test_dry_run_reports_without_changing_anything(world, capsys) -> None:
    settings, catalog, data = world
    for day in ("25", "26"):
        _publish(data, day, [("006a", f"d{day}", f"2026-07-{day}T00:00:00Z")])
    plan = DeltaCompactor(settings, catalog).plan("Opportunity")

    assert plan.is_worthwhile
    assert len(plan.partitions) == 2
    assert len(sorted((data / "opportunities").rglob("*.parquet"))) == 2


def test_cli_compacts_every_versioned_object(world, capsys, monkeypatch) -> None:
    settings, catalog, data = world
    for day in ("25", "26"):
        _publish(data, day, [("006a", f"d{day}", f"2026-07-{day}T00:00:00Z")])
    monkeypatch.setenv("FAKEFORCE_SEED_DIR", str(data))
    monkeypatch.setenv("FAKEFORCE_DATA_ROOTS", str(data))
    monkeypatch.setenv("FAKEFORCE_CATALOG_PATH", str(data.parent / "catalog.json"))
    monkeypatch.setenv("FAKEFORCE_STATE_DIR", str(data.parent / "state"))

    assert main(["--all"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["compaction"][0]["partitions_merged"] == 2


def test_compaction_ranks_through_a_compatibility_alias(tmp_path) -> None:
    """The deployed catalog versions by SystemModstamp, which is an alias.

    The alias is synthesized at read time and no stored file carries it, so
    compaction must rank by the column that actually exists and must not write
    the alias into the merged file -- doing so would leave the engine aliasing
    a column that is already there.
    """
    data = tmp_path / "data"
    _write(
        data / "crm_opportunities.parquet",
        [("006a", "base-a", "2026-07-24T00:00:00Z")],
    )
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "version": 1,
                "objects": [
                    {
                        "name": "Opportunity",
                        "sources": ["crm_opportunities.parquet"],
                        "delta_patterns": ["opportunities/**/*.parquet"],
                        "version_field": "SystemModstamp",
                        "compatibility_aliases": {"SystemModstamp": "LastModifiedDate"},
                        "id_field": "Id",
                        "soft_delete_field": "IsDeleted",
                    }
                ],
            }
        )
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data),
            "FAKEFORCE_DATA_ROOTS": str(data),
            "FAKEFORCE_CATALOG_PATH": str(tmp_path / "catalog.json"),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
            "FAKEFORCE_MEMORY_LIMIT": "256MB",
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006a", "day26", "2026-07-26T00:00:00Z")])

    result = DeltaCompactor(settings, catalog).compact("Opportunity")

    assert result.partitions_merged == 2
    merged = data / "opportunities" / "compacted" / COMPACTED_FILENAME
    assert "SystemModstamp" not in pq.ParquetFile(merged).schema_arrow.names
    assert _visible(settings, catalog) == {"006a": "day26"}
    with DuckDBEngine(settings, catalog).connection(objects=("Opportunity",)) as conn:
        assert conn.execute(
            'SELECT "SystemModstamp" FROM "ff_source_Opportunity"'
        ).fetchone()[0] == "2026-07-26T00:00:00Z"


def test_compaction_does_not_disturb_the_simulator_snapshot(world) -> None:
    """The simulator globs the same directory FakeForce compacts.

    It reads the seed base unioned with whatever Parquet it finds under the
    object's delta root, so a compacted base lands in its scan too. That is
    safe -- the compacted file holds every id, so its anti-join excludes the
    whole stale base -- but it is the interaction most likely to break in
    production, so it is pinned here rather than reasoned about.
    """
    import duckdb

    from simulator.crm_snapshot import CrmSnapshotReader
    from simulator.policy import SimulationPolicy

    settings, catalog, data = world
    _publish(data, "25", [("006a", "day25", "2026-07-25T00:00:00Z")])
    _publish(data, "26", [("006c", "new-c", "2026-07-26T00:00:00Z")])

    reader = CrmSnapshotReader(SimulationPolicy(), data)

    def snapshot():
        relation = reader._relation(data / "crm_opportunities.parquet", "opportunities")
        with duckdb.connect() as conn:
            return sorted(
                conn.execute(f'SELECT "Id", "Name" FROM {relation}').fetchall()
            )

    before = snapshot()
    assert before == [("006a", "day25"), ("006b", "base-b"), ("006c", "new-c")]

    DeltaCompactor(settings, catalog).compact("Opportunity")

    assert snapshot() == before
