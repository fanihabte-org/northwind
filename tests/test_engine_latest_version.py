"""Latest-version resolution: correctness first, then the cheap path.

The rule is that a query sees exactly one row per id, the newest one. These
tests pin that rule across every shape the catalog can take, so the SQL behind
it stays free to change.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakeforce.catalog import DatasetCatalog
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


def _engine(tmp_path, *, version_field: str | None = "LastModifiedDate") -> DuckDBEngine:
    data_root = tmp_path / "data"
    entry = {
        "name": "Opportunity",
        "sources": ["crm_opportunities.parquet"],
        "delta_patterns": ["opportunities/**/*.parquet"],
        "id_field": "Id",
        "soft_delete_field": "IsDeleted",
    }
    if version_field is not None:
        entry["version_field"] = version_field
    else:
        entry.pop("delta_patterns")
    (tmp_path / "catalog.json").write_text(
        json.dumps({"version": 1, "objects": [entry]})
    )
    settings = Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(data_root),
            "FAKEFORCE_DATA_ROOTS": str(data_root),
            "FAKEFORCE_CATALOG_PATH": str(tmp_path / "catalog.json"),
            "FAKEFORCE_STATE_DIR": str(tmp_path / "state"),
            "FAKEFORCE_MEMORY_LIMIT": "256MB",
        }
    )
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    return DuckDBEngine(settings, catalog)


def _rows(engine):
    with engine.connection(objects=("Opportunity",)) as conn:
        return {
            row[0]: row[1]
            for row in conn.execute(
                'SELECT "Id", "Name" FROM "ff_source_Opportunity" ORDER BY "Id"'
            ).fetchall()
        }


@pytest.fixture()
def base(tmp_path):
    _write(
        tmp_path / "data" / "crm_opportunities.parquet",
        [("006a", "base-a", "2026-07-24T00:00:00Z"), ("006b", "base-b", "2026-07-24T00:00:00Z")],
    )
    return tmp_path


def test_without_deltas_every_base_row_is_returned_once(base) -> None:
    engine = _engine(base)

    assert _rows(engine) == {"006a": "base-a", "006b": "base-b"}


def test_without_deltas_the_plan_does_not_rank_at_all(base) -> None:
    """Nothing can supersede anything, so the window is pure waste."""
    engine = _engine(base)
    spec = engine.catalog.get("Opportunity")

    assert "row_number()" not in engine._latest_version_sql(spec)


def test_a_delta_supersedes_its_base_row(base) -> None:
    _write(
        base / "data" / "opportunities" / "business_date=2026-07-25" / "delta.parquet",
        [("006a", "updated-a", "2026-07-25T00:00:00Z")],
    )
    engine = _engine(base)

    assert _rows(engine) == {"006a": "updated-a", "006b": "base-b"}


def test_the_newest_of_several_deltas_wins(base) -> None:
    for day, name in (("25", "first"), ("27", "newest"), ("26", "middle")):
        _write(
            base / "data" / "opportunities" / f"business_date=2026-07-{day}" / "delta.parquet",
            [("006a", name, f"2026-07-{day}T00:00:00Z")],
        )
    engine = _engine(base)

    assert _rows(engine)["006a"] == "newest"


def test_a_delta_may_introduce_an_id_the_base_never_had(base) -> None:
    _write(
        base / "data" / "opportunities" / "business_date=2026-07-25" / "delta.parquet",
        [("006c", "brand-new", "2026-07-25T00:00:00Z")],
    )
    engine = _engine(base)

    assert _rows(engine) == {"006a": "base-a", "006b": "base-b", "006c": "brand-new"}


def test_ranking_resumes_once_a_delta_exists(base) -> None:
    _write(
        base / "data" / "opportunities" / "business_date=2026-07-25" / "delta.parquet",
        [("006a", "updated-a", "2026-07-25T00:00:00Z")],
    )
    engine = _engine(base)
    spec = engine.catalog.get("Opportunity")

    assert "row_number()" in engine._latest_version_sql(spec)


def test_an_unversioned_object_keeps_every_row(tmp_path) -> None:
    """Without a version field there is no newest, so nothing is collapsed."""
    _write(
        tmp_path / "data" / "crm_opportunities.parquet",
        [("006a", "one", "2026-07-24T00:00:00Z"), ("006a", "two", "2026-07-25T00:00:00Z")],
    )
    engine = _engine(tmp_path, version_field=None)

    with engine.connection(objects=("Opportunity",)) as conn:
        count = conn.execute('SELECT count(*) FROM "ff_source_Opportunity"').fetchone()[0]

    assert count == 2


def test_a_newly_published_delta_changes_the_answer_without_a_restart(base) -> None:
    """FakeForce is long-running; the simulator publishes while it serves."""
    engine = _engine(base)
    assert _rows(engine)["006a"] == "base-a"

    _write(
        base / "data" / "opportunities" / "business_date=2026-07-25" / "delta.parquet",
        [("006a", "published-later", "2026-07-25T00:00:00Z")],
    )

    assert _rows(engine)["006a"] == "published-later"
