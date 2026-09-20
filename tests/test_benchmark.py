from __future__ import annotations

import json

import pytest

from fakeforce.benchmark import (
    BenchmarkReport,
    PhaseTiming,
    benchmark_read_path,
    build_delta_fixture,
    main,
    run_delta_sweep,
    time_phase,
)
from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings


def _settings(catalog_path, state_directory) -> Settings:
    return Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(catalog_path.parent / "data"),
            "FAKEFORCE_DATA_ROOTS": str(catalog_path.parent / "data"),
            "FAKEFORCE_CATALOG_PATH": str(catalog_path),
            "FAKEFORCE_STATE_DIR": str(state_directory),
            "FAKEFORCE_MEMORY_LIMIT": "256MB",
        }
    )


def test_time_phase_runs_the_callable_once_per_iteration() -> None:
    calls = []
    timing = time_phase("probe", lambda: calls.append(1), iterations=4)

    assert len(calls) == 4
    assert timing.name == "probe"
    assert timing.iterations == 4
    assert timing.total_seconds >= 0.0
    assert timing.seconds_per_iteration == pytest.approx(timing.total_seconds / 4)


def test_time_phase_rejects_a_non_positive_iteration_count() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        time_phase("probe", lambda: None, iterations=0)


def test_fixture_writes_the_deployed_delta_partition_layout(tmp_path) -> None:
    catalog_path = build_delta_fixture(
        tmp_path, base_rows=50, delta_partitions=3, rows_per_delta=5
    )

    assert (tmp_path / "data" / "crm_opportunity.parquet").is_file()
    partitions = sorted(
        path.parent.name for path in (tmp_path / "data" / "opportunity").rglob("delta.parquet")
    )
    assert partitions == [
        "business_date=2026-07-25",
        "business_date=2026-07-26",
        "business_date=2026-07-27",
    ]

    catalog = DatasetCatalog.from_file(catalog_path, (tmp_path / "data",))
    spec = catalog.get("Opportunity")
    assert spec is not None
    assert spec.version_field == "SystemModstamp"
    assert len(spec.current_sources()) == 4


def test_fixture_deltas_actually_supersede_base_rows(tmp_path) -> None:
    """The fixture is only useful if deduplication has real work to do."""
    catalog_path = build_delta_fixture(
        tmp_path, base_rows=40, delta_partitions=2, rows_per_delta=10
    )
    settings = _settings(catalog_path, tmp_path / "state")
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)

    from fakeforce.engine import DuckDBEngine

    engine = DuckDBEngine(settings, catalog)
    with engine.connection() as conn:
        total, distinct = conn.execute(
            'SELECT count(*), count(DISTINCT "Id") FROM "ff_source_Opportunity"'
        ).fetchone()
        newest = conn.execute(
            'SELECT max("LastModifiedDate") FROM "ff_source_Opportunity"'
        ).fetchone()[0]

    assert total == 40, "deduplication must collapse superseded rows"
    assert distinct == 40
    assert newest.startswith("2026-07-26"), "the newest delta must win"


def test_benchmark_reports_every_phase_with_non_negative_derived_costs(tmp_path) -> None:
    catalog_path = build_delta_fixture(
        tmp_path, base_rows=200, delta_partitions=2, rows_per_delta=20
    )
    settings = _settings(catalog_path, tmp_path / "state")
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)

    report = benchmark_read_path(
        settings,
        catalog,
        object_name="Opportunity",
        page_size=50,
        iterations=1,
        cursor_root=tmp_path / "cursors",
    )

    assert isinstance(report, BenchmarkReport)
    assert report.object_name == "Opportunity"
    assert report.source_file_count == 3
    assert report.delta_partitions == 2
    assert report.source_bytes > 0

    expected = {
        "catalog_snapshot_id",
        "source_resolution",
        "connect_without_views",
        "connect_with_views",
        "connect_scoped_view",
        "query_plan",
        "first_page",
        "first_page_with_cursor_index",
        "view_construction",
        "scoped_view_construction",
        "cursor_index",
    }
    assert {timing.name for timing in report.phases} == expected
    for timing in report.phases:
        assert isinstance(timing, PhaseTiming)
        assert timing.total_seconds >= 0.0, f"{timing.name} must never be negative"


def test_benchmark_resolves_object_names_case_insensitively(tmp_path) -> None:
    catalog_path = build_delta_fixture(tmp_path, base_rows=20, delta_partitions=0)
    settings = _settings(catalog_path, tmp_path / "state")
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)

    report = benchmark_read_path(
        settings,
        catalog,
        object_name="opportunity",
        page_size=10,
        iterations=1,
        cursor_root=tmp_path / "cursors",
    )

    assert report.object_name == "Opportunity"


def test_benchmark_rejects_an_unconfigured_object(tmp_path) -> None:
    catalog_path = build_delta_fixture(tmp_path, base_rows=20, delta_partitions=0)
    settings = _settings(catalog_path, tmp_path / "state")
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)

    with pytest.raises(KeyError, match="not configured"):
        benchmark_read_path(
            settings, catalog, object_name="Contact", iterations=1, cursor_root=tmp_path
        )


def test_delta_sweep_grows_the_scanned_source_count(tmp_path) -> None:
    reports = run_delta_sweep(
        [0, 4],
        base_rows=60,
        rows_per_delta=5,
        page_size=25,
        iterations=1,
        workspace=tmp_path,
    )

    assert [report.delta_partitions for report in reports] == [0, 4]
    assert [report.source_file_count for report in reports] == [1, 5]


def test_fixture_is_deterministic_for_a_fixed_seed(tmp_path) -> None:
    first = build_delta_fixture(tmp_path / "a", base_rows=30, delta_partitions=1)
    second = build_delta_fixture(tmp_path / "b", base_rows=30, delta_partitions=1)

    assert (first.parent / "data" / "crm_opportunity.parquet").read_bytes() == (
        second.parent / "data" / "crm_opportunity.parquet"
    ).read_bytes()


def test_fixture_validates_its_arguments(tmp_path) -> None:
    with pytest.raises(ValueError, match="base_rows"):
        build_delta_fixture(tmp_path, base_rows=0)
    with pytest.raises(ValueError, match="delta_partitions"):
        build_delta_fixture(tmp_path, base_rows=10, delta_partitions=-1)
    with pytest.raises(ValueError, match="rows_per_delta"):
        build_delta_fixture(tmp_path, base_rows=10, rows_per_delta=0)


def test_cli_writes_a_comparable_json_report(tmp_path, capsys) -> None:
    output = tmp_path / "benchmark.json"
    exit_code = main(
        [
            "--deltas", "0,2",
            "--base-rows", "40",
            "--rows-per-delta", "5",
            "--page-size", "20",
            "--iterations", "1",
            "--output", str(output),
        ]
    )
    capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(output.read_text())
    assert payload["api_version"] == "v60.0"
    assert [report["delta_partitions"] for report in payload["reports"]] == [0, 2]
    assert payload["reports"][0]["phases"][0]["iterations"] == 1


def test_cli_rejects_a_malformed_delta_sweep() -> None:
    with pytest.raises(SystemExit):
        main(["--deltas", "-1"])
