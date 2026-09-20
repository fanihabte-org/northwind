"""Phase-resolved timing for the FakeForce read path.

Optimization work needs to attribute cost to a *phase* rather than to a request.
A request that takes four seconds tells you nothing about whether the time went
into resolving sources, rebuilding views, or executing SQL, and an optimization
that cannot name the phase it moves cannot be shown to have worked.

This module times each phase separately against a catalog whose delta-partition
count the caller controls, so the growth curve of a change is measurable before
and after it lands.

    python -m fakeforce.benchmark --deltas 0,30,365
    python -m fakeforce.benchmark --use-env-catalog --object Opportunity

The synthetic fixture mirrors the deployed CRM layout: a Parquet base file plus
``<object>/business_date=YYYY-MM-DD/delta.parquet`` partitions, versioned through
a ``SystemModstamp`` compatibility alias, exactly as ``fakeforce/catalog.json``
and ``simulator/crm_storage.py`` arrange them.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import tempfile
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq

from fakeforce.catalog import DatasetCatalog
from fakeforce.config import Settings
from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.engine import DuckDBEngine
from fakeforce.query_service import LazyQueryService

T = TypeVar("T")

API_VERSION = "v60.0"
DEFAULT_ITERATIONS = 3
DEFAULT_PAGE_SIZE = 2000


@dataclass(frozen=True)
class PhaseTiming:
    """Wall-clock cost of one isolated phase of the read path."""

    name: str
    iterations: int
    total_seconds: float

    @property
    def seconds_per_iteration(self) -> float:
        return self.total_seconds / self.iterations if self.iterations else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_seconds": round(self.total_seconds, 6),
            "seconds_per_iteration": round(self.seconds_per_iteration, 6),
        }


@dataclass(frozen=True)
class BenchmarkReport:
    """One measured configuration, comparable across code revisions."""

    object_name: str
    query: str
    delta_partitions: int
    catalog_objects: int
    source_file_count: int
    source_bytes: int
    page_size: int
    phases: tuple[PhaseTiming, ...] = field(default_factory=tuple)

    def phase(self, name: str) -> PhaseTiming | None:
        for timing in self.phases:
            if timing.name == name:
                return timing
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "object": self.object_name,
            "query": self.query,
            "delta_partitions": self.delta_partitions,
            "catalog_objects": self.catalog_objects,
            "source_file_count": self.source_file_count,
            "source_bytes": self.source_bytes,
            "page_size": self.page_size,
            "phases": [timing.to_dict() for timing in self.phases],
        }


def time_phase(name: str, work: Callable[[], Any], iterations: int) -> PhaseTiming:
    """Run ``work`` ``iterations`` times and report the total elapsed time.

    The caller's callable owns any setup it needs.  No warm-up round is
    discarded: a cold first call is exactly the cost a request pays, because
    FakeForce builds its connection and views per request.
    """
    if iterations <= 0:
        raise ValueError("iterations must be greater than zero")
    started = time.perf_counter()
    for _ in range(iterations):
        work()
    return PhaseTiming(name, iterations, time.perf_counter() - started)


def _salesforce_id(prefix: str, number: int) -> str:
    """Build an all-digit Salesforce-shaped id, leading zeros included."""
    return f"{prefix}{number:015d}"


def _crm_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("Id", pa.string()),
            pa.field("IsDeleted", pa.bool_()),
            pa.field("Name", pa.string()),
            pa.field("StageName", pa.string()),
            pa.field("Amount", pa.float64()),
            pa.field("OwnerId", pa.string()),
            pa.field("LastModifiedDate", pa.string()),
        ]
    )


def _rows(
    ids: Iterable[str], modified_on: date, generator: random.Random
) -> dict[str, list[Any]]:
    stages = ("Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won")
    identifiers = list(ids)
    return {
        "Id": identifiers,
        "IsDeleted": [False] * len(identifiers),
        "Name": [f"Record {identifier[-6:]}" for identifier in identifiers],
        "StageName": [generator.choice(stages) for _ in identifiers],
        "Amount": [round(generator.uniform(500.0, 500_000.0), 2) for _ in identifiers],
        "OwnerId": [_salesforce_id("005", generator.randrange(1, 500)) for _ in identifiers],
        "LastModifiedDate": [f"{modified_on.isoformat()}T12:00:00Z"] * len(identifiers),
    }


def build_delta_fixture(
    root: Path,
    *,
    object_name: str = "Opportunity",
    base_rows: int = 50_000,
    delta_partitions: int = 0,
    rows_per_delta: int = 500,
    seed: int = 20260728,
    baseline: date = date(2026, 7, 24),
    companions: int = 0,
) -> Path:
    """Write a deterministic base file plus ``delta_partitions`` daily deltas.

    Each delta updates rows that already exist in the base, which is the case
    that forces deduplication to do real work.

    ``companions`` adds that many further objects to the catalog, each with the
    same shape.  The deployed catalog holds several objects and views are built
    per connection, so a benchmark with a single object cannot see the cost an
    unscoped connection pays for objects the query never reads.

    Returns the catalog path.
    """
    if base_rows <= 0:
        raise ValueError("base_rows must be greater than zero")
    if delta_partitions < 0:
        raise ValueError("delta_partitions cannot be negative")
    if rows_per_delta <= 0:
        raise ValueError("rows_per_delta must be greater than zero")

    generator = random.Random(seed)
    schema = _crm_schema()
    data_root = root / "data"
    delta_root = data_root / object_name.lower()
    data_root.mkdir(parents=True, exist_ok=True)

    base_ids = [_salesforce_id("006", number) for number in range(1, base_rows + 1)]
    base_path = data_root / f"crm_{object_name.lower()}.parquet"
    pq.write_table(
        pa.table(_rows(base_ids, baseline, generator), schema=schema),
        base_path,
        compression="zstd",
    )

    for offset in range(1, delta_partitions + 1):
        business_date = baseline + timedelta(days=offset)
        updated = generator.sample(base_ids, min(rows_per_delta, len(base_ids)))
        partition = delta_root / f"business_date={business_date.isoformat()}"
        partition.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(_rows(updated, business_date, generator), schema=schema),
            partition / "delta.parquet",
            compression="zstd",
        )

    entries = [_catalog_entry(object_name, base_path.name)]
    for index in range(companions):
        companion = f"Companion{index}"
        companion_base = data_root / f"crm_{companion.lower()}.parquet"
        pq.write_table(
            pa.table(_rows(base_ids, baseline, generator), schema=schema),
            companion_base,
            compression="zstd",
        )
        for offset in range(1, delta_partitions + 1):
            business_date = baseline + timedelta(days=offset)
            updated = generator.sample(base_ids, min(rows_per_delta, len(base_ids)))
            partition = data_root / companion.lower() / f"business_date={business_date.isoformat()}"
            partition.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table(_rows(updated, business_date, generator), schema=schema),
                partition / "delta.parquet",
                compression="zstd",
            )
        entries.append(_catalog_entry(companion, companion_base.name))

    catalog_path = root / "catalog.json"
    catalog_path.write_text(
        json.dumps({"version": 1, "objects": entries}, indent=2), encoding="utf-8"
    )
    return catalog_path


def _catalog_entry(object_name: str, base_file: str) -> dict[str, Any]:
    return {
        "name": object_name,
        "sources": [base_file],
        "delta_patterns": [f"{object_name.lower()}/**/*.parquet"],
        "version_field": "SystemModstamp",
        "compatibility_aliases": {"SystemModstamp": "LastModifiedDate"},
        "id_field": "Id",
        "soft_delete_field": "IsDeleted",
        "mode": "read_only",
    }


def benchmark_read_path(
    settings: Settings,
    catalog: DatasetCatalog,
    *,
    object_name: str,
    query: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    iterations: int = DEFAULT_ITERATIONS,
    cursor_root: Path | None = None,
) -> BenchmarkReport:
    """Time every phase a synchronous query pays for, in isolation."""
    spec = catalog.get(object_name)
    if spec is None:
        raise KeyError(f"object is not configured: {object_name}")
    soql = query or f"SELECT Id, Name FROM {object_name}"

    engine = DuckDBEngine(settings, catalog)
    service = LazyQueryService(catalog, engine, API_VERSION)
    artifacts = CursorArtifactStore(
        cursor_root or settings.state_directory / "artifacts" / "cursors"
    )

    sources = spec.current_sources()
    source_bytes = sum(path.stat().st_size for path in sources)

    def open_connection(create_views: bool, scoped: bool = False) -> None:
        objects = (spec.object_name,) if scoped else None
        with engine.connection(create_source_views=create_views, objects=objects):
            pass

    phases = [
        time_phase("catalog_snapshot_id", lambda: catalog.snapshot_id, iterations),
        time_phase("source_resolution", spec.current_sources, iterations),
        time_phase("connect_without_views", lambda: open_connection(False), iterations),
        time_phase("connect_with_views", lambda: open_connection(True), iterations),
        time_phase(
            "connect_scoped_view", lambda: open_connection(True, scoped=True), iterations
        ),
        time_phase("query_plan", lambda: service.plan(soql, False), iterations),
        time_phase(
            "first_page",
            lambda: service.fetch_page(soql, False, page_size),
            iterations,
        ),
        time_phase(
            "first_page_with_cursor_index",
            lambda: service.fetch_page_with_cursor_index(
                soql, False, page_size, 0, f"bench{time.perf_counter_ns():x}", artifacts
            ),
            iterations,
        ),
    ]
    phases.append(
        _derive("view_construction", phases, "connect_with_views", "connect_without_views")
    )
    phases.append(
        _derive("scoped_view_construction", phases, "connect_scoped_view", "connect_without_views")
    )
    phases.append(
        _derive("cursor_index", phases, "first_page_with_cursor_index", "first_page")
    )

    return BenchmarkReport(
        object_name=spec.object_name,
        query=soql,
        delta_partitions=max(0, len(sources) - 1),
        catalog_objects=len(catalog.object_names),
        source_file_count=len(sources),
        source_bytes=source_bytes,
        page_size=page_size,
        phases=tuple(phases),
    )


def _derive(
    name: str, phases: Sequence[PhaseTiming], minuend: str, subtrahend: str
) -> PhaseTiming:
    """Report the cost one phase adds over another, never below zero."""
    timings = {timing.name: timing for timing in phases}
    larger, smaller = timings[minuend], timings[subtrahend]
    return PhaseTiming(
        name, larger.iterations, max(0.0, larger.total_seconds - smaller.total_seconds)
    )


def _settings_for(catalog_path: Path, state_directory: Path) -> Settings:
    return Settings.from_env(
        {
            "FAKEFORCE_SEED_DIR": str(catalog_path.parent / "data"),
            "FAKEFORCE_DATA_ROOTS": str(catalog_path.parent / "data"),
            "FAKEFORCE_CATALOG_PATH": str(catalog_path),
            "FAKEFORCE_STATE_DIR": str(state_directory),
            "FAKEFORCE_MEMORY_LIMIT": "1GB",
        }
    )


def run_delta_sweep(
    delta_counts: Sequence[int],
    *,
    object_name: str = "Opportunity",
    base_rows: int = 50_000,
    rows_per_delta: int = 500,
    page_size: int = DEFAULT_PAGE_SIZE,
    iterations: int = DEFAULT_ITERATIONS,
    workspace: Path | None = None,
    companions: int = 0,
) -> list[BenchmarkReport]:
    """Measure the same query against a growing number of delta partitions."""
    reports: list[BenchmarkReport] = []
    for delta_partitions in delta_counts:
        context = (
            nullcontext(workspace / f"deltas-{delta_partitions}")
            if workspace is not None
            else tempfile.TemporaryDirectory()
        )
        with context as raw_root:
            root = Path(raw_root)
            root.mkdir(parents=True, exist_ok=True)
            catalog_path = build_delta_fixture(
                root,
                object_name=object_name,
                base_rows=base_rows,
                delta_partitions=delta_partitions,
                rows_per_delta=rows_per_delta,
                companions=companions,
            )
            settings = _settings_for(catalog_path, root / "state")
            catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
            reports.append(
                benchmark_read_path(
                    settings,
                    catalog,
                    object_name=object_name,
                    page_size=page_size,
                    iterations=iterations,
                    cursor_root=root / "cursors",
                )
            )
    return reports


def _parse_delta_counts(value: str) -> list[int]:
    counts = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        parsed = int(part)
        if parsed < 0:
            raise argparse.ArgumentTypeError("delta counts cannot be negative")
        counts.append(parsed)
    if not counts:
        raise argparse.ArgumentTypeError("provide at least one delta count")
    return counts


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--deltas",
        type=_parse_delta_counts,
        default=[0, 30, 365],
        help="comma-separated delta-partition counts to sweep (default: 0,30,365)",
    )
    parser.add_argument("--object", default="Opportunity")
    parser.add_argument("--base-rows", type=int, default=50_000)
    parser.add_argument("--rows-per-delta", type=int, default=500)
    parser.add_argument(
        "--companions",
        type=int,
        default=0,
        help="extra catalog objects the query never reads (default: 0)",
    )
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument(
        "--use-env-catalog",
        action="store_true",
        help="benchmark the catalog this deployment is configured with, not a fixture",
    )
    parser.add_argument("--query", default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)

    if args.use_env_catalog:
        settings = Settings.from_env()
        catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
        with tempfile.TemporaryDirectory() as cursors:
            reports = [
                benchmark_read_path(
                    settings,
                    catalog,
                    object_name=args.object,
                    query=args.query,
                    page_size=args.page_size,
                    iterations=args.iterations,
                    cursor_root=Path(cursors),
                )
            ]
    else:
        reports = run_delta_sweep(
            args.deltas,
            object_name=args.object,
            base_rows=args.base_rows,
            rows_per_delta=args.rows_per_delta,
            page_size=args.page_size,
            iterations=args.iterations,
            companions=args.companions,
        )

    payload = json.dumps(
        {"api_version": API_VERSION, "reports": [report.to_dict() for report in reports]},
        indent=2,
    )
    if args.output is not None:
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
