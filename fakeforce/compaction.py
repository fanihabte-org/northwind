"""Bounded, restart-safe compaction of published CRM delta partitions.

The simulator publishes one Parquet partition per object per business date and
nothing ever merges them, so the file count grows without bound. Two costs grow
with it: opening and scanning the files, and ranking their rows to resolve each
id to its newest record.

Compaction merges the published partitions into a single file holding one row
per id, then removes the partitions it merged. The seed is never rewritten --
it is mounted read-only in the deployed containers and is the deterministic
output of the generator -- so the merged file is published as a delta of its
own, beside the partitions it replaces.

    python -m fakeforce.compaction --all --dry-run
    python -m fakeforce.compaction --object Opportunity

Safety, in the order the steps happen:

* the partition list is snapshotted first, so a partition published while
  compaction runs is neither merged nor deleted;
* the merged file is written to a temporary path and published with
  ``os.replace``, so a reader never observes a partial file;
* only then are the merged partitions removed. Between those two steps a reader
  sees both the merged file and its sources, which resolve to the same newest
  row per id, so the answer never changes;
* a crash at any point leaves the partitions in place, and rerunning merges
  them again. The operation is idempotent.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb

from fakeforce.catalog import DatasetCatalog, DatasetSpec
from fakeforce.config import Settings
from fakeforce.engine import _quote_identifier, _quote_literal
from fakeforce.storage import require_disk_reserve

from fakeforce.catalog import COMPACTED_BASE_FILENAME, COMPACTED_DIRECTORY

COMPACTED_FILENAME = COMPACTED_BASE_FILENAME


class CompactionError(RuntimeError):
    """Compaction cannot run safely against this object."""


def _nearest_existing_ancestor_is_writable(path: Path) -> bool:
    """Whether ``mkdir(parents=True)`` could create ``path``, without trying.

    A delta root a partition was never published into does not exist yet --
    that is not the same as being read-only. Walk up to whichever ancestor
    does exist and check that one; ``mkdir(parents=True, exist_ok=True)``
    would need exactly that ancestor to be writable to create the rest.
    """
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            return False
        current = parent
    return os.access(current, os.W_OK)


@dataclass(frozen=True)
class CompactionPlan:
    object_name: str
    base_paths: tuple[Path, ...]
    partitions: tuple[Path, ...]
    compacted_path: Path

    @property
    def is_worthwhile(self) -> bool:
        """There is nothing to fold in until a partition has been published."""
        return len(self.partitions) > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "object": self.object_name,
            "partitions": len(self.partitions),
            "compacted_path": str(self.compacted_path),
            "worthwhile": self.is_worthwhile,
        }


@dataclass(frozen=True)
class CompactionResult:
    object_name: str
    partitions_merged: int
    rows_written: int
    bytes_written: int
    removed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "object": self.object_name,
            "partitions_merged": self.partitions_merged,
            "rows_written": self.rows_written,
            "bytes_written": self.bytes_written,
            "partitions_removed": self.removed,
        }


class DeltaCompactor:
    """Merges an object's published partitions into one file per object."""

    def __init__(self, settings: Settings, catalog: DatasetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog

    def plan(self, object_name: str) -> CompactionPlan:
        spec = self._spec(object_name)
        return CompactionPlan(
            object_name=spec.object_name,
            base_paths=spec.effective_base(),
            partitions=spec.published_deltas(),
            compacted_path=self._compacted_path(spec),
        )

    def compact(self, object_name: str) -> CompactionResult:
        plan = self.plan(object_name)
        spec = self._spec(object_name)
        if not plan.is_worthwhile:
            return CompactionResult(plan.object_name, 0, 0, 0, 0)

        destination = plan.compacted_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        require_disk_reserve(destination.parent, self.settings.disk_reserve_bytes)

        # The base is merged in as well, so the result supersedes it outright
        # and the object is left with no deltas at all.
        rows, written_bytes = self._write_merged(
            spec, plan.base_paths + plan.partitions, destination
        )
        removed = self._remove(plan.partitions)
        self.catalog.refresh()
        return CompactionResult(
            plan.object_name, len(plan.partitions), rows, written_bytes, removed
        )

    def compact_all(self) -> list[CompactionResult]:
        results = []
        for object_name in self.catalog.object_names:
            spec = self.catalog.get(object_name)
            if spec is None or spec.version_field is None or not spec.delta_patterns:
                continue
            results.append(self.compact(object_name))
        return results

    def _spec(self, object_name: str) -> DatasetSpec:
        spec = self.catalog.get(object_name)
        if spec is None:
            raise CompactionError(f"object is not configured: {object_name}")
        if spec.version_field is None:
            raise CompactionError(
                f"{spec.object_name} has no version field, so it has no newest row to keep"
            )
        if not spec.delta_patterns:
            raise CompactionError(f"{spec.object_name} publishes no delta partitions")
        return spec

    def _compacted_path(self, spec: DatasetSpec) -> Path:
        """The first configured delta root that can actually be written to.

        ``delta_roots()`` returns every configured root sorted, with no
        guarantee any given one is writable -- a deployment's seed and state
        roots are both valid delta roots, but the seed is mounted read-only
        and happens to sort first ("seed" < "state"). Picking ``roots[0]``
        unconditionally sent compaction's output at a read-only mount in
        production. Only inspects existing directories, so this stays safe
        to call from ``--dry-run``: it never creates anything.
        """
        roots = spec.delta_roots()
        writable = [root for root in roots if _nearest_existing_ancestor_is_writable(root)]
        if not writable:
            raise CompactionError(
                f"{spec.object_name} has no writable delta root among {roots or '(none configured)'}"
            )
        return writable[0] / COMPACTED_DIRECTORY / COMPACTED_FILENAME

    def _write_merged(
        self, spec: DatasetSpec, partitions: tuple[Path, ...], destination: Path
    ) -> tuple[int, int]:
        """Rank the partitions among themselves and publish one row per id.

        DuckDB streams the copy and spills under the configured limits, so this
        never materializes the merged object in Python.
        """
        # The configured version field may be a compatibility alias, which the
        # engine synthesizes at read time and the stored files do not carry.
        # Rank by the column that actually exists, and write only raw columns,
        # so the merged file keeps the same schema as the partitions it
        # replaces and the engine can alias it exactly as it aliased them.
        version_column = dict(spec.compatibility_aliases).get(
            spec.version_field, spec.version_field
        )
        sources = ", ".join(_quote_literal(path) for path in partitions)
        reader = (
            f"read_parquet([{sources}], union_by_name = true, hive_partitioning = false)"
        )
        merged = (
            f"SELECT * EXCLUDE (__fakeforce_compaction_rank) FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY {_quote_identifier(spec.id_field)} "
            f"ORDER BY {_quote_identifier(version_column)} DESC) "
            f"AS __fakeforce_compaction_rank FROM {reader}) "
            "WHERE __fakeforce_compaction_rank = 1"
        )
        handle, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.stem}.", suffix=".tmp", dir=destination.parent
        )
        os.close(handle)
        temporary = Path(temporary_name)
        temporary.unlink()
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit = ?", [self.settings.memory_limit])
            connection.execute("SET temp_directory = ?", [str(self.settings.temp_directory)])
            connection.execute("SET max_temp_directory_size = ?", [self.settings.max_temp_size])
            connection.execute(
                f"COPY ({merged}) TO {_quote_literal(temporary)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            rows = connection.execute(
                f"SELECT count(*) FROM read_parquet({_quote_literal(temporary)})"
            ).fetchone()[0]
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            connection.close()
        written_bytes = temporary.stat().st_size
        os.replace(temporary, destination)
        return rows, written_bytes

    @staticmethod
    def _remove(partitions: tuple[Path, ...]) -> int:
        removed = 0
        for path in partitions:
            try:
                path.unlink()
                removed += 1
            except FileNotFoundError:
                continue
            parent = path.parent
            try:
                next(parent.iterdir())
            except StopIteration:
                parent.rmdir()
            except OSError:
                continue
        return removed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--object", help="compact one configured object")
    group.add_argument("--all", action="store_true", help="compact every versioned object")
    parser.add_argument(
        "--dry-run", action="store_true", help="report what would be merged, change nothing"
    )
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    catalog = DatasetCatalog.from_file(settings.catalog_path, settings.data_roots)
    compactor = DeltaCompactor(settings, catalog)

    if args.dry_run:
        names = (
            [args.object]
            if args.object
            else [
                name
                for name in catalog.object_names
                if (spec := catalog.get(name)) is not None
                and spec.version_field is not None
                and spec.delta_patterns
            ]
        )
        payload = [compactor.plan(name).to_dict() for name in names]
    else:
        results = (
            compactor.compact_all() if args.all else [compactor.compact(args.object)]
        )
        payload = [result.to_dict() for result in results]

    print(json.dumps({"compaction": payload}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
