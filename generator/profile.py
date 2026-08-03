"""Profile seed files with DuckDB without materialising them in Python memory."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _source(path: Path) -> str:
    literal = "'" + str(path).replace("'", "''") + "'"
    if path.suffix == ".parquet":
        return f"read_parquet({literal})"
    if path.suffix in {".csv", ".gz"}:
        return f"read_csv_auto({literal})"
    raise ValueError(f"unsupported seed file: {path.name}")


def profile_dataset(path: Path, *, target_partition_rows: int = 1_000_000) -> dict:
    """Return bounded-memory file shape, approximate cardinality and a partition plan."""
    if target_partition_rows <= 0:
        raise ValueError("target_partition_rows must be greater than zero")
    source = _source(path)
    with tempfile.TemporaryDirectory(prefix="northwind-profile-") as spill:
        connection = duckdb.connect()
        try:
            connection.execute("SET memory_limit = '512MB'")
            connection.execute(f"SET temp_directory = '{spill}'")
            columns = [row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]
            stats = ["count(*) AS row_count"]
            for index, column in enumerate(columns):
                quoted = _quote(column)
                stats += [f"count(*) - count({quoted}) AS nulls_{index}", f"approx_count_distinct({quoted}) AS distinct_{index}"]
            row = connection.execute(f"SELECT {', '.join(stats)} FROM {source}").fetchone()
        finally:
            connection.close()
    row_count = int(row[0])
    column_stats = {column: {"null_count": int(row[1 + index * 2]), "approx_distinct_count": int(row[2 + index * 2])} for index, column in enumerate(columns)}
    dates = [name for name in columns if name.lower().endswith(("_date", "_at")) or name in {"CreatedDate", "LastModifiedDate", "CloseDate"}]
    date_column = next((name for name in dates if column_stats[name]["approx_distinct_count"] > 1), None)
    partitioned = row_count > target_partition_rows and date_column is not None
    return {"file": str(path), "byte_size": path.stat().st_size, "row_count": row_count, "columns": column_stats,
            "partition_recommendation": {"strategy": "date" if partitioned else "none", "column": date_column if partitioned else None,
              "target_rows_per_partition": target_partition_rows, "estimated_partitions": max(1, -(-row_count // target_partition_rows))}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-directory", type=Path, default=ROOT / "seed")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--target-partition-rows", type=int, default=1_000_000)
    args = parser.parse_args()
    files = sorted(path for path in args.seed_directory.iterdir() if path.suffix in {".parquet", ".csv", ".gz"})
    report = {"datasets": [profile_dataset(path, target_partition_rows=args.target_partition_rows) for path in files]}
    output = args.output or args.seed_directory / "_profile.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {len(report['datasets'])} dataset profiles to {output}")


if __name__ == "__main__":
    main()
