"""
Load the generated datasets into Postgres.

Applies each source system's independently owned, versioned migrations and then
COPYs its files in.
Ops and ERP are intentionally separate Postgres databases. If any system-local
primary key, foreign key, unique, or check constraint is violated, that system's
transaction aborts and nothing is committed.

    python generator/load.py                        # ops + erp
    python generator/load.py --system ops
    python generator/load.py --migrate-only
    OPS_PG_DSN='postgresql://...' python generator/load.py --system ops

Reads whichever format the generator produced -- .csv.gz, .csv or .parquet.
Everything is streamed, so loading tens of millions of rows does not need
tens of millions of rows' worth of memory.
"""

from __future__ import annotations

import argparse
import gzip
import io
import os
import sys
import time
from pathlib import Path

from migrate import apply_migrations

try:
    import psycopg
except ImportError:
    sys.exit("pip install 'psycopg[binary]' first")

ROOT = Path(__file__).resolve().parents[1]
SEED = Path(os.getenv("NORTHWIND_SEED_DIR", ROOT / "seed"))

OPS_DSN = os.getenv("OPS_PG_DSN", "postgresql://ops:ops@localhost:5433/ops")
ERP_DSN = os.getenv("ERP_PG_DSN", "postgresql://erp:erp@localhost:5434/erp")

OPS_TABLES = [
    ("ops.customers",         "ops_customers"),
    ("ops.products",          "ops_products"),
    ("ops.warehouses",        "ops_warehouses"),
    ("ops.carriers",          "ops_carriers"),
    ("ops.orders",            "ops_orders"),
    ("ops.order_lines",       "ops_order_lines"),
    ("ops.shipments",         "ops_shipments"),
    ("ops.support_cases",     "ops_support_cases"),
]
ERP_TABLES = [
    ("erp.companies",         "erp_companies"),
    ("erp.cost_centers",      "erp_cost_centers"),
    ("erp.gl_accounts",       "erp_gl_accounts"),
    ("erp.fx_rates",          "erp_fx_rates"),
    ("erp.revenue_postings",  "erp_revenue_postings"),
]
SYSTEMS = {
    "ops": (OPS_DSN, OPS_TABLES),
    "erp": (ERP_DSN, ERP_TABLES),
}


BATCH = 250_000

# Seed files generated before the audit-metadata migration deliberately do not
# contain these columns.  Keep them loadable: production source tables enforce
# NOT NULL audit fields before COPY, so the values must be derived while the
# rows are still in a temporary staging table.  The expressions mirror the
# checkpointed audit backfill and never overwrite values supplied by a newer
# seed.
LEGACY_AUDIT_EXPRESSIONS: dict[str, dict[str, str]] = {
    "ops.products": {"created_at": "launch_date::timestamp + time '02:00'"},
    "ops.warehouses": {
        "created_at": "timestamp '2022-01-03 08:00:00'",
        "updated_at": "timestamp '2022-01-03 08:00:00'",
    },
    "ops.carriers": {
        "created_at": "timestamp '2022-01-03 08:00:00'",
        "updated_at": "timestamp '2022-01-03 08:00:00'",
    },
    "ops.order_lines": {"created_at": "updated_at"},
    "ops.shipments": {
        "created_at": "ship_date::timestamp + time '08:00'",
        "updated_at": "coalesce(delivered_date, ship_date)::timestamp + time '17:00'",
    },
    "ops.invoices": {"updated_at": "created_at"},
    "ops.support_cases": {
        "created_at": "opened_at",
        "updated_at": "opened_at + coalesce(resolution_hours, 0) * interval '1 hour'",
    },
    "erp.companies": {
        "created_at": "timestamp '2022-01-03 08:00:00'",
        "updated_at": "timestamp '2022-01-03 08:00:00'",
    },
    "erp.cost_centers": {
        "created_at": "valid_from::timestamp + time '08:00'",
        "updated_at": "coalesce(valid_to, valid_from)::timestamp + time '17:00'",
    },
    "erp.gl_accounts": {
        "created_at": "timestamp '2022-01-03 08:00:00'",
        "updated_at": "timestamp '2022-01-03 08:00:00'",
    },
    "erp.fx_rates": {"created_at": "loaded_at", "updated_at": "loaded_at"},
    "erp.revenue_postings": {"created_at": "posted_at"},
}


def find_seed(stem: str) -> Path:
    for suffix in (".csv.gz", ".csv", ".parquet"):
        p = SEED / f"{stem}{suffix}"
        if p.exists():
            return p
    raise FileNotFoundError(
        f"No seed file for {stem} in {SEED}. Run `python generator/generate.py` first.")


def _copy_sql(table: str, cols: list[str]) -> str:
    quoted = ", ".join(f'"{c}"' for c in cols)
    return f"COPY {table} ({quoted}) FROM STDIN WITH (FORMAT csv, NULL '', QUOTE '\"')"


def copy_csv(cur, table: str, path: Path, cols: list[str] | None = None) -> int:
    """Stream the file straight into COPY without materialising it."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        cols = cols or fh.readline().rstrip("\r\n").split(",")
        if cols and fh.tell() == 0:
            fh.readline()
        n = 0
        with cur.copy(_copy_sql(table, cols)) as cp:
            while True:
                chunk = fh.read(1 << 22)
                if not chunk:
                    break
                n += chunk.count("\n")
                cp.write(chunk)
    return n


def copy_parquet(cur, table: str, path: Path, cols: list[str] | None = None) -> int:
    """Stream parquet row-group batches through COPY.

    Arrow writes nulls as empty fields, which is exactly what NULL '' expects,
    so a null Int64 stays null rather than becoming a zero or a string.
    """
    import pyarrow.parquet as pq
    from pyarrow import csv as pacsv

    pf = pq.ParquetFile(path)
    cols = cols or pf.schema_arrow.names
    opts = pacsv.WriteOptions(include_header=False)
    n = 0
    with cur.copy(_copy_sql(table, cols)) as cp:
        for batch in pf.iter_batches(batch_size=BATCH):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, opts)
            cp.write(buf.getvalue())
            n += batch.num_rows
    return n


def _target_columns(cur, table: str) -> list[str]:
    schema, name = table.split(".", 1)
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        [schema, name],
    )
    return [row[0] for row in cur.fetchall()]


def _copy_legacy_seed(cur, table: str, path: Path, seed_columns: list[str]) -> int:
    target_columns = _target_columns(cur, table)
    missing = set(target_columns) - set(seed_columns)
    expressions = LEGACY_AUDIT_EXPRESSIONS.get(table, {})
    unsupported = missing - set(expressions)
    if unsupported:
        raise ValueError(f"seed {path.name} is missing required columns for {table}: {sorted(unsupported)}")
    if not missing:
        return copy_parquet(cur, table, path, seed_columns) if path.suffix == ".parquet" else copy_csv(cur, table, path, seed_columns)

    stage = "northwind_legacy_seed"
    quoted_seed = ", ".join(f'"{column}"' for column in seed_columns)
    cur.execute(f"CREATE TEMP TABLE {stage} AS SELECT {quoted_seed} FROM {table} WHERE false")
    rows = copy_parquet(cur, stage, path, seed_columns) if path.suffix == ".parquet" else copy_csv(cur, stage, path, seed_columns)
    selected = ", ".join(
        f'"{column}"' if column in seed_columns else f'{expressions[column]} AS "{column}"'
        for column in target_columns
    )
    quoted_target = ", ".join(f'"{column}"' for column in target_columns)
    cur.execute(f"INSERT INTO {table} ({quoted_target}) SELECT {selected} FROM {stage}")
    cur.execute(f"DROP TABLE {stage}")
    return rows


def copy_table(cur, table: str, stem: str) -> int:
    path = find_seed(stem)
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq
        columns = pq.ParquetFile(path).schema_arrow.names
    else:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            columns = fh.readline().rstrip("\r\n").split(",")
    return _copy_legacy_seed(cur, table, path, columns)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", choices=("ops", "erp", "all"), default="all")
    ap.add_argument(
        "--migrate-only",
        action="store_true",
        help="apply pending schema migrations without loading the one-time seed",
    )
    ap.add_argument("--ops-dsn", default=OPS_DSN)
    ap.add_argument("--erp-dsn", default=ERP_DSN)
    args = ap.parse_args()

    t0 = time.time()
    selected = ("ops", "erp") if args.system == "all" else (args.system,)
    dsn_overrides = {"ops": args.ops_dsn, "erp": args.erp_dsn}
    for system in selected:
        _, loads = SYSTEMS[system]
        dsn = dsn_overrides[system]
        print(f"\n{system.upper()} target: {dsn.split('@')[-1]}")
        with psycopg.connect(dsn, autocommit=False) as conn:
            with conn.cursor() as cur:
                applied = apply_migrations(conn, system)
                if applied:
                    print("Applied migrations: " + ", ".join(migration.filename for migration in applied))
                else:
                    print("Schema is already current")
                if args.migrate_only:
                    conn.commit()
                    continue
                total = 0
                for table, stem in loads:
                    t = time.time()
                    n = copy_table(cur, table, stem)
                    total += n
                    print(f"  {table:<24} {n:>11,} rows   {time.time() - t:6.1f}s")
            conn.commit()
        print(f"  COMMIT succeeded. {total:,} {system} rows.")
    print(f"\nCompleted in {time.time() - t0:.1f}s.")
    print("CRM is served independently through the FakeForce REST API.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
