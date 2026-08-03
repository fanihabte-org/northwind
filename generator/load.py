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


def copy_csv(cur, table: str, path: Path) -> int:
    """Stream the file straight into COPY without materialising it."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        cols = fh.readline().rstrip("\r\n").split(",")
        n = 0
        with cur.copy(_copy_sql(table, cols)) as cp:
            while True:
                chunk = fh.read(1 << 22)
                if not chunk:
                    break
                n += chunk.count("\n")
                cp.write(chunk)
    return n


def copy_parquet(cur, table: str, path: Path) -> int:
    """Stream parquet row-group batches through COPY.

    Arrow writes nulls as empty fields, which is exactly what NULL '' expects,
    so a null Int64 stays null rather than becoming a zero or a string.
    """
    import pyarrow.parquet as pq
    from pyarrow import csv as pacsv

    pf = pq.ParquetFile(path)
    cols = pf.schema_arrow.names
    opts = pacsv.WriteOptions(include_header=False)
    n = 0
    with cur.copy(_copy_sql(table, cols)) as cp:
        for batch in pf.iter_batches(batch_size=BATCH):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, opts)
            cp.write(buf.getvalue())
            n += batch.num_rows
    return n


def copy_table(cur, table: str, stem: str) -> int:
    path = find_seed(stem)
    if path.suffix == ".parquet":
        return copy_parquet(cur, table, path)
    return copy_csv(cur, table, path)


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
