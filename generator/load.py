"""
Load the generated datasets into Postgres.

Applies sql/01_sources.sql (which enforces every constraint) and then COPYs each
file in. If any primary key, foreign key, unique or check constraint is violated
the transaction aborts and nothing is committed -- so a successful run is proof
the sources are internally valid.

    python generator/load.py                        # ops + erp
    python generator/load.py --with-crm             # also load crm.* for SQL joins
    PGDSN='postgresql://...' python generator/load.py

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

try:
    import psycopg
except ImportError:
    sys.exit("pip install 'psycopg[binary]' first")

ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "seed"

DSN = os.getenv("PGDSN", "postgresql://northwind:northwind@localhost:5433/northwind")

CORE = [
    ("erp.companies",         "erp_companies"),
    ("erp.cost_centers",      "erp_cost_centers"),
    ("erp.gl_accounts",       "erp_gl_accounts"),
    ("erp.fx_rates",          "erp_fx_rates"),
    ("ops.customers",         "ops_customers"),
    ("ops.products",          "ops_products"),
    ("ops.warehouses",        "ops_warehouses"),
    ("ops.carriers",          "ops_carriers"),
    ("ops.orders",            "ops_orders"),
    ("ops.order_lines",       "ops_order_lines"),
    ("ops.shipments",         "ops_shipments"),
    ("ops.support_cases",     "ops_support_cases"),
    ("erp.revenue_postings",  "erp_revenue_postings"),
]
CRM = [
    ("crm.sales_reps",    "crm_sales_reps"),
    ("crm.campaigns",     "crm_campaigns"),
    ("crm.accounts",      "crm_accounts"),
    ("crm.opportunities", "crm_opportunities"),
]


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
    ap.add_argument("--with-crm", action="store_true",
                    help="also load crm.accounts / crm.opportunities into Postgres")
    ap.add_argument("--dsn", default=DSN)
    args = ap.parse_args()

    loads = CORE + (CRM if args.with_crm else [])
    print(f"Target: {args.dsn.split('@')[-1]}")
    t0 = time.time()

    with psycopg.connect(args.dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            print("\nApplying sql/01_sources.sql (all constraints enabled)")
            cur.execute((ROOT / "sql" / "01_sources.sql").read_text())
            print("\nLoading:")
            total = 0
            for table, stem in loads:
                t = time.time()
                n = copy_table(cur, table, stem)
                total += n
                print(f"  {table:<24} {n:>11,} rows   {time.time() - t:6.1f}s")
        conn.commit()

    print(f"\n  COMMIT succeeded. {total:,} rows in {time.time() - t0:.1f}s.")
    print("  Every primary key, foreign key, unique and check constraint holds.")
    print(f"  Source: {find_seed('ops_orders').name}")
    if not args.with_crm:
        print("\n  crm.* not loaded. Read the CRM over the FakeForce REST API,")
        print("  or re-run with --with-crm to query it in SQL instead.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
