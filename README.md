# Northwind Systems

A synthetic B2B business with three source systems, generated at whatever scale you
want. Built to be worked on: extract it, model it, analyse it, forecast it.

| Source | Stands in for | Access |
|---|---|---|
| **FakeForce** `localhost:8080` | Salesforce | REST + OAuth + SOQL, with pagination, rate limits, a recycle bin and controllable failures |
| **`ops`** `localhost:5433` | SQL Server order management | Separate Postgres database — customers, catalogue, orders, fulfilment, support |
| **`erp`** `localhost:5434` | ERP finance | Separate Postgres database — entities, cost centres, GL, FX, the revenue ledger |
| **`analytics`** `localhost:5435` | Redshift | An **empty** Postgres. Yours to design. |

Read **[`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md)** first — it documents
every table, column, relationship and metric definition.

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env                         # set an absolute exports directory
python generator/generate.py --format parquet # creates local, ignored seed data

docker compose up -d --build          # applies versioned source migrations
python generator/load.py              # loads separate Ops and ERP databases
```

On a new Docker deployment, the simulator waits safely for the CRM Parquet seed
and the required Ops/ERP baseline reference and transaction tables to contain
rows. The empty `ops.invoices` table is valid: invoices start being created by
the daily simulator. It logs missing prerequisites every 60 seconds (configurable with
`SIMULATION_BOOTSTRAP_RETRY_SECONDS`) instead of crashing. Run
`generator/load.py` once; do not rerun it on an initialized database.

### Daily source increments

Run the daily simulator once from the host after the source databases and seed
files are available. It processes every missing business date in order, so it is
safe to schedule at midnight and safe to rerun after downtime:

```bash
SIMULATION_BASELINE_DATE=2026-07-24 python -m simulator.scheduler
```

Use the date immediately before the first incremental business date as the
baseline. The first run records it in `state/simulation/simulation.duckdb`; later
runs reject a different baseline while retaining a separate, advancing completion
watermark for restart and catch-up. Set `SIMULATION_SEED`, `OPS_PG_DSN`,
`ERP_PG_DSN`, `FAKEFORCE_STATE_DIR`, and `FAKEFORCE_SEED_DIR` when the defaults do
not fit your deployment. `--through YYYY-MM-DD` is useful for controlled catch-up
and tests.

The Compose simulator runs its catch-up once when it starts, then sleeps until the
next midnight in `America/Los_Angeles`; it does not poll every second. Its CRM
snapshot DuckDB connection is limited to `SIMULATION_DUCKDB_MEMORY_LIMIT` (512MB by
default), uses one worker, and spills intermediates to `state/simulator-spill` up to
`SIMULATION_DUCKDB_MAX_TEMP_SIZE` (20GB by default).

If an individual daily run fails—for example while a source database is briefly
unavailable—the daemon logs the error and retries in 60 seconds, then backs off up
to one hour. Configure `SIMULATION_RETRY_INITIAL_SECONDS` and
`SIMULATION_RETRY_MAX_SECONDS` in `.env` if needed; completed days are never rerun.

For a self-hosted deployment, configure `FAKEFORCE_EXPORTS_DIR` in `.env` before
starting Compose. Bulk Query CSV artifacts will be stored there rather than in the
repository. See [`docs/SELF_HOSTED_DEPLOYMENT.md`](docs/SELF_HOSTED_DEPLOYMENT.md).

Smoke test:

```bash
TOKEN=$(curl -s -X POST localhost:8080/services/oauth2/token \
  -d grant_type=client_credentials -d client_id=demo -d client_secret=demo \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

curl -s -H "Authorization: Bearer $TOKEN" \
  --get localhost:8080/services/data/v60.0/query \
  --data-urlencode "q=SELECT Id, Name, AccountNumber FROM Account LIMIT 3"
```

---

## Scale

`--scale 1` is the default. Facts scale sub-linearly with it, so doubling the flag
does not double the rows.

| `--scale` | Total rows | Time (Apple silicon, parquet) |
|---|---|---|
| `0.06` | ~2.0 M | 10 s — useful local smoke-test size |
| `0.3` | ~5.5 M | 25 s |
| `1` | ~11 M | 50 s |
| `3` | ~23 M | 2 min |
| `8` | ~42 M | 3–4 min |

`csv.gz` output is roughly 4x slower to write than parquet because gzip is
single-threaded; the generation itself is the same speed either way.

```bash
python generator/generate.py --scale 3
python generator/generate.py --scale 1 --format parquet   # ~4x smaller, much faster
python generator/generate.py --scale 1 --seed 42          # a different world
```

The generator is deterministic: the same `--seed` and `--scale` reproduce the same
dataset byte for byte, so your tests and models stay comparable across runs.

`generator/load.py` reads whichever format you generated. Parquet is about half the
size, several times faster to write and read, and preserves types and nulls exactly —
prefer it unless you specifically want to eyeball raw text.

For warehouse planning, run `python -m generator.profile` after generation. It writes
an ignored `seed/_profile.json` with disk size, row counts, null counts, approximate
cardinalities, and date-partition recommendations using bounded DuckDB scans.

---

## What is here

```
generator/generate.py     the business simulation -- read it, or don't
generator/migrate.py      applies checksummed, versioned source schema migrations
generator/load.py         runs pending migrations, then COPYs the one-time seed data in
sql/migrations/ops/       Ops migrations: locally owned PK / FK / UNIQUE / CHECK constraints
sql/migrations/erp/       ERP migrations: locally owned PK / FK / UNIQUE / CHECK constraints
fakeforce/app.py          Salesforce-shaped REST API over catalogued CRM files
fakeforce/catalog.json    default object-to-file catalogue (not hard-coded in the API)
docs/DATA_DICTIONARY.md   every table, column, relationship and metric
docker-compose.yml        two Postgres instances and the API
seed/                     locally generated data (ignored by Git)
```

There is no warehouse schema, no bronze/silver/gold or dbt project. The FakeForce
runtime has a focused pytest suite; run it with `python -m pytest` after installing
the requirements. The GitHub Actions workflow creates its own small deterministic
Parquet seed before testing; production seed data and Bulk Query exports are never
committed.

---

## The sources are valid databases

Each source owns an independent, checksummed migration ledger in
`simulation.schema_migrations`. The Compose `migrations` service runs pending
migrations before the simulator starts; it never drops schemas or truncates data.
You can also run `python generator/migrate.py` or
`python generator/load.py --migrate-only` from the host. The one-time loader then
loads each system in a transaction — if a local constraint is violated, none of
that system's rows commit.

`sql/01_sources.sql` is retained only as the original single-database reference
script. Do not run it against the split source databases: it drops schemas and
does not represent the deployed ownership model.

That matters because it means the data is not sabotaged. It is a working business,
recorded correctly by three systems that were never designed to talk to each other.
Whatever you find difficult about integrating it will be difficult for the same
reasons it is difficult at work: two systems cannot share a foreign key, a market
convention is not what you assumed, master data changes on a different schedule than
transactions, two extracts see different moments in time.

The dictionary documents what every column means. It does not tell you what will bite
you. Finding that out is the exercise.

---

## The API

FakeForce imitates the shape of the Salesforce REST API and lets you break it on
purpose — which the real thing will not do for you.

```
POST /services/oauth2/token                       client_credentials
GET  /services/data/v60.0/query?q=<SOQL>          excludes soft-deleted records
GET  /services/data/v60.0/queryAll?q=<SOQL>       includes them
GET  /services/data/v60.0/query/{locator}         cursor paging via nextRecordsUrl
GET  /services/data/v60.0/sobjects/{obj}/describe
GET  /services/data/v60.0/sobjects/{obj}/deleted
GET  /services/data/v60.0/limits
POST /services/data/v60.0/jobs/query
GET  /services/data/v60.0/jobs/query/{id}/results
POST /services/data/v60.0/jobs/ingest
PUT  /services/data/v60.0/jobs/ingest/{id}/batches
PATCH /services/data/v60.0/jobs/ingest/{id}
GET  /services/data/v60.0/jobs/ingest/{id}/successfulResults
GET  /services/data/v60.0/jobs/ingest/{id}/failedResults
```

SOQL support: `SELECT … FROM … [WHERE …] [ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]]
[LIMIT n] [OFFSET n]`. `WHERE` takes AND-joined comparisons plus `LIKE` and `IN`.
No `OR`, no aggregates, no subqueries.

`OFFSET` is capped at 2000 and returns `NUMBER_OUTSIDE_VALID_RANGE` above it, exactly
as Salesforce does — which is why offset paging cannot be your extraction strategy.
Use the cursor, or keyset on `LastModifiedDate`.

### Storage, memory and restart behavior

FakeForce never eagerly reads an entire CRM file into Python memory. Object-to-file
mapping comes from `fakeforce/catalog.json`; a deployment can point
`FAKEFORCE_CATALOG_PATH` and `FAKEFORCE_DATA_ROOTS` at any registered Parquet, CSV,
or CSV.GZ file below its approved data roots. DuckDB scans those files lazily and is
configured with a memory limit, a spill directory, and a maximum spill size.

REST cursor locators are small DuckDB records plus a disk-backed Parquet ID index.
Bulk Query writes durable CSV parts and a manifest; its result locator advances by
part. Both are replayable across a process restart until their retention expires.
The operational DuckDB state store also holds API use, jobs, checkpoints, and Ingest
row results. Startup removes expired cursor and job artifacts and resumes incomplete
Bulk jobs through the bounded worker queue.

Useful resource settings are `FAKEFORCE_MEMORY_LIMIT`, `FAKEFORCE_TEMP_DIRECTORY`,
`FAKEFORCE_MAX_TEMP_SIZE`, `FAKEFORCE_DISK_RESERVE_BYTES`,
`FAKEFORCE_HEAVY_QUERY_WORKERS`, and `FAKEFORCE_BULK_WORKERS`. The internal
`/_diagnostics` snapshot combines memory, spill/export/state disk capacity,
admission queues, open cursors, durable job/checkpoint state, and rolling API
use. Its component snapshots remain available at `/_diagnostics/memory`,
`/_diagnostics/queries`, and `/_diagnostics/jobs`.

### Bulk API 2.0

Bulk Query accepts `operation=query`, validates the SOQL, and produces durable CSV
parts asynchronously. Poll the job until `JobComplete`, then read `/results` and
follow `Sforce-Locator` while it is present.

Bulk Ingest currently supports CSV `insert` only. An object must be explicitly marked
`"mode": "mutable"` in the catalog; its seed file is copied disk-to-disk into the
state DuckDB database on startup. Upload CSV while the job is `Open`, patch it to
`UploadComplete`, then poll until terminal. The worker applies bounded batches and
persists the inserted/failed outcome and checkpoint in the same DuckDB transaction.
`update`, `upsert`, `delete`, and `hardDelete` are not implemented yet and are
rejected rather than silently behaving incorrectly.

Extracting several million records over HTTP also takes a lot of round trips. At the
default `page_size` of 2000 and `rate_limit_per_min` of 120, 8.8M opportunities is
4,400 requests and over half an hour of 429 backoff. Raise both for bulk work:

```bash
curl -s -X POST localhost:8080/_chaos -H 'content-type: application/json' \
  -d '{"page_size": 50000, "rate_limit_per_min": 100000}'
```

`rate_limit_per_min` is a FakeForce safety throttle shared by every
`/services/data/v60.0` request, including Bulk Query submission, polling, and
CSV-result locator requests. On a `429 REQUEST_LIMIT_EXCEEDED`, wait for the
number of seconds in `Retry-After` before retrying the same request or locator.

Use `python -m simulator.status --state-directory state` to inspect the durable
simulation baseline, completion watermark, and any incomplete daily run.

Ops audit-field backfill is deliberately separate from deploy and the daily
simulator. After migration `003_add_audit_backfill_state.sql` is applied, inspect
progress with `python -m generator.audit_backfill --status`. Run a bounded unit
of work with `python -m generator.audit_backfill --batch-size 10000 --max-batches 1`.
It records a checkpoint per table and resumes after interruption; use
`--until-complete` only when the database has enough maintenance capacity.
After every target reports completed and null/order validation returns zero,
migration `004_enforce_audit_metadata.sql` makes the audit fields non-null and
rejects `updated_at < created_at` for all future Ops writes.

### Breaking it on purpose

```bash
curl -s localhost:8080/_chaos                       # current settings
curl -s -X POST localhost:8080/_chaos/reset         # behave
curl -s -X POST localhost:8080/_chaos/outage -d '{"seconds": 120}'

curl -s -X POST localhost:8080/_chaos -H 'content-type: application/json' -d '{
  "error_rate":         0.30,        # retryable 503s
  "bad_request_rate":   0.05,        # non-retryable 400s
  "latency_ms":         500,
  "hang_rate":          0.02,        # 30s stall, to exercise client timeouts
  "rate_limit_per_min": 30,          # 429 REQUEST_LIMIT_EXCEEDED + Retry-After
  "page_size":          200,         # force cursor pagination
  "drift_after":        "2026-01-15" # Accounts gain a Territory__c field
}'
```

Every knob has an environment variable (`FAKEFORCE_ERROR_RATE`, `FAKEFORCE_PAGE_SIZE`,
…) so a container can start already broken for a scheduled drill.

---

## Working in Redshift instead

`sql/01_sources.sql` is standard enough to port. The differences that matter:
`BIGSERIAL` becomes `BIGINT IDENTITY(1,1)`, indexes become `DISTKEY` / `SORTKEY`,
and `COPY … FROM STDIN` becomes `COPY … FROM 's3://…' IAM_ROLE …`. Redshift does not
enforce primary or foreign keys — it accepts the declarations and uses them for query
planning only — so if you load there, the constraint guarantees described above no
longer hold and validating them becomes your job.
