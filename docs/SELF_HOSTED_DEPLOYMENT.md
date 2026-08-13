# Self-hosted deployment

Keep the deployment checkout, generated datasets, and service state separate.
Clone this repository to a user-owned directory such as
`~/project/northwind`; do not place data or runtime state in that checkout.

Create the machine-owned data and state directories, then configure their absolute
host paths:

```bash
mkdir -p /srv/data/northwind/seed
mkdir -p /srv/data/northwind/exports
mkdir -p /srv/volumes/northwind/state
cp .env.example .env
# Edit .env: set all three /srv paths and SIMULATION_BASELINE_DATE.
```

Compose mounts the seed directory read-only at `/app/seed`, durable service state
at `/app/state`, and exports at `/app/exports`. FakeForce writes only its durable
Bulk Query CSV parts and manifest to the export directory; the repository remains
safe to update with Git.

## Daily simulator

The `simulator` Compose service starts by catching up from its durable watermark,
then runs at each midnight in `America/Los_Angeles`. It executes CRM, then Ops,
then ERP for each missing date. It shares `state/simulation/` with FakeForce so CRM
Parquet deltas become visible through the API without copying them into the Git
checkout.

Its DuckDB CRM snapshot connection is independently bounded: by default it uses
at most `512MB`, one execution thread, and `state/simulator-spill/` for up to `20GB`
of temporary spill data. Set `SIMULATION_DUCKDB_MEMORY_LIMIT`,
`SIMULATION_DUCKDB_MAX_TEMP_SIZE`, or `SIMULATION_DUCKDB_THREADS` in `.env` only
when the Docker Desktop memory and disk budget permits it.

Set `SIMULATION_BASELINE_DATE` in `.env` to the day immediately before the first
incremental business date. This value is recorded on first start and cannot safely
be changed afterwards. Check the service with:

```bash
docker compose logs -f simulator
```

## Generated seed data

`seed/` is intentionally ignored by Git: it can contain millions of records and
must never be pushed with the application source. The test workflow creates a small,
deterministic Parquet seed for its ephemeral runner before it runs the tests:

```bash
python generator/generate.py --scale 0.05 --format parquet --seed 20260728
```

Generate the target machine's data once, after cloning the repository and before its
first `docker compose up`. Use the scale appropriate for that environment (the
default `1.0` is the full data set):

```bash
cd ~/project/northwind
python generator/generate.py --scale 1.0 --format parquet --seed 20260728
mv seed/* /srv/data/northwind/seed/
```

This writes the files required by the default `Account` and `Opportunity` catalog.
Move them to the configured seed directory after generation. Deployment updates do
not touch that directory. To use a different dataset, keep the configured Parquet/CSV
files under the catalog's approved data root and update `fakeforce/catalog.json`
rather than hard-code object-specific file paths in the API.

Start Compose after generating the seed. The `migrations` service applies
non-destructive, checksummed Ops, ERP, and analytics-registry migrations before the
simulator starts. It uses the warehouse database named `rev_engine_pipeline`; on a
pre-existing analytics Docker volume, it creates that database only if it is missing.
After the Ops and ERP services are healthy, load their baseline once:

```bash
docker compose up -d --build
python generator/load.py
```

For a later schema-only upgrade, run `python generator/migrate.py`; it records
the applied migration checksum in each source/analytics database and never reloads
or deletes business rows.

## Source status and contract registry

Use the migration image for low-cost operational snapshots; it reads catalog metadata
and durable checkpoints rather than scanning large business tables:

```bash
docker compose run --rm migrations python -m generator.source_status
```

The central contract registry is stored in
`rev_engine_pipeline.metadata.table_versions` and
`rev_engine_pipeline.metadata.column_versions`. Populate or refresh it after a
source-schema change:

```bash
docker compose run --rm migrations python -m generator.metadata_registry
```

This registry sync reads only the Ops and ERP information schemas. It does not load
source records into analytics and creates a new version only when a column contract
changes.

## Audit-field maintenance

Historical audit fields are backfilled explicitly and durably; deployments do not
silently perform the large historical updates. Check progress or resume work in
bounded batches:

```bash
docker compose run --rm migrations python -m generator.audit_backfill --system ops --status
docker compose run --rm migrations python -m generator.audit_backfill --system erp --status
```

Use `--batch-size 100000 --until-complete` only when the target has sufficient
maintenance capacity. The ERP revenue ledger is append-only: updates/deletes are
rejected and financial corrections are new `CRN` or `ADJ` entries.

The simulator service waits for both database baselines and the CRM Parquet files
before it initializes or advances simulation state. The `ops.invoices` table can
be empty because the daily simulator creates invoices. This prevents a clean
deployment from failing while the one-time load is still in progress.

## GitHub Actions deployment

The workflow at `.github/workflows/ci-deploy.yml` runs tests for pull requests and
pushes to `main`. It deploys only successful pushes to `main`, using a self-hosted
Linux x64 runner labelled `self-hosted`, `Linux`, and `X64`.

Before enabling the deploy job:

1. Initialize this directory as a Git repository and push it to GitHub.
2. Install a GitHub Actions self-hosted runner on the target Linux server with the
   labels `self-hosted`, `Linux`, and `X64`, and ensure its service account can run
   `docker`.
3. Make a clean deployment clone at the value below. The workflow refuses to deploy
   if that checkout has uncommitted changes.
4. Generate the seed data in that clone once, using the command above. It is local to
   the target machine and is not part of the Git repository.
5. Add GitHub repository variables:

   - `NORTHWIND_DEPLOY_DIR` — for example `/home/your-user/projects/northwind`
   - `NORTHWIND_EXPORTS_DIR` — for example `/srv/data/northwind/exports`

The runner fetches the exact tested commit, rebuilds the migration, `fakeforce`,
and simulator images, and runs the one-shot migration service before restarting
the application services. This means a new versioned database migration is applied
on the target machine rather than being skipped because an older migration container
already exited successfully. The runner then waits for the API health endpoint. It
does not run deployment code for pull requests.

The source-integration workflow also verifies live ERP ledger behavior: a posted
entry rejects `UPDATE` and `DELETE`, while a corrective credit-note-shaped `CRN`
insert is accepted and rolled back. This protects the append-only accounting rule
from regressions in either a migration or the runtime schema.

`.env`, `seed/`, `state/`, exports, virtual environments, and macOS `.DS_Store`
metadata are all ignored by Git. The deploy job therefore continues to reject real
source changes in its checkout without being blocked by local runtime artifacts.
