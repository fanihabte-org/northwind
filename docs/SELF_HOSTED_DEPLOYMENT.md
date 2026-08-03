# Self-hosted deployment

Keep the deployment checkout and generated artifacts separate. On the target Mac,
clone this repository to a stable directory such as
`/Users/your-user/northwind`. Do not place Bulk Query results inside that
checkout.

Create the durable export directory and configure its absolute host path:

```bash
mkdir -p /Users/your-user/data_forge/exports
cp .env.example .env
# Edit .env: set FAKEFORCE_EXPORTS_DIR and SIMULATION_BASELINE_DATE.
```

Compose mounts this directory at `/app/exports`. FakeForce writes only its durable
Bulk Query CSV parts and manifest there; generated source data remains read-only and
the repository remains safe to update with Git.

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
cd /Users/your-user/northwind
python generator/generate.py --scale 1.0 --format parquet --seed 20260728
```

This writes the files required by the default `Account` and `Opportunity` catalog to
the local `seed/` directory. Deployment updates leave those untracked files in place;
do not run `git clean -fdx` in the deployment checkout unless you intend to regenerate
them. To use a different dataset, keep the configured Parquet/CSV files under the
catalog's approved data root and update `fakeforce/catalog.json` rather than hard-code
object-specific file paths in the API.

Start Compose after generating the seed. The `migrations` service applies
non-destructive, checksummed Ops and ERP schema migrations before the simulator
starts. After the Ops and ERP services are healthy, load their baseline once:

```bash
docker compose up -d --build
python generator/load.py
```

For a later schema-only upgrade, run `python generator/migrate.py`; it records
the applied migration checksum in each source database and never reloads or
deletes business rows.

The simulator service waits for both database baselines and the CRM Parquet files
before it initializes or advances simulation state. The `ops.invoices` table can
be empty because the daily simulator creates invoices. This prevents a clean
deployment from failing while the one-time load is still in progress.

## GitHub Actions deployment

The workflow at `.github/workflows/ci-deploy.yml` runs tests for pull requests and
pushes to `main`. It deploys only successful pushes to `main`, using a self-hosted
runner labelled `self-hosted`, `macOS`, and `X64` on the target Mac.

Before enabling the deploy job:

1. Initialize this directory as a Git repository and push it to GitHub.
2. Install a GitHub Actions self-hosted runner on the target Mac with the labels
   `self-hosted`, `macOS`, and `X64`, and ensure its service account can run `docker`.
3. Make a clean deployment clone at the value below. The workflow refuses to deploy
   if that checkout has uncommitted changes.
4. Generate the seed data in that clone once, using the command above. It is local to
   the target machine and is not part of the Git repository.
5. Add GitHub repository variables:

   - `NORTHWIND_DEPLOY_DIR` — for example `/Users/your-user/northwind`
   - `NORTHWIND_EXPORTS_DIR` — for example `/Users/your-user/data_forge/exports`

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
