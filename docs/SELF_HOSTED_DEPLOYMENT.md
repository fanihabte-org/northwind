# Self-hosted deployment

Keep the deployment checkout and generated artifacts separate. On the target Mac,
clone this repository to a stable directory such as
`/Users/your-user/data_forge/northwind`. Do not place Bulk Query results inside that
checkout.

Create the durable export directory and configure its absolute host path:

```bash
mkdir -p /Users/your-user/data_forge/exports
cp .env.example .env
# Edit .env: set FAKEFORCE_EXPORTS_DIR and SIMULATION_BASELINE_DATE.
docker compose up -d --build
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

Set `SIMULATION_BASELINE_DATE` in `.env` to the day immediately before the first
incremental business date. This value is recorded on first start and cannot safely
be changed afterwards. Check the service with:

```bash
docker compose logs -f simulator
docker compose exec simulator python -m simulator.daemon --once
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
cd /Users/your-user/data_forge/northwind
python generator/generate.py --scale 1.0 --format parquet --seed 20260728
```

This writes the files required by the default `Account` and `Opportunity` catalog to
the local `seed/` directory. Deployment updates leave those untracked files in place;
do not run `git clean -fdx` in the deployment checkout unless you intend to regenerate
them. To use a different dataset, keep the configured Parquet/CSV files under the
catalog's approved data root and update `fakeforce/catalog.json` rather than hard-code
object-specific file paths in the API.

## GitHub Actions deployment

The workflow at `.github/workflows/ci-deploy.yml` runs tests for pull requests and
pushes to `main`. It deploys only successful pushes to `main`, using a self-hosted
runner labelled `dataforge-deploy` on the target Mac.

Before enabling the deploy job:

1. Initialize this directory as a Git repository and push it to GitHub.
2. Install a GitHub Actions self-hosted runner on the target Mac, give it the
   `dataforge-deploy` label, and ensure its service account can run `docker`.
3. Make a clean deployment clone at the value below. The workflow refuses to deploy
   if that checkout has uncommitted changes.
4. Generate the seed data in that clone once, using the command above. It is local to
   the target machine and is not part of the Git repository.
5. Add GitHub repository variables:

   - `DATAFORGE_DEPLOY_DIR` — for example `/Users/your-user/data_forge/northwind`
   - `DATAFORGE_EXPORTS_DIR` — for example `/Users/your-user/data_forge/exports`

The runner fetches the exact tested commit, updates the clean deployment checkout,
rebuilds the `fakeforce` and `simulator` services, and waits for the API health
endpoint. It does not run deployment code for pull requests.
