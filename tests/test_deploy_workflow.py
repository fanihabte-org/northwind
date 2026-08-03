from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_deploy_rebuilds_and_runs_migrations_before_restarting_services() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci-deploy.yml").read_text()

    build = 'docker compose --project-directory "$DEPLOY_DIR" build migrations fakeforce simulator'
    migrate = 'docker compose --project-directory "$DEPLOY_DIR" run --rm migrations'
    restart = 'docker compose --project-directory "$DEPLOY_DIR" up -d fakeforce simulator'
    assert build in workflow
    assert migrate in workflow
    assert restart in workflow
    assert workflow.index(build) < workflow.index(migrate) < workflow.index(restart)
