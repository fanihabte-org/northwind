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


def test_deploy_targets_the_linux_production_runner() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci-deploy.yml").read_text()

    assert "runs-on: [self-hosted, Linux, X64]" in workflow


def test_deploy_preconditions_explain_why_they_failed() -> None:
    """Silent preconditions under `set -e` report only "exit code 1".

    Every check in this step exits 1 without printing: test -d, both
    diff --quiet, and cat-file -e. A deploy that stopped on any of them was
    indistinguishable from a deploy that stopped on any other.
    """
    workflow = (ROOT / ".github" / "workflows" / "ci-deploy.yml").read_text()

    assert 'fail() { echo "::error::deploy precondition failed: $*"; exit 1; }' in workflow
    for reason in (
        "is not a git checkout",
        "has uncommitted changes",
        "has staged changes",
        "could not fetch from the deployment checkout's origin",
        "not the same repository",
    ):
        assert reason in workflow, f"no diagnostic for: {reason}"


def test_deploy_reports_both_repositories_when_the_commit_is_missing() -> None:
    """The mismatch this surfaces is the runner tracking a different repo."""
    workflow = (ROOT / ".github" / "workflows" / "ci-deploy.yml").read_text()

    assert 'git -C "$DEPLOY_DIR" remote get-url origin' in workflow
    assert "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" in workflow
