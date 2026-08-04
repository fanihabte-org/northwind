from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_read_only_performance_workflow_is_manual_and_does_not_deploy() -> None:
    workflow = (ROOT / ".github" / "workflows" / "read-only-performance.yml").read_text()

    assert "workflow_dispatch:" in workflow
    assert "generator/generate.py --scale" in workflow
    assert "python -m generator.profile" in workflow
    assert "python scripts/read_only_performance_probe.py" in workflow
    assert "Deploy Northwind" not in workflow
