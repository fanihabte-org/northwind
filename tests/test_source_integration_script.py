from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_source_integration_exercises_live_append_only_ledger_guards() -> None:
    script = (ROOT / "scripts" / "ci-source-integration.sh").read_text()

    assert "UPDATE erp.revenue_postings" in script
    assert "DELETE FROM erp.revenue_postings" in script
    assert "SQLSTATE '55000'" in script
    assert "'CRN'" in script
    assert "ROLLBACK;" in script
    assert '"$PYTHON_BIN" generator/load.py' in script
    assert "NORTHWIND_PYTHON_BIN" in script
