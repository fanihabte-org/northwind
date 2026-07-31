"""Smoke checks for the test-suite contract.

These checks deliberately avoid importing the API.  Importing it currently loads
the complete CRM dataset, which FF-004 will remove.  The test scaffold must stay
fast enough to run before the runtime is available.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_project_has_the_expected_runtime_entrypoint() -> None:
    assert (ROOT / "fakeforce" / "app.py").is_file()
    assert (ROOT / "requirements.txt").is_file()
