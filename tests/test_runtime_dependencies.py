from __future__ import annotations

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]


def test_duckdb_is_available_to_the_runtime() -> None:
    assert duckdb.sql("SELECT 40 + 2").fetchone() == (42,)


def test_container_installs_duckdb() -> None:
    dockerfile = (ROOT / "fakeforce" / "Dockerfile").read_text()
    assert " duckdb" in dockerfile


def test_container_copies_bulk_subpackage_for_non_development_runs() -> None:
    dockerfile = (ROOT / "fakeforce" / "Dockerfile").read_text()
    assert "COPY . /app/fakeforce/" in dockerfile
