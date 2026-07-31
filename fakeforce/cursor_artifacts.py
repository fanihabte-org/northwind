"""Disk-backed ordered ID indexes for replayable REST query locators."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from fakeforce.engine import _quote_identifier, _quote_literal
from fakeforce.storage import publish_temp_file


class CursorArtifactStore:
    """Writes only result sequence and record IDs; never full REST pages."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write_index(
        self,
        connection: duckdb.DuckDBPyConnection,
        locator_id: str,
        result_sql: str,
        parameters: tuple[Any, ...],
        id_column: str,
    ) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        destination = self.root / f"{locator_id}.parquet"
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{locator_id}.", suffix=".tmp", dir=self.root
        )
        os.close(fd)
        temporary = Path(temporary_name)
        temporary.unlink()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT row_number() OVER () - 1 AS sequence_number,
                           {_quote_identifier(id_column)} AS record_id
                    FROM ({result_sql}) AS result
                ) TO {_quote_literal(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                list(parameters),
            )
            publish_temp_file(temporary, destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return destination

    @staticmethod
    def read_page_ids(
        connection: duckdb.DuckDBPyConnection, artifact_path: Path, offset: int, size: int
    ) -> list[str]:
        rows = connection.execute(
            """
            SELECT record_id
            FROM read_parquet(?)
            WHERE sequence_number >= ? AND sequence_number < ?
            ORDER BY sequence_number
            """,
            [str(artifact_path), offset, offset + size],
        ).fetchall()
        return [row[0] for row in rows]

    def remove(self, artifact_path: str | Path) -> bool:
        """Remove an artifact only when it is inside this store's safe root."""
        path = Path(artifact_path)
        try:
            path.resolve().relative_to(self.root.resolve())
        except ValueError:
            return False
        if not path.is_file():
            return False
        path.unlink()
        return True

    def reconcile(self, expired_paths: list[str], active_paths: set[str]) -> None:
        """Finish cleanup after an interrupted process or retention expiry."""
        self.root.mkdir(parents=True, exist_ok=True)
        for path in expired_paths:
            self.remove(path)
        active = {Path(path).resolve() for path in active_paths}
        for artifact in self.root.glob("*.parquet"):
            if artifact.resolve() not in active:
                self.remove(artifact)
        for temporary in self.root.glob(".*.tmp"):
            temporary.unlink(missing_ok=True)
