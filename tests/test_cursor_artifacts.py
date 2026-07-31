from __future__ import annotations

from fakeforce.cursor_artifacts import CursorArtifactStore


def test_cursor_artifact_stores_ordered_ids_and_reads_one_page(tmp_path) -> None:
    import duckdb

    store = CursorArtifactStore(tmp_path / "artifacts" / "cursors")
    conn = duckdb.connect()
    try:
        conn.execute("CREATE TABLE source_records (Id VARCHAR, Name VARCHAR)")
        conn.execute(
            "INSERT INTO source_records VALUES ('003', 'C'), ('001', 'A'), ('002', 'B')"
        )
        artifact = store.write_index(
            conn,
            "locator-1",
            'SELECT "Id" FROM source_records ORDER BY "Id"',
            (),
            "Id",
        )

        assert artifact.is_file()
        assert store.read_page_ids(conn, artifact, offset=1, size=2) == ["002", "003"]
    finally:
        conn.close()
