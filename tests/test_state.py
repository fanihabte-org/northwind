from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fakeforce.config import Settings
from fakeforce.state import SCHEMA_VERSION, StateStore


def test_state_store_creates_durable_schema(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)

    store.initialize()
    store.initialize()

    assert store.database_path.is_file()
    with store.connection() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        migrations = conn.execute(
            "SELECT version FROM schema_migrations"
        ).fetchall()

    assert {
        "schema_migrations",
        "dataset_snapshots",
        "cursors",
        "jobs",
        "job_checkpoints",
        "artifacts",
        "api_usage_events",
    }.issubset(tables)
    assert migrations == [(SCHEMA_VERSION,)]


def test_state_store_registers_a_dataset_snapshot_once(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)
    store.initialize()

    store.register_snapshot("snapshot-1", "catalog-hash")
    store.register_snapshot("snapshot-1", "catalog-hash")

    with store.connection() as conn:
        rows = conn.execute("SELECT snapshot_id, catalog_hash FROM dataset_snapshots").fetchall()
    assert rows == [("snapshot-1", "catalog-hash")]


def test_state_store_persists_and_reads_open_cursor_metadata(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)
    store.initialize()

    store.create_cursor(
        locator_id="locator-1",
        api_version="v60.0:query",
        normalized_query="SELECT Id FROM Account",
        query_hash="query-hash",
        object_name="Account",
        dataset_snapshot_id="snapshot-1",
        batch_size=2000,
        page_offset=2000,
        total_size=5000,
        result_artifact="/tmp/cursor.parquet",
        expires_at=datetime.now(timezone.utc) + timedelta(days=2),
    )

    cursor = store.get_cursor("locator-1")

    assert cursor is not None
    assert cursor.normalized_query == "SELECT Id FROM Account"
    assert cursor.page_offset == 2000
    assert cursor.result_artifact == "/tmp/cursor.parquet"
    assert store.count_open_cursors() == 1


def test_state_store_tracks_api_usage_in_the_rolling_24_hour_window(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)
    store.initialize()

    assert store.record_api_usage("/query") == 1
    assert store.record_api_usage("/limits") == 2
    assert store.api_usage_last_24_hours() == 2


def test_state_store_expires_jobs_and_their_checkpoints(tmp_path) -> None:
    from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType

    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    store = StateStore.from_settings(settings)
    store.initialize()
    store.create_job(
        BulkJob(
            "expired-job", BulkJobType.QUERY, "v60.0", BulkJobState.JOB_COMPLETE,
            None, "query", "SELECT Id FROM Account", "snapshot-1",
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
    )
    store.add_job_checkpoint("expired-job", 1, 12, "/tmp/part.csv")

    assert store.expire_jobs() == ["expired-job"]
    assert store.get_job("expired-job") is None
    with store.connection() as conn:
        assert conn.execute("SELECT count(*) FROM job_checkpoints").fetchone()[0] == 0
