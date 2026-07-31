from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fakeforce.config import Settings
from fakeforce.bulk.jobs import BulkJob, BulkJobState, BulkJobType
from fakeforce.bulk.results import BulkQueryResultStore
from fakeforce.cursor_artifacts import CursorArtifactStore
from fakeforce.recovery import recover_bulk_artifacts, recover_cursor_artifacts
from fakeforce.state import StateStore


def _cursor(store, locator_id, artifact, expires_at) -> None:
    store.create_cursor(
        locator_id=locator_id,
        api_version="v60.0:query",
        normalized_query="SELECT Id FROM Account",
        query_hash="hash",
        object_name="Account",
        dataset_snapshot_id="snapshot",
        batch_size=2000,
        page_offset=0,
        total_size=2,
        result_artifact=str(artifact),
        expires_at=expires_at,
    )


def test_recovery_removes_expired_and_orphaned_artifacts_but_keeps_active_ones(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    state = StateStore.from_settings(settings)
    state.initialize()
    artifacts = CursorArtifactStore(tmp_path / "state" / "artifacts" / "cursors")
    artifacts.root.mkdir(parents=True)
    active = artifacts.root / "active.parquet"
    expired = artifacts.root / "expired.parquet"
    orphan = artifacts.root / "orphan.parquet"
    temporary = artifacts.root / ".incomplete.tmp"
    for path in (active, expired, orphan, temporary):
        path.write_bytes(b"test")
    _cursor(state, "active", active, datetime.now(timezone.utc) + timedelta(days=1))
    _cursor(state, "expired", expired, datetime.now(timezone.utc) - timedelta(seconds=1))

    recover_cursor_artifacts(state, artifacts)

    assert active.exists()
    assert not expired.exists()
    assert not orphan.exists()
    assert not temporary.exists()
    assert state.get_cursor("expired") is None
    assert state.get_cursor("active") is not None


def test_recovery_removes_only_expired_bulk_job_directories(tmp_path) -> None:
    settings = Settings.from_env({"FAKEFORCE_STATE_DIR": str(tmp_path / "state")})
    state = StateStore.from_settings(settings)
    state.initialize()
    state.create_job(
        BulkJob(
            "expired", BulkJobType.QUERY, "v60.0", BulkJobState.JOB_COMPLETE,
            None, "query", "SELECT Id FROM Account", "snapshot",
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
    )
    state.create_job(
        BulkJob(
            "active", BulkJobType.QUERY, "v60.0", BulkJobState.JOB_COMPLETE,
            None, "query", "SELECT Id FROM Account", "snapshot",
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
    )
    root = settings.state_directory / "bulk" / "query"
    (root / "expired").mkdir(parents=True)
    (root / "active").mkdir(parents=True)

    recover_bulk_artifacts(state, BulkQueryResultStore(root))

    assert not (root / "expired").exists()
    assert (root / "active").exists()
