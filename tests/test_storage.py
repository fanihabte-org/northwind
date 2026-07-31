from __future__ import annotations

from collections import namedtuple

import pytest

from fakeforce.storage import (
    InsufficientDiskSpaceError,
    atomic_binary_writer,
    publish_temp_file,
    require_disk_reserve,
)


def test_atomic_writer_publishes_only_after_stream_is_closed(tmp_path) -> None:
    destination = tmp_path / "artifacts" / "page.csv"

    with atomic_binary_writer(destination) as stream:
        stream.write(b"Id,Name\n001,Example\n")
        assert not destination.exists()

    assert destination.read_bytes() == b"Id,Name\n001,Example\n"
    assert not list(destination.parent.glob("*.tmp"))


def test_atomic_writer_keeps_previous_artifact_after_failure(tmp_path) -> None:
    destination = tmp_path / "page.csv"
    destination.write_bytes(b"previous")

    with pytest.raises(RuntimeError, match="simulated failure"):
        with atomic_binary_writer(destination) as stream:
            stream.write(b"incomplete")
            raise RuntimeError("simulated failure")

    assert destination.read_bytes() == b"previous"
    assert not list(destination.parent.glob("*.tmp"))


def test_disk_reserve_rejects_unsafe_write(monkeypatch, tmp_path) -> None:
    usage = namedtuple("usage", "total used free")(100, 90, 10)
    monkeypatch.setattr("fakeforce.storage.shutil.disk_usage", lambda _: usage)

    with pytest.raises(InsufficientDiskSpaceError, match="insufficient free disk space"):
        require_disk_reserve(tmp_path, reserve_bytes=8, additional_bytes=3)


def test_publish_temp_file_replaces_destination_atomically(tmp_path) -> None:
    temporary = tmp_path / "artifact.tmp"
    destination = tmp_path / "artifact.csv"
    temporary.write_bytes(b"new")
    destination.write_bytes(b"old")

    publish_temp_file(temporary, destination)

    assert destination.read_bytes() == b"new"
    assert not temporary.exists()
