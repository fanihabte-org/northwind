"""Filesystem primitives for durable FakeForce artifacts."""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator


class InsufficientDiskSpaceError(RuntimeError):
    """Raised before an artifact or spill file consumes the disk reserve."""


def require_disk_reserve(
    directory: Path, reserve_bytes: int, additional_bytes: int = 0
) -> None:
    """Ensure anticipated work leaves the configured amount of free space."""
    directory.mkdir(parents=True, exist_ok=True)
    available = shutil.disk_usage(directory).free
    if available - additional_bytes < reserve_bytes:
        raise InsufficientDiskSpaceError(
            f"insufficient free disk space in {directory}: {available} bytes available, "
            f"{reserve_bytes} bytes reserved, {additional_bytes} bytes requested"
        )


def _fsync_directory(directory: Path) -> None:
    """Persist a rename on filesystems that support directory fsync."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # Some platforms/filesystems do not implement directory fsync.  The
        # file itself has already been fsynced, so do not turn that into data
        # loss by rejecting the completed atomic rename.
        pass
    finally:
        os.close(fd)


def publish_temp_file(temporary_path: Path, destination: Path) -> None:
    """Durably publish a file another writer produced in the destination directory."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with temporary_path.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(temporary_path, destination)
    _fsync_directory(destination.parent)


@contextmanager
def atomic_binary_writer(destination: Path) -> Iterator[BinaryIO]:
    """Yield a temporary stream, then publish it only after it is durable.

    Callers must commit related database state *after* this context exits.
    A failed writer deletes its private temporary file and leaves the previous
    destination untouched.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
