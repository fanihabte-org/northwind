from __future__ import annotations

import threading
import time
from contextlib import contextmanager

import pytest

from fakeforce.deadline import QueryDeadlineExceeded, run_with_deadline


class BlockingConnection:
    def __init__(self) -> None:
        self.interrupted = threading.Event()

    def interrupt(self) -> None:
        self.interrupted.set()


def test_deadline_interrupts_the_owning_connection() -> None:
    connection = BlockingConnection()

    @contextmanager
    def connection_factory():
        yield connection

    def blocking_work(_: object) -> None:
        connection.interrupted.wait(timeout=1)

    started = time.monotonic()
    with pytest.raises(QueryDeadlineExceeded):
        run_with_deadline(connection_factory, blocking_work, timeout_seconds=0.01)

    assert connection.interrupted.is_set()
    assert time.monotonic() - started < 0.5
