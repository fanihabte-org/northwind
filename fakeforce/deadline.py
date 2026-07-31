"""Bounded execution helper for interruptible DuckDB work."""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from contextlib import AbstractContextManager
from typing import Callable, TypeVar


T = TypeVar("T")


class QueryDeadlineExceeded(TimeoutError):
    """A synchronous query exceeded its configured wall-clock deadline."""


def run_with_deadline(
    connection_factory: Callable[[], AbstractContextManager[object]],
    work: Callable[[object], T],
    timeout_seconds: float,
) -> T:
    """Run work on its owning thread and interrupt its connection on timeout.

    DuckDB connections are deliberately opened inside the worker thread.  On a
    timeout, `interrupt()` asks the engine to cancel rather than letting the
    Python process race toward memory or disk exhaustion.  The worker owns the
    connection lifetime and closes it once interruption completes.
    """
    connection_ready = threading.Event()
    holder: dict[str, object] = {}

    def invoke() -> T:
        with connection_factory() as connection:
            holder["connection"] = connection
            connection_ready.set()
            return work(connection)

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fakeforce-query")
    future: Future[T] = executor.submit(invoke)
    try:
        result = future.result(timeout=timeout_seconds)
    except TimeoutError as error:
        connection_ready.wait(timeout=0.25)
        connection = holder.get("connection")
        if connection is not None:
            interrupt = getattr(connection, "interrupt", None)
            if callable(interrupt):
                interrupt()
        # Allow an interrupted DuckDB operation to release its connection.  Do
        # not make the HTTP deadline depend on an uncooperative driver thread.
        executor.shutdown(wait=False, cancel_futures=True)
        raise QueryDeadlineExceeded("synchronous query exceeded its time limit") from error
    except BaseException:
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True, cancel_futures=True)
        return result
