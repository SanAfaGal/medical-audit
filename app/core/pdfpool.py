"""Process pool for the CPU-bound PDF scans.

PyMuPDF keeps Python's GIL while it parses, so a thread pool cannot use more than about one core
(measured: 16 threads gave 1.2x over a plain loop, 8 processes gave 2.3x). The scans therefore hand their
files to worker *processes* in chunks and collect the answers in order.

* The pool starts lazily, lives as long as the app, and uses the ``spawn`` start method on every OS: ``fork``
  from a process that already runs threads (uvicorn, the asyncio loop) can deadlock, and Windows only has spawn.
* A crash while parsing a hostile PDF now kills a worker, not the API process; the batch is retried once.
* ``PDF_WORKERS`` sets the worker count (default: up to 8, never more than the CPUs). ``0`` or ``1`` turns the
  pool off and everything runs in the calling thread. Small batches skip it too: starting work in another
  process costs more than scanning a few dozen files.
"""

from __future__ import annotations

import atexit
import logging
import multiprocessing
import os
import threading
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from typing import TypeVar

from app.core.helpers import ProgressFn

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

#: Below this many items the work stays in the calling thread.
MIN_PARALLEL = 40

_pool: ProcessPoolExecutor | None = None
_pool_workers = 0
_lock = threading.Lock()


def worker_count() -> int:
    """Number of worker processes to use (<= 1 means: no pool)."""
    raw = os.environ.get("PDF_WORKERS", "").strip()
    if raw.isdigit():
        return int(raw)
    return min(8, os.cpu_count() or 1)


def _get_pool(workers: int) -> ProcessPoolExecutor:
    global _pool, _pool_workers
    with _lock:
        if _pool is None or _pool_workers != workers:
            if _pool is not None:
                _pool.shutdown(wait=False, cancel_futures=True)
            _pool = ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"))
            _pool_workers = workers
        return _pool


def _discard_pool() -> None:
    global _pool
    with _lock:
        if _pool is not None:
            _pool.shutdown(wait=False, cancel_futures=True)
            _pool = None


def shutdown() -> None:
    """Stop the workers (called at interpreter exit; safe to call earlier, the pool restarts on demand)."""
    _discard_pool()


atexit.register(shutdown)


def run_chunked(
    worker: Callable[[list[T]], list[R]],
    items: Sequence[T],
    *,
    chunk_size: int,
    names: Sequence[str] | None = None,
    on_progress: ProgressFn | None = None,
    cancel_event: threading.Event | None = None,
) -> list[R | None]:
    """Apply ``worker`` to ``items`` in chunks and return one result per item, in order.

    ``worker`` must be a module-level function (it is pickled) taking a list of items and returning a list of
    results of the same length. Items skipped because ``cancel_event`` was set come back as ``None``.
    ``on_progress(done, total, name)`` is called as chunks finish (once per item on the in-thread path).
    """
    total = len(items)
    results: list[R | None] = [None] * total
    if total == 0:
        return results

    def report(done: int, last: int) -> None:
        if on_progress is not None:
            on_progress(done, total, names[last] if names else "")

    workers = worker_count()
    if workers <= 1 or total < MIN_PARALLEL:
        for index, item in enumerate(items):
            if cancel_event is not None and cancel_event.is_set():
                break
            results[index] = worker([item])[0]
            report(index + 1, index)
        return results

    chunks = [(start, list(items[start : start + chunk_size])) for start in range(0, total, chunk_size)]
    for attempt in (1, 2):
        try:
            pool = _get_pool(workers)
            futures = {pool.submit(worker, chunk): (start, len(chunk)) for start, chunk in chunks}
            done = 0
            for future in as_completed(futures):
                if future.cancelled():
                    continue
                start, size = futures[future]
                results[start : start + size] = future.result()
                done += size
                report(done, start + size - 1)
                if cancel_event is not None and cancel_event.is_set():
                    for pending in futures:
                        pending.cancel()  # chunks already running finish; the rest never start
            return results
        except BrokenProcessPool:
            # A worker died (e.g. a PDF crashed the parser). Start a fresh pool and redo the batch once.
            logger.warning("PDF worker pool broke (attempt %d); restarting it", attempt)
            _discard_pool()
            results = [None] * total
            if attempt == 2:
                raise
    return results  # pragma: no cover - the loop always returns or raises
