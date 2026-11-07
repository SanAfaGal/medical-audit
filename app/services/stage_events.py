"""Central reporting for pipeline stages: one way to show progress, liveness and cancellation.

Every long-running stage should look the same to the user, so the pieces live here instead of
being re-implemented per stage:

* ``progress_line`` / ``event_lines``  - the log lines and ``[DATA]`` progress payload for the
  event contract that blocking jobs in ``app/core`` emit (see ``app.core.helpers.emit_event``).
* ``run_with_events``  - runs a blocking job in a worker thread and streams its events as log
  lines, wiring task cancellation to the thread.
* ``run_with_progress``  - like ``run_with_events`` but only drives the progress bar, for scans that
  report their findings once at the end.
* ``ProgressJob``  - ``run_with_progress`` packaged so the stage can read the job's return value.
* ``with_heartbeat`` / ``heartbeat_event``  - server-side liveness signal that works for *any*
  stage, including ones that never emit a line.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Generic, Mapping, TypeVar

T = TypeVar("T")


def _fmt(level: str, msg: str) -> str:
    return f"[{level}] {msg}"


def progress_line(current: int, total: int, item: str = "", label: str = "Elementos") -> str:
    """Machine-readable progress payload consumed by the pipeline UI progress bar."""
    return "[DATA] " + json.dumps(
        {"type": "progress", "current": current, "total": total, "item": item, "label": label},
        ensure_ascii=False,
    )


def event_lines(
    kind: str,
    i: int,
    total: int,
    item: str,
    detail: str,
    *,
    noun: str = "elemento",
    verb: str = "Procesando",
    label: str = "Elementos",
) -> list[str]:
    """Translate one ``event_fn`` event into human log lines (plus [DATA] progress payloads).

    ``noun``/``verb``/``label`` customise the wording per stage ("factura"/"Descargando"/"Facturas").
    One bad item is a WARN, never an ERROR: an ``[ERROR]`` line marks the whole stage as failed.
    """
    cap = noun.capitalize()
    if kind == "status":
        return [_fmt("INFO", detail)]
    if kind == "warning":
        return [_fmt("WARN", detail)]
    if kind == "item_start":
        return [_fmt("INFO", f"[{i}/{total}] {verb} {noun} {item}..."), progress_line(i - 1, total, item, label)]
    if kind == "item_ok":
        suffix = f" {detail}" if detail else " listo"
        return [_fmt("INFO", f"[{i}/{total}] {cap} {item}{suffix}"), progress_line(i, total, item, label)]
    if kind == "item_error":
        return [
            _fmt("WARN", f"[{i}/{total}] {cap} {item} no se pudo procesar: {detail}"),
            progress_line(i, total, item, label),
        ]
    if kind == "cancelled":
        return [_fmt("WARN", f"Proceso detenido antes del {noun} {i}/{total}")]
    return []


async def run_with_events(
    run_blocking: Callable[[Callable[[str, int, int, str, str], None], object], object],
    ctx: Mapping[str, Any],
    *,
    noun: str = "elemento",
    verb: str = "Procesando",
    label: str = "Elementos",
) -> AsyncGenerator[str, None]:
    """Run a blocking job in a thread, streaming its events as log lines.

    ``run_blocking(event_fn, cancel_event)`` performs the work. Cancelling the task also sets
    ``ctx['cancel_event']`` so the worker thread stops after the current item instead of
    carrying on in the background.
    """
    loop = asyncio.get_running_loop()
    q: asyncio.Queue[str] = asyncio.Queue()

    def _on_event(kind: str, i: int, total: int, item: str, detail: str) -> None:
        for line in event_lines(kind, i, total, item, detail, noun=noun, verb=verb, label=label):
            loop.call_soon_threadsafe(q.put_nowait, line)

    cancel_event = ctx.get("cancel_event")
    task = asyncio.ensure_future(loop.run_in_executor(None, lambda: run_blocking(_on_event, cancel_event)))
    try:
        while not task.done():
            try:
                yield q.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.3)
        while not q.empty():
            yield q.get_nowait()
        await task  # propagate exceptions
    except asyncio.CancelledError:
        if cancel_event is not None:
            cancel_event.set()  # stop the worker thread after the current item
        raise


async def run_with_progress(
    run_blocking: Callable[[Callable[[int, int, str], None], threading.Event | None], T],
    ctx: Mapping[str, Any],
    result: list[T],
    *,
    label: str = "Archivos",
    interval: float = 0.3,
) -> AsyncGenerator[str, None]:
    """Run a blocking job in a thread and stream only its progress bar (no per-item log lines).

    For jobs that scan many files and report their findings at the end: the thread calls
    ``on_progress(done, total, item)`` as often as it likes and the UI gets at most one
    ``[DATA]`` progress line per ``interval``. ``run_blocking(on_progress, cancel_event)``'s return
    value is appended to ``result`` once the job ends.
    """
    loop = asyncio.get_running_loop()
    latest: dict[str, tuple[int, int, str]] = {}

    def _on_progress(done: int, total: int, item: str = "") -> None:
        latest["cur"] = (done, total, item)  # a single dict store is atomic under the GIL

    cancel_event = ctx.get("cancel_event")
    task = asyncio.ensure_future(loop.run_in_executor(None, lambda: run_blocking(_on_progress, cancel_event)))
    last: tuple[int, int, str] | None = None
    try:
        while not task.done():
            cur = latest.get("cur")
            if cur is not None and cur != last:
                last = cur
                yield progress_line(cur[0], cur[1], cur[2], label)
            await asyncio.sleep(interval)
        result.append(await task)  # propagate exceptions
        cur = latest.get("cur")
        if cur is not None and cur != last:
            yield progress_line(cur[0], cur[1], cur[2], label)
    except asyncio.CancelledError:
        if cancel_event is not None:
            cancel_event.set()
        raise


class ProgressJob(Generic[T]):
    """A blocking job whose progress bar is streamed as log lines and whose return value ends up in ``result``.

    job = ProgressJob(lambda on_progress, cancel: scan(files, on_progress, cancel), ctx, label="PDFs", total=len(files))
    async for line in job:
        yield line
    found = job.result
    """

    def __init__(
        self,
        run_blocking: Callable[[Callable[[int, int, str], None], threading.Event | None], T],
        ctx: Mapping[str, Any],
        *,
        label: str = "Archivos",
        total: int | None = None,
    ) -> None:
        self._run_blocking = run_blocking
        self._ctx = ctx
        self._label = label
        self._total = total
        self.result: T

    def __aiter__(self) -> AsyncGenerator[str, None]:
        return self._lines()

    async def _lines(self) -> AsyncGenerator[str, None]:
        if self._total is not None:
            yield progress_line(0, self._total, label=self._label)  # the bar shows up before the first file ends
        out: list[T] = []
        async for line in run_with_progress(self._run_blocking, self._ctx, out, label=self._label):
            yield line
        self.result = out[0]


def heartbeat_event(idle: float, elapsed: float) -> str:
    """SSE frame telling the browser the server is alive and how long the stage has been quiet."""
    payload = json.dumps({"idle": round(idle), "elapsed": round(elapsed)})
    return f"event: heartbeat\ndata: {payload}\n\n"


async def with_heartbeat(source: AsyncIterator[T], interval: float) -> AsyncGenerator[T | None, None]:
    """Yield items from ``source``; yield ``None`` whenever ``interval`` seconds pass without one.

    The pending ``__anext__`` is awaited with ``asyncio.wait`` (not ``wait_for``) so a quiet
    period never cancels the source generator.
    """
    it = source.__aiter__()
    pending: asyncio.Future = asyncio.ensure_future(it.__anext__())
    try:
        while True:
            done, _ = await asyncio.wait({pending}, timeout=interval)
            if not done:
                yield None
                continue
            try:
                item = pending.result()
            except StopAsyncIteration:
                return
            pending = asyncio.ensure_future(it.__anext__())  # keep working while the client reads
            yield item
    finally:
        if not pending.done():
            pending.cancel()
        with contextlib.suppress(BaseException):
            await pending
