"""Tests for the central stage reporting: event lines, heartbeat, threaded runs and Drive events."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

import pytest

from app.core.downloader import _first_line
from app.core.drive import DriveSync
from app.core.helpers import emit_event
from app.services.stage_events import (
    event_lines,
    heartbeat_event,
    progress_line,
    run_with_events,
    with_heartbeat,
)


def _data(line: str) -> dict:
    assert line.startswith("[DATA] ")
    return json.loads(line[len("[DATA] ") :])


class TestEventLines:
    def test_item_start_reports_completed_count_before_this_item(self):
        lines = event_lines("item_start", 3, 10, "FE123", "", noun="factura", verb="Descargando", label="Facturas")
        assert lines[0] == "[INFO] [3/10] Descargando factura FE123..."
        assert _data(lines[1]) == {"type": "progress", "current": 2, "total": 10, "item": "FE123", "label": "Facturas"}

    def test_item_ok_counts_it_as_completed_and_uses_detail(self):
        lines = event_lines("item_ok", 3, 10, "FE123", "descargada", noun="factura")
        assert lines[0] == "[INFO] [3/10] Factura FE123 descargada"
        assert _data(lines[1])["current"] == 3

    def test_item_error_is_a_warning_not_an_error(self):
        lines = event_lines("item_error", 3, 10, "FE123", "Timeout 30000ms exceeded.", noun="factura")
        # [ERROR] would mark the whole stage as failed in the UI; one bad item must not.
        assert lines[0].startswith("[WARN]")
        assert "Timeout 30000ms exceeded." in lines[0]
        assert _data(lines[1])["current"] == 3

    def test_status_warning_and_cancelled(self):
        assert event_lines("status", 0, 0, "", "Iniciando navegador...") == ["[INFO] Iniciando navegador..."]
        assert event_lines("warning", 0, 0, "", "algo raro") == ["[WARN] algo raro"]
        (line,) = event_lines("cancelled", 4, 9, "", "", noun="carpeta")
        assert line.startswith("[WARN]") and "4/9" in line

    def test_unknown_event_is_ignored(self):
        assert event_lines("whatever", 1, 1, "x", "") == []

    def test_progress_line_defaults(self):
        assert _data(progress_line(0, 5)) == {
            "type": "progress",
            "current": 0,
            "total": 5,
            "item": "",
            "label": "Elementos",
        }


class TestEmitEvent:
    def test_passes_arguments(self):
        calls = []
        emit_event(lambda *a: calls.append(a), "item_ok", 1, 2, "FE1", "d")
        assert calls == [("item_ok", 1, 2, "FE1", "d")]

    def test_without_callback_is_noop(self):
        emit_event(None, "status")

    def test_swallows_callback_errors(self):
        def boom(*a):
            raise RuntimeError("ui exploded")

        emit_event(boom, "status")  # a reporting bug must not abort the job

    def test_first_line_trims_playwright_call_log(self):
        exc = TimeoutError("Page.goto: Timeout 30000ms exceeded.\nCall log:\n  - navigating to ...")
        assert _first_line(exc) == "Page.goto: Timeout 30000ms exceeded."
        assert _first_line(TimeoutError()) == "TimeoutError"


class TestRunWithEvents:
    async def test_streams_thread_events_as_log_lines(self):
        def job(event_fn, cancel_event):
            event_fn("status", 0, 0, "", "arranca")
            event_fn("item_start", 1, 2, "A", "")
            event_fn("item_ok", 1, 2, "A", "listo")

        lines = [line async for line in run_with_events(job, {}, noun="carpeta", verb="Descargando", label="Carpetas")]
        assert lines[0] == "[INFO] arranca"
        assert "[INFO] [1/2] Descargando carpeta A..." in lines
        assert "[INFO] [1/2] Carpeta A listo" in lines

    async def test_cancelling_the_task_stops_the_worker_thread(self):
        ctx = {"cancel_event": threading.Event()}
        returned = threading.Event()

        def job(event_fn, cancel_event):
            try:
                for i in range(1, 200):
                    if cancel_event.is_set():
                        return
                    event_fn("item_start", i, 200, str(i), "")
                    time.sleep(0.02)
            finally:
                returned.set()

        async def consume():
            async for _ in run_with_events(job, ctx):
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert ctx["cancel_event"].is_set()
        assert await asyncio.to_thread(returned.wait, 2)  # thread really stopped, not left running

    async def test_exceptions_from_the_job_propagate(self):
        def job(event_fn, cancel_event):
            raise RuntimeError("login falló")

        with pytest.raises(RuntimeError, match="login falló"):
            async for _ in run_with_events(job, {}):
                pass


class TestWithHeartbeat:
    async def test_yields_none_while_quiet_without_killing_the_source(self):
        async def source():
            yield "a"
            await asyncio.sleep(0.25)
            yield "b"

        out = [x async for x in with_heartbeat(source(), interval=0.05)]
        assert out[0] == "a" and out[-1] == "b"
        assert out.count(None) >= 2  # several heartbeats during the quiet gap
        assert [x for x in out if x is not None] == ["a", "b"]

    async def test_passes_through_fast_sources_and_finishes(self):
        async def source():
            for i in range(3):
                yield i

        assert [x async for x in with_heartbeat(source(), interval=5)] == [0, 1, 2]

    async def test_closing_early_cancels_the_source(self):
        cancelled = asyncio.Event()

        async def source():
            try:
                yield 1
                await asyncio.sleep(30)
                yield 2
            except asyncio.CancelledError:
                cancelled.set()
                raise

        gen = with_heartbeat(source(), interval=0.05)
        assert await gen.__anext__() == 1
        await gen.aclose()
        await asyncio.wait_for(cancelled.wait(), 2)

    def test_heartbeat_event_is_a_named_sse_frame(self):
        frame = heartbeat_event(idle=61.4, elapsed=125.6)
        assert frame.startswith("event: heartbeat\ndata: ")
        assert frame.endswith("\n\n")
        assert json.loads(frame.split("data: ")[1]) == {"idle": 61, "elapsed": 126}


class _FakeDrive(DriveSync):
    """DriveSync without credentials: search and folder sync are stubbed."""

    def __init__(self, found: set[str], folders: list[dict], files_per_folder: int = 3) -> None:  # noqa: D107
        self._found, self._folders, self._files = found, folders, files_per_folder
        self.synced: list[str] = []

    def _batch_search_folders(self, invoice_numbers):
        return self._found, self._folders

    def _sync_folder_tree(self, folder_id, local_path, depth=0, cancel_event=None):
        self.synced.append(local_path.name)
        return self._files


class TestDriveEvents:
    def _run(self, drive, numbers, cancel_event=None):
        events = []
        result = drive.download_missing_dirs(
            numbers, Path("/x"), event_fn=lambda *a: events.append(a), cancel_event=cancel_event
        )
        return result, events

    def test_reports_search_each_folder_and_not_found(self):
        folders = [{"id": "1", "name": "FE-100"}, {"id": "2", "name": "FE200 YA"}]
        drive = _FakeDrive({"100", "200"}, folders)

        result, events = self._run(drive, ["100", "200", "300"])

        assert sorted(result) == ["100", "200"]
        kinds = [e[0] for e in events]
        assert kinds[:2] == ["status", "status"]
        assert ("warning", 0, 0, "", "No se encontró carpeta en Drive para la factura 300") in events
        assert ("item_start", 1, 2, "FE-100", "") in events
        assert ("item_ok", 2, 2, "FE200 YA", "descargada (3 archivo(s))") in events
        assert drive.synced == ["FE-100", "FE200 YA"]

    def test_cancel_before_a_folder_stops_without_syncing_it(self):
        cancel = threading.Event()
        drive = _FakeDrive({"1", "2"}, [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}])
        original = drive._sync_folder_tree

        def sync_then_cancel(*a, **kw):
            n = original(*a, **kw)
            cancel.set()  # user cancels while folder A is being downloaded
            return n

        drive._sync_folder_tree = sync_then_cancel  # type: ignore[method-assign]

        _, events = self._run(drive, ["1", "2"], cancel_event=cancel)

        assert drive.synced == ["A"]  # B never started
        assert events[-1][0] == "cancelled"
        assert not any(e[0] == "item_ok" for e in events)  # A was interrupted: not reported as complete

    def test_empty_input_returns_immediately(self):
        drive = _FakeDrive(set(), [])
        assert self._run(drive, [])[0] == []
