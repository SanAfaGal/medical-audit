"""In-process pipeline task registry with SSE-compatible log streaming."""

from __future__ import annotations

import asyncio
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncGenerator, Literal

MAX_LOG_LINES = 5_000
MAX_COMPLETED_RUNS = 10
HEARTBEAT_SECONDS = 15  # stream_from emits a heartbeat (None) after this long without new lines


class TaskAlreadyRunningError(Exception):
    """Raised by PipelineTaskManager.start when a task is already running for the same context."""

    def __init__(self, run: "PipelineRun") -> None:
        self.run = run
        super().__init__(f"Task already running for this context: {run.task_id}")


@dataclass
class PipelineRun:
    task_id: str
    stage: str
    institution_id: int
    period_id: int
    status: Literal["running", "done", "error", "cancelled"] = "running"
    # False for interactive stages streamed straight over the HTTP request (GET /run): they hold the
    # per-context lock but have no background task, log buffer to replay or cancel handle.
    background: bool = True
    logs: list[str] = field(default_factory=list)
    log_offset: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    last_activity: datetime = field(default_factory=datetime.utcnow)
    # Set on cancel so blocking worker threads (Playwright downloads) can stop between items.
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _task: asyncio.Task[None] | None = field(default=None, repr=False)
    _condition: asyncio.Condition = field(default_factory=asyncio.Condition, repr=False)


class PipelineTaskManager:
    def __init__(self) -> None:
        self._runs: dict[str, PipelineRun] = {}

    def get_run(self, task_id: str) -> PipelineRun | None:
        return self._runs.get(task_id)

    def get_active_for_context(self, institution_id: int, period_id: int) -> PipelineRun | None:
        return next(
            (
                r
                for r in self._runs.values()
                if r.status == "running" and r.institution_id == institution_id and r.period_id == period_id
            ),
            None,
        )

    def get_all_active(self) -> list[PipelineRun]:
        """Running background tasks (the ones a client can re-attach to or cancel)."""
        return [r for r in self._runs.values() if r.status == "running" and r.background]

    def begin_sync(self, stage: str, institution_id: int, period_id: int) -> PipelineRun:
        """Register a stage streamed over the request, under the same per-context lock as background tasks.

        Raises TaskAlreadyRunningError if any stage is already running for the institution/period, so two
        stages can never touch the same period's files at once, whichever way they were started.
        Always pair with :meth:`end_sync` (use try/finally).
        """
        existing = self.get_active_for_context(institution_id, period_id)
        if existing is not None:
            raise TaskAlreadyRunningError(existing)
        run = PipelineRun(
            task_id=uuid.uuid4().hex,
            stage=stage,
            institution_id=institution_id,
            period_id=period_id,
            background=False,
        )
        self._runs[run.task_id] = run
        self._evict_old_runs()
        return run

    def end_sync(self, run: PipelineRun, status: Literal["done", "error", "cancelled"] = "done") -> None:
        """Release the lock taken by :meth:`begin_sync`."""
        run.status = status
        run.finished_at = datetime.utcnow()

    async def start(self, stage: str, institution_id: int, period_id: int, extra: dict[str, Any]) -> PipelineRun:
        # No `await` between the check and the registration below: this makes the
        # check-then-register atomic with respect to the event loop, closing the
        # TOCTOU window that existed when this check lived in the router (with
        # DB awaits in between).
        existing = self.get_active_for_context(institution_id, period_id)
        if existing is not None:
            raise TaskAlreadyRunningError(existing)
        run = PipelineRun(
            task_id=uuid.uuid4().hex,
            stage=stage,
            institution_id=institution_id,
            period_id=period_id,
        )
        self._runs[run.task_id] = run
        self._evict_old_runs()
        run._task = asyncio.create_task(self._run_stage(run, stage, institution_id, period_id, extra))
        return run

    async def cancel(self, task_id: str) -> bool:
        run = self._runs.get(task_id)
        if not run or run._task is None or run._task.done():
            return False
        run.cancel_event.set()
        run._task.cancel()
        return True

    async def stream_from(self, task_id: str, from_index: int) -> AsyncGenerator[tuple[int, str | None], None]:
        """Yield (index, line) from from_index onwards; blocks until done.

        While the task is quiet it yields ``(cursor, None)`` every HEARTBEAT_SECONDS so the
        caller can keep the SSE connection alive.
        """
        run = self._runs.get(task_id)
        if not run:
            return
        cursor = from_index
        async with run._condition:
            if cursor < run.log_offset:
                skipped = run.log_offset - cursor
                yield run.log_offset, f"[WARN] Se omitieron {skipped} línea(s) de log (buffer recortado)"
                cursor = run.log_offset
            while True:
                list_idx = cursor - run.log_offset
                while list_idx < len(run.logs):
                    yield cursor, run.logs[list_idx]
                    cursor += 1
                    list_idx += 1
                if run.status != "running" and list_idx >= len(run.logs):
                    return
                try:
                    await asyncio.wait_for(run._condition.wait(), timeout=HEARTBEAT_SECONDS)
                except asyncio.TimeoutError:
                    yield cursor, None

    async def _run_stage(
        self,
        run: PipelineRun,
        stage: str,
        institution_id: int,
        period_id: int,
        extra: dict[str, Any],
    ) -> None:
        from app.database import AsyncSessionLocal
        from app.repositories.institution_repo import InstitutionRepo
        from app.repositories.invoice_repo import InvoiceRepo
        from app.services import pipeline as pipeline_runner

        extra = {**extra, "cancel_event": run.cancel_event}
        try:
            async with AsyncSessionLocal() as db:
                institution = await InstitutionRepo(db).get_by_id(institution_id)
                period = await InvoiceRepo(db).get_period_by_id(period_id)
                if institution is None or period is None:
                    raise RuntimeError(f"Institution {institution_id} or period {period_id} not found")
                async for line in pipeline_runner.execute(stage, institution, period, db, extra):
                    async with run._condition:
                        run.logs.append(line)
                        run.last_activity = datetime.utcnow()
                        if len(run.logs) > MAX_LOG_LINES:
                            evicted = len(run.logs) - MAX_LOG_LINES
                            run.logs = run.logs[-MAX_LOG_LINES:]
                            run.log_offset += evicted
                        run._condition.notify_all()
            async with run._condition:
                run.status = "done"
                run.finished_at = datetime.utcnow()
                run._condition.notify_all()
        except asyncio.CancelledError:
            async with run._condition:
                run.logs.append("[WARN] Tarea cancelada por el usuario")
                run.status = "cancelled"
                run.finished_at = datetime.utcnow()
                run._condition.notify_all()
        except Exception as exc:  # noqa: BLE001 - task boundary: any stage failure becomes an error status
            async with run._condition:
                run.logs.append(f"[ERROR] Error inesperado: {type(exc).__name__}: {exc}")
                run.status = "error"
                run.finished_at = datetime.utcnow()
                run._condition.notify_all()

    def _evict_old_runs(self) -> None:
        done = [r for r in self._runs.values() if r.status != "running"]
        done.sort(key=lambda r: r.finished_at or datetime.min)
        for r in done[:-MAX_COMPLETED_RUNS]:
            self._runs.pop(r.task_id, None)
