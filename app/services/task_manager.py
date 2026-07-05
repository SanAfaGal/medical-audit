"""In-process pipeline task registry with SSE-compatible log streaming."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncGenerator, Literal

MAX_LOG_LINES = 5_000
MAX_COMPLETED_RUNS = 10


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
    logs: list[str] = field(default_factory=list)
    log_offset: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    _task: asyncio.Task | None = field(default=None, repr=False)
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
        return [r for r in self._runs.values() if r.status == "running"]

    async def start(self, stage: str, institution_id: int, period_id: int, extra: dict) -> PipelineRun:
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
        run._task.cancel()
        return True

    async def stream_from(self, task_id: str, from_index: int) -> AsyncGenerator[tuple[int, str], None]:
        """Yield (index, line) from from_index onwards; blocks until done."""
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
                await run._condition.wait()

    async def _run_stage(
        self,
        run: PipelineRun,
        stage: str,
        institution_id: int,
        period_id: int,
        extra: dict,
    ) -> None:
        from app.database import AsyncSessionLocal
        from app.repositories.institution_repo import InstitutionRepo
        from app.repositories.invoice_repo import InvoiceRepo
        from app.services import pipeline_runner

        try:
            async with AsyncSessionLocal() as db:
                institution = await InstitutionRepo(db).get_by_id(institution_id)
                period = await InvoiceRepo(db).get_period_by_id(period_id)
                if institution is None or period is None:
                    raise RuntimeError(f"Institution {institution_id} or period {period_id} not found")
                async for line in pipeline_runner.execute(stage, institution, period, db, extra):
                    async with run._condition:
                        run.logs.append(line)
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
        except Exception as exc:
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
