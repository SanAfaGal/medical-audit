"""API router for pipeline stage execution via SSE."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
import json
import time
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.routers.responses import temp_file_response
from app.routers.deps import PeriodScope, load_period_scope, period_scope
from app.routers.errors import http_errors

from app.services import period_files
from app.services import pipeline as pipeline_runner
from app.services.stage_events import heartbeat_event, with_heartbeat
from app.services.task_manager import HEARTBEAT_SECONDS, PipelineRun, PipelineTaskManager, TaskAlreadyRunningError

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


def get_task_manager(request: Request) -> PipelineTaskManager:
    manager: PipelineTaskManager = request.app.state.task_manager
    return manager


@router.get("/run/{stage}")
async def run_stage(
    stage: str,
    institution_id: int,
    period_id: int,
    invoice_numbers: str = "",
    doc_type_id: int = 0,
    scope: PeriodScope = Depends(period_scope),
    db: AsyncSession = Depends(get_db),
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """Stream SSE log lines from a pipeline stage.

    Optional query params:
    - ``invoice_numbers``: comma-separated list, used by DOWNLOAD_INVOICES_FROM_SIHOS.
    - ``doc_type_id``: doc type to target, used by DOWNLOAD_MEDICATION_SHEETS.
    """
    institution, period = scope.institution, scope.period

    extra: dict[str, Any] = {}
    if invoice_numbers:
        extra["invoice_numbers"] = [n.strip() for n in invoice_numbers.split(",") if n.strip()]
    if doc_type_id:
        extra["doc_type_id"] = doc_type_id

    async def event_gen() -> AsyncGenerator[str, None]:
        # Same per-(institution, period) lock as background tasks. Taken here, inside the generator,
        # so the finally below always releases it even if the client drops before the first byte.
        try:
            run = tm.begin_sync(stage, institution_id, period_id)
        except TaskAlreadyRunningError as exc:
            busy = (
                f"[ERROR] Ya hay una tarea activa para este hospital y período ({exc.run.stage}). Espera a que termine."
            )
            yield f"data: {json.dumps({'msg': busy})}\n\n"
            yield 'data: {"msg": "[DONE]"}\n\n'
            return

        status: Literal["done", "error", "cancelled"] = "error"
        try:
            started = last_line = time.monotonic()
            stream = pipeline_runner.execute(stage, institution, period, db, extra)
            async for line in with_heartbeat(stream, HEARTBEAT_SECONDS):
                if line is None:  # quiet period: tell the browser the server is still working
                    now = time.monotonic()
                    yield heartbeat_event(idle=now - last_line, elapsed=now - started)
                    continue
                last_line = time.monotonic()
                payload = json.dumps({"msg": line})
                yield f"data: {payload}\n\n"
            status = "done"
            yield 'data: {"msg": "[DONE]"}\n\n'
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        finally:
            tm.end_sync(run, status)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/run/{stage}")
async def start_stage_task(
    stage: str,
    institution_id: int,
    period_id: int,
    invoice_numbers: str = "",
    doc_type_id: int = 0,
    scope: PeriodScope = Depends(period_scope),
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """Start a pipeline stage as a background task; returns task_id for streaming."""
    extra: dict[str, Any] = {}
    if invoice_numbers:
        extra["invoice_numbers"] = [n.strip() for n in invoice_numbers.split(",") if n.strip()]
    if doc_type_id:
        extra["doc_type_id"] = doc_type_id

    try:
        run = await tm.start(stage, institution_id, period_id, extra)
    except TaskAlreadyRunningError as exc:
        raise HTTPException(409, f"Ya hay una tarea activa: {exc.run.task_id}") from exc
    return {"task_id": run.task_id, "stage": stage}


@router.get("/stream/{task_id}")
async def stream_task_logs(
    task_id: str,
    from_: int = Query(0, alias="from"),
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """SSE stream of log lines for a background task, with replay from a cursor."""
    run = tm.get_run(task_id)
    if not run:
        raise HTTPException(404, "Tarea no encontrada")

    async def event_gen() -> AsyncGenerator[str, None]:
        async for idx, line in tm.stream_from(task_id, from_):
            if line is None:  # quiet period: named event so the UI can show server-side liveness
                now = datetime.utcnow()
                yield heartbeat_event(
                    idle=(now - run.last_activity).total_seconds(),
                    elapsed=(now - run.created_at).total_seconds(),
                )
                continue
            payload = json.dumps({"msg": line, "idx": idx})
            yield f"data: {payload}\n\n"
        run_final = tm.get_run(task_id)
        status = run_final.status if run_final else "done"
        yield f"data: {json.dumps({'msg': '[DONE]', 'status': status})}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _run_summary(r: PipelineRun) -> dict[str, Any]:
    return {
        "task_id": r.task_id,
        "stage": r.stage,
        "institution_id": r.institution_id,
        "period_id": r.period_id,
        "status": r.status,
        "log_count": len(r.logs) + r.log_offset,
        "created_at": r.created_at.isoformat(),
        "last_activity": r.last_activity.isoformat(),
    }


@router.get("/active")
async def list_active(
    institution_id: int | None = None,
    period_id: int | None = None,
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """List running pipeline tasks, optionally filtered by institution and/or period."""
    return [
        _run_summary(r)
        for r in tm.get_all_active()
        if (institution_id is None or r.institution_id == institution_id)
        and (period_id is None or r.period_id == period_id)
    ]


@router.get("/status/{task_id}")
async def task_status(task_id: str, tm: PipelineTaskManager = Depends(get_task_manager)):
    """Lightweight status probe (running or finished). 404 if the server no longer knows the task."""
    run = tm.get_run(task_id)
    if not run:
        raise HTTPException(404, "Tarea no encontrada")
    return _run_summary(run)


@router.delete("/{task_id}", status_code=200)
async def cancel_task(task_id: str, tm: PipelineTaskManager = Depends(get_task_manager)):
    """Cancel a running background pipeline task."""
    cancelled = await tm.cancel(task_id)
    if not cancelled:
        raise HTTPException(404, "Tarea no encontrada o ya finalizada")
    return {"ok": True}


@router.post("/load-drive-zip")
@http_errors
async def load_drive_zip(
    institution_id: int = Form(...),
    period_id: int = Form(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Extract a .zip / .7z / .rar archive into the DRIVE folder of the given period."""
    drive_path = (await load_period_scope(db, institution_id, period_id)).paths.drive
    # Starlette already spooled the upload to disk: hand the file object over, never read it all into memory.
    extracted = await asyncio.to_thread(
        period_files.extract_drive_archive,
        drive_path,
        file.file,
        file.filename or "",
        file.size,
        settings.max_archive_bytes,
    )
    return {"ok": True, "extracted": extracted, "drive_path": str(drive_path)}


class _NonPdfDecision(BaseModel):
    rel_path: str
    action: Literal["delete", "convert"]


class _ProcessNonPdfRequest(BaseModel):
    institution_id: int
    period_id: int
    decisions: list[_NonPdfDecision]


@router.post("/process-non-pdf")
@http_errors
async def process_non_pdf_decisions(
    data: _ProcessNonPdfRequest,
    db: AsyncSession = Depends(get_db),
):
    """Apply delete/convert decisions for files found in the REMOVE_NON_PDF scan."""
    stage_path = (await load_period_scope(db, data.institution_id, data.period_id)).paths.stage
    outcome = await asyncio.to_thread(
        period_files.apply_non_pdf_decisions, stage_path, [(d.rel_path, d.action) for d in data.decisions]
    )
    return {"ok": True, "deleted": outcome.deleted, "converted": outcome.converted, "errors": outcome.errors}


@router.get("/stream-audit-zip")
@http_errors
async def stream_audit_zip(scope: PeriodScope = Depends(period_scope)):
    """Empaqueta la carpeta AUDIT en un ZIP temporal (en disco, no en memoria) y lo devuelve.
    El ZIP temporal se borra al terminar el envío, así que no hay límite práctico de tamaño.
    """
    zip_path, filename = await asyncio.to_thread(
        period_files.audit_zip, scope.paths.audit, scope.institution.name, scope.period.period_label
    )
    return temp_file_response(zip_path, filename, "application/zip")


@router.get("/file-preview")
@http_errors
async def file_preview(
    institution_id: int,
    period_id: int,
    rel_path: str,
    scope: PeriodScope = Depends(period_scope),
):
    """Serve a non-PDF file from STAGE for in-browser preview."""
    target, media_type = await asyncio.to_thread(period_files.preview_target, scope.paths.stage, rel_path)
    return FileResponse(str(target), media_type=media_type, filename=target.name)
