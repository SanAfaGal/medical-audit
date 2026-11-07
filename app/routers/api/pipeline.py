"""API router for pipeline stage execution via SSE."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.routers.responses import temp_file_response
from app.paths import audit_data_root
from app.repositories.institution_repo import InstitutionRepo
from app.repositories.invoice_repo import InvoiceRepo

from app.services import pipeline as pipeline_runner
from app.services.archive import ArchiveError, ArchiveToolMissing, build_zip_file, extract_archive
from app.services.stage_events import heartbeat_event, with_heartbeat
from app.services.task_manager import HEARTBEAT_SECONDS, PipelineTaskManager, TaskAlreadyRunningError

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


def get_task_manager(request: Request) -> PipelineTaskManager:
    return request.app.state.task_manager  # type: ignore[no-any-return]


@router.get("/run/{stage}")
async def run_stage(
    stage: str,
    institution_id: int,
    period_id: int,
    invoice_numbers: str = "",
    doc_type_id: int = 0,
    db: AsyncSession = Depends(get_db),
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """Stream SSE log lines from a pipeline stage.

    Optional query params:
    - ``invoice_numbers``: comma-separated list, used by DOWNLOAD_INVOICES_FROM_SIHOS.
    - ``doc_type_id``: doc type to target, used by DOWNLOAD_MEDICATION_SHEETS.
    """
    inst_repo = InstitutionRepo(db)
    institution = await inst_repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")

    inv_repo = InvoiceRepo(db)
    period = await inv_repo.get_period_by_id(period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")

    extra: dict = {}
    if invoice_numbers:
        extra["invoice_numbers"] = [n.strip() for n in invoice_numbers.split(",") if n.strip()]
    if doc_type_id:
        extra["doc_type_id"] = doc_type_id

    async def event_gen():
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
    db: AsyncSession = Depends(get_db),
    tm: PipelineTaskManager = Depends(get_task_manager),
):
    """Start a pipeline stage as a background task; returns task_id for streaming."""
    institution = await InstitutionRepo(db).get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")

    extra: dict = {}
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

    async def event_gen():
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


def _run_summary(r) -> dict:
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
async def load_drive_zip(
    institution_id: int = Form(...),
    period_id: int = Form(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Extract a .zip / .7z / .rar archive into the DRIVE folder of the given period."""
    inst_repo = InstitutionRepo(db)
    institution = await inst_repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")

    inv_repo = InvoiceRepo(db)
    period = await inv_repo.get_period_by_id(period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")

    drive_path: Path = audit_data_root / institution.name / period.period_label / "DRIVE"
    drive_path.mkdir(parents=True, exist_ok=True)

    # Starlette already spooled the upload to disk: hand the file object over, never read it all into memory.
    if settings.max_archive_bytes and (file.size or 0) > settings.max_archive_bytes:
        raise HTTPException(413, f"Archivo demasiado grande (máx {settings.max_archive_bytes} bytes)")

    try:
        # Extraction is blocking disk work: keep it off the event loop.
        extracted = await asyncio.to_thread(extract_archive, file.file, file.filename or "", drive_path)
    except ArchiveToolMissing as exc:
        raise HTTPException(500, str(exc)) from exc
    except ArchiveError as exc:
        raise HTTPException(400, str(exc)) from exc

    return {
        "ok": True,
        "extracted": len(extracted),
        "drive_path": str(drive_path),
    }


class _NonPdfDecision(BaseModel):
    rel_path: str
    action: Literal["delete", "convert"]


class _ProcessNonPdfRequest(BaseModel):
    institution_id: int
    period_id: int
    decisions: list[_NonPdfDecision]


@router.post("/process-non-pdf")
async def process_non_pdf_decisions(
    data: _ProcessNonPdfRequest,
    db: AsyncSession = Depends(get_db),
):
    """Apply delete/convert decisions for files found in the REMOVE_NON_PDF scan."""
    from app.core.ops import IMAGE_EXTENSIONS, convert_image_to_pdf

    institution = await InstitutionRepo(db).get_by_id(data.institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    period = await InvoiceRepo(db).get_period_by_id(data.period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")

    stage_path = audit_data_root / institution.name / period.period_label / "STAGE"
    if not stage_path.is_dir():
        raise HTTPException(400, "Directorio STAGE no existe")

    deleted = 0
    converted = 0
    errors: list[str] = []
    loop = asyncio.get_running_loop()

    for decision in data.decisions:
        # Security: resolve and verify path is inside stage_path
        try:
            abs_path = (stage_path / decision.rel_path).resolve()
            abs_path.relative_to(stage_path.resolve())
        except (ValueError, OSError):
            errors.append(f"Ruta inválida: {decision.rel_path}")
            continue

        if not abs_path.exists():
            errors.append(f"Archivo no encontrado: {decision.rel_path}")
            continue

        if decision.action == "delete":
            try:
                abs_path.unlink()
                deleted += 1
            except OSError as exc:
                errors.append(f"No se pudo eliminar {abs_path.name}: {exc}")

        elif decision.action == "convert":
            ext = abs_path.suffix.lstrip(".").lower()
            if ext not in IMAGE_EXTENSIONS:
                errors.append(f"Formato no convertible: {abs_path.name}")
                continue
            try:
                await loop.run_in_executor(None, convert_image_to_pdf, abs_path)
                converted += 1
            except Exception as exc:  # noqa: BLE001 - one bad image must not abort the batch
                errors.append(f"Error convirtiendo {abs_path.name}: {exc}")

    return {"ok": True, "deleted": deleted, "converted": converted, "errors": errors}


@router.get("/stream-audit-zip")
async def stream_audit_zip(
    institution_id: int,
    period_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Empaqueta la carpeta AUDIT en un ZIP temporal (en disco, no en memoria) y lo devuelve.
    El ZIP temporal se borra al terminar el envío, así que no hay límite práctico de tamaño.
    """
    import re

    institution = await InstitutionRepo(db).get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")
    base = audit_data_root / institution.name / period.period_label
    audit_path = base / "AUDIT"

    if not audit_path.is_dir():
        raise HTTPException(404, "Directorio AUDIT no encontrado")

    all_files = await asyncio.to_thread(lambda: [f for f in audit_path.rglob("*") if f.is_file()])
    if not all_files:
        raise HTTPException(404, "No hay archivos en AUDIT")

    zip_path = await asyncio.to_thread(build_zip_file, ((f, f.relative_to(audit_path)) for f in all_files))

    safe_inst = re.sub(r"[^\w]", "_", institution.name)
    safe_period = re.sub(r"[^\w]", "_", period.period_label)
    filename = f"{safe_inst}_{safe_period}.zip"

    return temp_file_response(zip_path, filename, "application/zip")


_PREVIEW_MIME: dict[str, str] = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "tiff": "image/tiff",
    "tif": "image/tiff",
    "gif": "image/gif",
    "webp": "image/webp",
    "bmp": "image/bmp",
}


@router.get("/file-preview")
async def file_preview(
    institution_id: int,
    period_id: int,
    rel_path: str,
    db: AsyncSession = Depends(get_db),
):
    """Serve a non-PDF file from STAGE for in-browser preview."""
    institution = await InstitutionRepo(db).get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if not period:
        raise HTTPException(404, "Período no encontrado")

    stage_path = audit_data_root / institution.name / period.period_label / "STAGE"

    try:
        abs_path = (stage_path / rel_path).resolve()
        abs_path.relative_to(stage_path.resolve())
    except (ValueError, OSError) as exc:
        raise HTTPException(400, "Ruta inválida") from exc

    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(404, "Archivo no encontrado")

    ext = abs_path.suffix.lstrip(".").lower()
    media_type = _PREVIEW_MIME.get(ext, "application/octet-stream")

    return FileResponse(str(abs_path), media_type=media_type, filename=abs_path.name)
