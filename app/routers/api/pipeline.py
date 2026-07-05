"""API router for pipeline stage execution via SSE."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.paths import audit_data_root, safe_join
from app.repositories.institution_repo import InstitutionRepo
from app.repositories.invoice_repo import InvoiceRepo

from app.services import pipeline_runner
from app.services.task_manager import PipelineTaskManager, TaskAlreadyRunningError

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
        async for line in pipeline_runner.execute(stage, institution, period, db, extra):
            payload = json.dumps({"msg": line})
            yield f"data: {payload}\n\n"
        yield 'data: {"msg": "[DONE]"}\n\n'

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
        raise HTTPException(409, f"Ya hay una tarea activa: {exc.run.task_id}")
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


@router.get("/active")
async def list_active(tm: PipelineTaskManager = Depends(get_task_manager)):
    """List all currently running pipeline tasks."""
    return [
        {
            "task_id": r.task_id,
            "stage": r.stage,
            "institution_id": r.institution_id,
            "period_id": r.period_id,
            "status": r.status,
            "log_count": len(r.logs),
            "created_at": r.created_at.isoformat(),
        }
        for r in tm.get_all_active()
    ]


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

    drive_path: Path = (
        audit_data_root / institution.name / period.period_label / "DRIVE"
    )
    drive_path.mkdir(parents=True, exist_ok=True)

    filename = file.filename or ""
    file_bytes = await file.read()
    ext = Path(filename).suffix.lower()

    extracted: list[str] = []

    if ext == ".zip":
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            members = [m for m in zf.infolist() if not m.filename.endswith("/")]
            # Solo eliminar el directorio raíz si TODOS los archivos comparten
            # exactamente un directorio de nivel superior (es decir, el ZIP tiene
            # una carpeta envolvente). Si hay múltiples carpetas de nivel superior
            # (estructura real de facturas), preservar la ruta completa.
            top_levels = {Path(m.filename).parts[0] for m in members if Path(m.filename).parts}
            has_single_wrapper = len(top_levels) == 1
            for member in members:
                parts = Path(member.filename).parts
                if has_single_wrapper and len(parts) > 1:
                    rel = Path(*parts[1:])
                else:
                    rel = Path(*parts)
                try:
                    dest = safe_join(drive_path, str(rel))
                except ValueError:
                    raise HTTPException(400, f"Ruta insegura en ZIP: {member.filename}")
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(zf.read(member.filename))
                extracted.append(str(rel))

    elif ext == ".7z":
        import tempfile
        import shutil

        try:
            import py7zr
        except ImportError:
            raise HTTPException(500, "py7zr no está instalado (soporte .7z no disponible)")
        with tempfile.TemporaryDirectory() as tmpdir:
            with py7zr.SevenZipFile(io.BytesIO(file_bytes), mode="r") as szf:
                all_names = szf.getnames()
                top_levels = {Path(n).parts[0] for n in all_names if Path(n).parts}
                szf.extractall(path=tmpdir)
            src_root = Path(tmpdir)
            # Si hay un único directorio wrapper, bajar un nivel
            if len(top_levels) == 1:
                wrapper = Path(tmpdir) / top_levels.pop()
                if wrapper.is_dir():
                    src_root = wrapper
            for item in src_root.iterdir():
                dest = drive_path / item.name
                if item.is_dir():
                    shutil.copytree(str(item), str(dest), dirs_exist_ok=True)
                else:
                    shutil.copy2(str(item), str(dest))
        extracted = [str(p) for p in drive_path.rglob("*") if p.is_file()]

    elif ext == ".rar":
        import tempfile
        import shutil

        try:
            import rarfile
        except ImportError:
            raise HTTPException(500, "rarfile no está instalado (soporte .rar no disponible)")
        with tempfile.TemporaryDirectory() as tmpdir:
            with tempfile.NamedTemporaryFile(suffix=".rar", delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            try:
                with rarfile.RarFile(tmp_path) as rf:
                    all_names = rf.namelist()
                    top_levels = {Path(n).parts[0] for n in all_names if Path(n).parts}
                    rf.extractall(path=tmpdir)
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            src_root = Path(tmpdir)
            if len(top_levels) == 1:
                wrapper = Path(tmpdir) / top_levels.pop()
                if wrapper.is_dir():
                    src_root = wrapper
            for item in src_root.iterdir():
                dest = drive_path / item.name
                if item.is_dir():
                    shutil.copytree(str(item), str(dest), dirs_exist_ok=True)
                else:
                    shutil.copy2(str(item), str(dest))
        extracted = [str(p) for p in drive_path.rglob("*") if p.is_file()]

    else:
        raise HTTPException(400, f"Formato no soportado: '{ext}'. Use .zip, .7z o .rar")

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
    from core.ops import IMAGE_EXTENSIONS, convert_image_to_pdf

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
            except Exception as exc:
                errors.append(f"Error convirtiendo {abs_path.name}: {exc}")

    return {"ok": True, "deleted": deleted, "converted": converted, "errors": errors}


@router.get("/stream-audit-zip")
async def stream_audit_zip(
    institution_id: int,
    period_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Empaqueta la carpeta AUDIT en memoria y la devuelve como ZIP descargable.
    No escribe nada en disco — el ZIP se genera al vuelo en cada descarga.
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

    all_files = [f for f in audit_path.rglob("*") if f.is_file()]
    if not all_files:
        raise HTTPException(404, "No hay archivos en AUDIT")

    def _build_zip_bytes() -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in all_files:
                zf.write(f, f.relative_to(audit_path))
        return buf.getvalue()

    loop = asyncio.get_running_loop()
    zip_bytes = await loop.run_in_executor(None, _build_zip_bytes)

    safe_inst = re.sub(r"[^\w]", "_", institution.name)
    safe_period = re.sub(r"[^\w]", "_", period.period_label)
    filename = f"{safe_inst}_{safe_period}.zip"

    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
    except (ValueError, OSError):
        raise HTTPException(400, "Ruta inválida")

    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(404, "Archivo no encontrado")

    ext = abs_path.suffix.lstrip(".").lower()
    media_type = _PREVIEW_MIME.get(ext, "application/octet-stream")

    return FileResponse(str(abs_path), media_type=media_type, filename=abs_path.name)
