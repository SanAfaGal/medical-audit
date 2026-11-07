"""Router para el explorador de archivos PDF integrado en la UI.

Solo traduce HTTP: resuelve el sandbox del período, delega en ``app/services/explorer`` (trabajo de disco en un
hilo, fuera del event loop) y convierte ``ExplorerError`` en ``HTTPException``.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import ParamSpec, TypeVar

import aiofiles
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.routers.deps import load_period_scope
from app.routers.responses import content_disposition, temp_file_response
from app.schemas.explorer import (
    BatchDeleteResult,
    CopyRequest,
    DeleteBatchRequest,
    DeleteRequest,
    ListResponse,
    MergeRequest,
    MkdirRequest,
    MoveRequest,
    OperationResult,
    ReorderRequest,
    RenameRequest,
    SplitPreviewResult,
    SplitRequest,
    UploadResult,
)
from app.services.archive import build_zip_file
from app.services.explorer import ExplorerError
from app.services.explorer import files as file_ops
from app.services.explorer import pdf as pdf_ops
from app.services.explorer.sandbox import resolve_in

router = APIRouter(prefix="/explorer", tags=["explorer"])
logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _http_errors(endpoint: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Answer an ``ExplorerError`` raised by a service with the HTTP error it describes."""

    @functools.wraps(endpoint)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await endpoint(*args, **kwargs)
        except ExplorerError as exc:
            raise HTTPException(exc.status_code, exc.detail) from exc

    return wrapper


async def _resolve_sandbox(institution_id: int, period_id: int, db: AsyncSession) -> Path:
    """Devuelve la ruta sandbox: audit_data_root / institution.name / period.period_label."""
    scope = await load_period_scope(db, institution_id, period_id)
    return scope.paths.base.resolve()


@router.get("/list", response_model=ListResponse)
@_http_errors
async def list_directory(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    root: str = Query("DRIVE"),
    path: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """Lista carpetas y archivos en el directorio indicado (los no-PDF se marcan con is_pdf=False)."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    rel = f"{root}/{path}".strip("/")
    target = resolve_in(sandbox, rel)

    if not target.exists():
        return ListResponse(entries=[], current_path=rel)
    if not target.is_dir():
        raise ExplorerError(400, "La ruta no es un directorio")

    entries = await asyncio.to_thread(file_ops.list_nodes, sandbox, target)  # a walk can touch thousands of files
    return ListResponse(entries=entries, current_path=rel)


@router.get("/search", response_model=ListResponse)
@_http_errors
async def search_entries(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    root: str = Query("STAGE"),
    path: str = Query(""),
    q: str = Query(..., min_length=1),
    db: AsyncSession = Depends(get_db),
):
    """Búsqueda recursiva de archivos y carpetas cuyo nombre contiene *q*."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    rel = f"{root}/{path}".strip("/") if path else root
    base = resolve_in(sandbox, rel)

    if not base.is_dir():
        return ListResponse(entries=[], current_path=rel)

    entries = await asyncio.to_thread(file_ops.search_nodes, sandbox, base, q)
    return ListResponse(entries=entries, current_path=rel)


@router.get("/file")
@_http_errors
async def serve_file(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Sirve el PDF como StreamingResponse para previsualización."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    target = resolve_in(sandbox, path)

    if not target.exists() or not target.is_file():
        raise ExplorerError(404, "Archivo no encontrado")
    if target.suffix.lower() != ".pdf":
        raise ExplorerError(400, "Solo se pueden servir archivos PDF")

    return StreamingResponse(
        _stream(target),
        media_type="application/pdf",
        headers={"Content-Disposition": content_disposition(target.name, inline=True)},
    )


async def _stream(path: Path):
    async with aiofiles.open(path, "rb") as f:
        while chunk := await f.read(65536):
            yield chunk


@router.get("/thumbnails")
@_http_errors
async def get_thumbnails(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Renderiza cada página como PNG base64 para la UI de reordenamiento."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    thumbnails = await asyncio.to_thread(pdf_ops.render_thumbnails, sandbox, path)
    return {"thumbnails": thumbnails, "count": len(thumbnails)}


@router.post("/rename", response_model=OperationResult)
@_http_errors
async def rename_entry(body: RenameRequest, db: AsyncSession = Depends(get_db)):
    """Renombra un archivo o carpeta."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    message = await asyncio.to_thread(file_ops.rename, sandbox, body.path, body.new_name)
    return OperationResult(ok=True, message=message)


@router.post("/move", response_model=OperationResult)
@_http_errors
async def move_entry(body: MoveRequest, db: AsyncSession = Depends(get_db)):
    """Mueve un archivo o carpeta a otra carpeta destino."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    message = await asyncio.to_thread(file_ops.move, sandbox, body.src, body.dst_folder)
    return OperationResult(ok=True, message=message)


@router.post("/copy", response_model=OperationResult)
@_http_errors
async def copy_entry(body: CopyRequest, db: AsyncSession = Depends(get_db)):
    """Copia un archivo o carpeta a otra carpeta destino."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    message = await asyncio.to_thread(file_ops.copy, sandbox, body.src, body.dst_folder)
    return OperationResult(ok=True, message=message)


@router.post("/delete", response_model=OperationResult)
@_http_errors
async def delete_entry(body: DeleteRequest, db: AsyncSession = Depends(get_db)):
    """Elimina un archivo o carpeta (con todo su contenido si es carpeta)."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    message = await asyncio.to_thread(file_ops.delete_one, sandbox, body.path)
    return OperationResult(ok=True, message=message)


@router.post("/delete-batch", response_model=BatchDeleteResult)
@_http_errors
async def delete_batch(body: DeleteBatchRequest, db: AsyncSession = Depends(get_db)):
    """Elimina múltiples archivos o carpetas en una sola operación."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    return await asyncio.to_thread(file_ops.delete_many, sandbox, body.paths)


@router.post("/mkdir", response_model=OperationResult)
@_http_errors
async def make_directory(body: MkdirRequest, db: AsyncSession = Depends(get_db)):
    """Crea una nueva carpeta dentro del sandbox."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    message = await asyncio.to_thread(file_ops.make_directory, sandbox, body.path, body.name)
    return OperationResult(ok=True, message=message)


@router.post("/merge", response_model=OperationResult)
@_http_errors
async def merge_pdfs(body: MergeRequest, db: AsyncSession = Depends(get_db)):
    """Une múltiples PDFs en uno solo."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    name = await asyncio.to_thread(pdf_ops.merge_files, sandbox, body.paths, body.output_name)
    return OperationResult(ok=True, message=f"PDFs unidos en {name}")


@router.get("/split-preview", response_model=SplitPreviewResult)
@_http_errors
async def preview_split(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    ranges: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Calcula los grupos de páginas que resultarían de dividir, sin escribir archivos."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    groups = await asyncio.to_thread(pdf_ops.preview_split, sandbox, path, ranges)
    return SplitPreviewResult(groups=groups)


@router.post("/split", response_model=OperationResult)
@_http_errors
async def split_pdf(body: SplitRequest, db: AsyncSession = Depends(get_db)):
    """Divide un PDF por rangos o página a página."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    created = await asyncio.to_thread(pdf_ops.split, sandbox, body.path, body.ranges)
    return OperationResult(ok=True, message=f"PDF dividido en {created} archivo(s)")


@router.post("/reorder", response_model=OperationResult)
@_http_errors
async def reorder_pages(body: ReorderRequest, db: AsyncSession = Depends(get_db)):
    """Reordena las páginas de un PDF y elimina las que no estén en `page_order` (in-place)."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    dropped = await asyncio.to_thread(pdf_ops.reorder, sandbox, body.path, body.page_order)
    if dropped:
        message = f"Se guardaron {len(body.page_order)} página(s) ({dropped} eliminada(s))"
    else:
        message = "Páginas reordenadas correctamente"
    return OperationResult(ok=True, message=message)


@router.post("/upload", response_model=UploadResult)
@_http_errors
async def upload_files(
    institution_id: int = Form(...),
    period_id: int = Form(...),
    path: str = Form(""),
    relative_paths_json: str = Form("[]"),
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Sube uno o más PDFs al directorio indicado (ver ``file_ops.save_uploads`` para las reglas)."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    return await file_ops.save_uploads(sandbox, path, files, relative_paths_json, settings.max_upload_bytes)


@router.get("/download-file")
@_http_errors
async def download_file(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    rel_path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Descarga un archivo individual como attachment."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    target = resolve_in(sandbox, rel_path)
    if not target.is_file():
        raise ExplorerError(404, "Archivo no encontrado")

    return StreamingResponse(
        _stream(target),
        media_type="application/octet-stream",
        headers={"Content-Disposition": content_disposition(target.name)},
    )


@router.get("/download-zip")
@_http_errors
async def download_zip(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    rel_paths: list[str] = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Empaqueta una lista de rutas (archivos y/o carpetas) en un ZIP temporal en disco."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    # Temporary ZIP on disk (streamed file by file): packages of many GB never sit in memory.
    zip_path = await asyncio.to_thread(build_zip_file, file_ops.zip_entries(sandbox, rel_paths))
    zip_name = (Path(rel_paths[0]).name + ".zip") if len(rel_paths) == 1 else "seleccion.zip"
    return temp_file_response(zip_path, zip_name, "application/zip")


@router.post("/console-hide", response_model=OperationResult)
async def console_hide():
    """Oculta la ventana de consola de Windows."""
    try:
        import ctypes

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
        return OperationResult(ok=True, message="Consola ocultada")
    except Exception as e:  # noqa: BLE001 - best-effort Windows-only call; report, never crash
        return OperationResult(ok=False, message=f"No se pudo ocultar la consola: {e}")
