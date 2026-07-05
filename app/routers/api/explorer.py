"""Router para el explorador de archivos PDF integrado en la UI."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import shutil
import zipfile
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.paths import audit_data_root, safe_join
from app.schemas.explorer import (
    BatchDeleteResult,
    CopyRequest,
    DeleteBatchRequest,
    DeleteRequest,
    FileNode,
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

router = APIRouter(prefix="/explorer", tags=["explorer"])
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de seguridad (sandbox)
# ──────────────────────────────────────────────────────────────────────────────


async def _resolve_sandbox(institution_id: int, period_id: int, db: AsyncSession) -> Path:
    """Devuelve la ruta sandbox: audit_data_root / institution.name / period.period_label."""
    institution = await db.get(Institution, institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")

    period = await db.get(AuditPeriod, period_id)
    if not period or period.institution_id != institution_id:
        raise HTTPException(404, "Período no encontrado")

    sandbox = (audit_data_root / institution.name / period.period_label).resolve()
    return sandbox


def _safe_resolve(sandbox: Path, rel: str) -> Path:
    """Resuelve una ruta relativa dentro del sandbox. Lanza 400 si escapa del sandbox."""
    try:
        return safe_join(sandbox, rel.lstrip("/\\"))
    except ValueError:
        raise HTTPException(400, "Ruta fuera del directorio permitido")


def _validate_entry_name(name: str) -> None:
    """Valida que un nombre de archivo/carpeta no contenga separadores de ruta ni esté vacío."""
    if not name or not name.strip():
        raise HTTPException(400, "El nombre no puede estar vacío")
    if "/" in name or "\\" in name:
        raise HTTPException(400, "El nombre no puede contener separadores de ruta")


def _delete_path(target: Path, sandbox: Path) -> str:
    """Elimina un archivo o carpeta del sandbox. Devuelve mensaje descriptivo."""
    if target == sandbox:
        raise HTTPException(400, "No se puede eliminar la raíz del período")
    if not target.exists():
        raise HTTPException(404, "Archivo o carpeta no encontrado")
    try:
        if target.is_file():
            target.unlink()
            return f'"{target.name}" eliminado'
        else:
            shutil.rmtree(target)
            return f'Carpeta "{target.name}" eliminada'
    except OSError as e:
        logger.error("Error al eliminar %s: %s", target, e)
        raise HTTPException(500, "Error al eliminar el archivo o carpeta")


# ──────────────────────────────────────────────────────────────────────────────
# Listar directorio
# ──────────────────────────────────────────────────────────────────────────────


@router.get("/list", response_model=ListResponse)
async def list_directory(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    root: str = Query("DRIVE"),
    path: str = Query(""),
    db: AsyncSession = Depends(get_db),
):
    """Lista carpetas y archivos PDF en el directorio indicado."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)

    rel = f"{root}/{path}".strip("/")
    target = _safe_resolve(sandbox, rel)

    if not target.exists():
        return ListResponse(entries=[], current_path=rel)
    if not target.is_dir():
        raise HTTPException(400, "La ruta no es un directorio")

    entries: list[FileNode] = []
    try:
        for item in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if item.is_dir():
                entries.append(
                    FileNode(
                        name=item.name,
                        path=str(item.relative_to(sandbox)).replace("\\", "/"),
                        is_dir=True,
                        file_count=sum(1 for f in item.rglob("*") if f.is_file()),
                    )
                )
            elif item.suffix.lower() == ".pdf":
                entries.append(
                    FileNode(
                        name=item.name,
                        path=str(item.relative_to(sandbox)).replace("\\", "/"),
                        is_dir=False,
                        size=item.stat().st_size,
                    )
                )
    except PermissionError as e:
        raise HTTPException(403, f"Sin permiso para leer el directorio: {e}")

    return ListResponse(entries=entries, current_path=rel)


# ──────────────────────────────────────────────────────────────────────────────
# Servir PDF
# ──────────────────────────────────────────────────────────────────────────────


@router.get("/file")
async def serve_file(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Sirve el PDF como StreamingResponse para previsualización."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    target = _safe_resolve(sandbox, path)

    if not target.exists() or not target.is_file():
        raise HTTPException(404, "Archivo no encontrado")
    if target.suffix.lower() != ".pdf":
        raise HTTPException(400, "Solo se pueden servir archivos PDF")

    async def _iter():
        async with aiofiles.open(target, "rb") as f:
            while chunk := await f.read(65536):
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{target.name}"'},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Miniaturas
# ──────────────────────────────────────────────────────────────────────────────


@router.get("/thumbnails")
async def get_thumbnails(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Renderiza cada página como PNG base64 para la UI de reordenamiento."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    target = _safe_resolve(sandbox, path)

    if not target.exists() or target.suffix.lower() != ".pdf":
        raise HTTPException(404, "Archivo PDF no encontrado")

    def _render() -> list[str]:
        import fitz  # PyMuPDF

        doc = fitz.open(str(target))
        result = []
        matrix = fitz.Matrix(0.5, 0.5)
        for page in doc:
            pix = page.get_pixmap(matrix=matrix)
            png_bytes = pix.tobytes("png")
            result.append(base64.b64encode(png_bytes).decode())
        doc.close()
        return result

    thumbnails = await asyncio.get_running_loop().run_in_executor(None, _render)
    return {"thumbnails": thumbnails, "count": len(thumbnails)}


# ──────────────────────────────────────────────────────────────────────────────
# Renombrar
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/rename", response_model=OperationResult)
async def rename_entry(body: RenameRequest, db: AsyncSession = Depends(get_db)):
    """Renombra un archivo o carpeta."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    src = _safe_resolve(sandbox, body.path)

    if not src.exists():
        raise HTTPException(404, "Archivo o carpeta no encontrado")

    # Validar que new_name no contenga separadores de ruta
    if "/" in body.new_name or "\\" in body.new_name:
        raise HTTPException(400, "El nuevo nombre no puede contener separadores de ruta")

    dst = src.parent / body.new_name
    _safe_resolve(sandbox, str(dst.relative_to(sandbox)))  # verificar sandbox

    try:
        src.rename(dst)
    except OSError as e:
        logger.error("Error al renombrar %s a %s: %s", src, dst, e)
        raise HTTPException(500, "Error al renombrar el archivo o carpeta")

    return OperationResult(ok=True, message=f"Renombrado a {body.new_name}")


# ──────────────────────────────────────────────────────────────────────────────
# Mover
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/move", response_model=OperationResult)
async def move_entry(body: MoveRequest, db: AsyncSession = Depends(get_db)):
    """Mueve un archivo o carpeta a otra carpeta destino."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    src = _safe_resolve(sandbox, body.src)
    dst_folder = _safe_resolve(sandbox, body.dst_folder)

    if not src.exists():
        raise HTTPException(404, "Origen no encontrado")
    if not dst_folder.is_dir():
        raise HTTPException(400, "Destino no es un directorio")

    dst = dst_folder / src.name
    try:
        src.rename(dst)
    except OSError as e:
        logger.error("Error al mover %s a %s: %s", src, dst, e)
        raise HTTPException(500, "Error al mover el archivo o carpeta")

    return OperationResult(ok=True, message=f"Movido a {dst_folder.name}/{src.name}")


# ──────────────────────────────────────────────────────────────────────────────
# Unir PDFs (merge)
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/merge", response_model=OperationResult)
async def merge_pdfs(body: MergeRequest, db: AsyncSession = Depends(get_db)):
    """Une múltiples PDFs en uno solo."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)

    if len(body.paths) < 2:
        raise HTTPException(400, "Se necesitan al menos 2 PDFs para unir")

    resolved_paths = [_safe_resolve(sandbox, p) for p in body.paths]
    for p in resolved_paths:
        if not p.exists() or p.suffix.lower() != ".pdf":
            raise HTTPException(404, f"PDF no encontrado: {p.name}")

    # Carpeta de salida: la del primer PDF
    output_name = body.output_name if body.output_name.endswith(".pdf") else body.output_name + ".pdf"
    output_path = resolved_paths[0].parent / output_name
    _safe_resolve(sandbox, str(output_path.relative_to(sandbox)))  # verificar sandbox

    def _merge():
        import fitz

        merged = fitz.open()
        for p in resolved_paths:
            doc = fitz.open(str(p))
            merged.insert_pdf(doc)
            doc.close()
        merged.save(str(output_path))
        merged.close()

    await asyncio.get_running_loop().run_in_executor(None, _merge)
    return OperationResult(ok=True, message=f"PDFs unidos en {output_name}")


# ──────────────────────────────────────────────────────────────────────────────
# Dividir PDF (split)
# ──────────────────────────────────────────────────────────────────────────────


def _parse_ranges(ranges_str: str, total_pages: int) -> list[list[int]]:
    """Parsea un string de rangos como '1-3, 5' (1-based) a listas de índices 0-based."""
    groups: list[list[int]] = []
    for token in ranges_str.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            parts = token.split("-")
            start = int(parts[0].strip()) - 1
            end = int(parts[1].strip()) - 1
            groups.append(list(range(max(0, start), min(total_pages - 1, end) + 1)))
        else:
            idx = int(token) - 1
            if 0 <= idx < total_pages:
                groups.append([idx])
    return groups


def _compute_split_groups(ranges: str | None, total_pages: int) -> list[list[int]]:
    """Grupos de páginas (0-based) para dividir: por rangos si se dan, si no una página por grupo."""
    if ranges:
        return _parse_ranges(ranges, total_pages)
    return [[i] for i in range(total_pages)]


@router.get("/split-preview", response_model=SplitPreviewResult)
async def preview_split(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    path: str = Query(...),
    ranges: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Calcula los grupos de páginas que resultarían de dividir, sin escribir archivos."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    src = _safe_resolve(sandbox, path)

    if not src.exists() or src.suffix.lower() != ".pdf":
        raise HTTPException(404, "PDF no encontrado")

    def _preview() -> list[list[int]]:
        import fitz

        doc = fitz.open(str(src))
        total = doc.page_count
        doc.close()
        return _compute_split_groups(ranges, total)

    try:
        groups = await asyncio.get_running_loop().run_in_executor(None, _preview)
    except (ValueError, IndexError) as e:
        raise HTTPException(400, f"Rango de páginas inválido: {e}")

    return SplitPreviewResult(groups=groups)


@router.post("/split", response_model=OperationResult)
async def split_pdf(body: SplitRequest, db: AsyncSession = Depends(get_db)):
    """Divide un PDF por rangos o página a página."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    src = _safe_resolve(sandbox, body.path)

    if not src.exists() or src.suffix.lower() != ".pdf":
        raise HTTPException(404, "PDF no encontrado")

    def _split():
        import fitz

        doc = fitz.open(str(src))
        total = doc.page_count
        stem = src.stem

        groups = _compute_split_groups(body.ranges, total)

        output_dir = src.parent
        created = 0
        for i, pages in enumerate(groups, start=1):
            sub = fitz.open()
            sub.insert_pdf(doc, from_page=pages[0], to_page=pages[-1])
            # Para rangos no contiguos, insertar página por página
            if len(pages) > 1 and pages != list(range(pages[0], pages[-1] + 1)):
                sub = fitz.open()
                for pg in pages:
                    sub.insert_pdf(doc, from_page=pg, to_page=pg)
            out_name = f"{stem}_parte{i:03d}.pdf"
            sub.save(str(output_dir / out_name))
            sub.close()
            created += 1
        doc.close()
        return created

    try:
        n = await asyncio.get_running_loop().run_in_executor(None, _split)
    except (ValueError, IndexError) as e:
        raise HTTPException(400, f"Rango de páginas inválido: {e}")

    return OperationResult(ok=True, message=f"PDF dividido en {n} archivo(s)")


# ──────────────────────────────────────────────────────────────────────────────
# Reordenar páginas
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/reorder", response_model=OperationResult)
async def reorder_pages(body: ReorderRequest, db: AsyncSession = Depends(get_db)):
    """Reordena las páginas de un PDF y elimina las que no estén en `page_order` (in-place)."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    src = _safe_resolve(sandbox, body.path)

    if not src.exists() or src.suffix.lower() != ".pdf":
        raise HTTPException(404, "PDF no encontrado")

    def _reorder() -> int:
        import fitz

        doc = fitz.open(str(src))
        page_count: int = doc.page_count
        order = list(body.page_order)

        if not order:
            doc.close()
            raise ValueError("Debe quedar al menos 1 página")
        if len(order) != len(set(order)):
            doc.close()
            raise ValueError("No puede haber páginas repetidas")
        for idx in order:
            if idx < 0 or idx >= page_count:
                doc.close()
                raise ValueError(f"Índice de página inválido: {idx} (rango válido 0–{page_count - 1})")

        doc.select(order)
        # tobytes() evita guardar sobre el mismo archivo abierto (problema en Windows)
        out_bytes = doc.tobytes(garbage=4, deflate=True)
        doc.close()
        src.write_bytes(out_bytes)
        return page_count - len(order)

    try:
        deleted_count = await asyncio.get_running_loop().run_in_executor(None, _reorder)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, f"Error al reordenar: {e}")

    if deleted_count:
        message = f"Se guardaron {len(body.page_order)} página(s) ({deleted_count} eliminada(s))"
    else:
        message = "Páginas reordenadas correctamente"
    return OperationResult(ok=True, message=message)


# ──────────────────────────────────────────────────────────────────────────────
# Buscar archivos / carpetas por nombre (recursivo)
# ──────────────────────────────────────────────────────────────────────────────

_SEARCH_MAX_RESULTS = 100


@router.get("/search", response_model=ListResponse)
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
    base = _safe_resolve(sandbox, rel)

    if not base.is_dir():
        return ListResponse(entries=[], current_path=rel)

    q_lower = q.lower()
    entries: list[FileNode] = []

    for item in sorted(base.rglob("*"), key=lambda x: (not x.is_dir(), x.name.lower())):
        if q_lower not in item.name.lower():
            continue
        if not item.is_dir() and item.suffix.lower() != ".pdf":
            continue
        entries.append(
            FileNode(
                name=item.name,
                path=str(item.relative_to(sandbox)).replace("\\", "/"),
                is_dir=item.is_dir(),
                size=item.stat().st_size if item.is_file() else None,
            )
        )
        if len(entries) >= _SEARCH_MAX_RESULTS:
            break

    return ListResponse(entries=entries, current_path=rel)


# ──────────────────────────────────────────────────────────────────────────────
# Copiar archivo o carpeta
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/copy", response_model=OperationResult)
async def copy_entry(body: CopyRequest, db: AsyncSession = Depends(get_db)):
    """Copia un archivo o carpeta a otra carpeta destino."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    src = _safe_resolve(sandbox, body.src)
    dst_folder = _safe_resolve(sandbox, body.dst_folder)

    if not src.exists():
        raise HTTPException(404, "Origen no encontrado")
    if not dst_folder.is_dir():
        raise HTTPException(400, "Destino no es un directorio")
    if dst_folder == src.parent:
        raise HTTPException(400, "El origen ya se encuentra en esa carpeta")
    # Prevent copying a folder into one of its own descendants
    if src.is_dir() and (dst_folder == src or dst_folder.is_relative_to(src)):
        raise HTTPException(400, "No puedes copiar una carpeta dentro de sí misma")

    dst = dst_folder / src.name
    try:
        if src.is_file():
            shutil.copy2(src, dst)
        else:
            shutil.copytree(src, dst, dirs_exist_ok=True)
    except OSError as e:
        logger.error("Error al copiar %s a %s: %s", src, dst, e)
        raise HTTPException(500, "Error al copiar el archivo o carpeta")

    return OperationResult(ok=True, message=f'"{src.name}" copiado a {dst_folder.name}/')


# ──────────────────────────────────────────────────────────────────────────────
# Eliminar archivo o carpeta
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/delete", response_model=OperationResult)
async def delete_entry(body: DeleteRequest, db: AsyncSession = Depends(get_db)):
    """Elimina un archivo o carpeta (con todo su contenido si es carpeta)."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    target = _safe_resolve(sandbox, body.path)
    message = _delete_path(target, sandbox)
    return OperationResult(ok=True, message=message)


# ──────────────────────────────────────────────────────────────────────────────
# Eliminar en lote
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/delete-batch", response_model=BatchDeleteResult)
async def delete_batch(body: DeleteBatchRequest, db: AsyncSession = Depends(get_db)):
    """Elimina múltiples archivos o carpetas en una sola operación."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)

    deleted: list[str] = []
    errors: list[str] = []

    for rel_path in body.paths:
        try:
            target = _safe_resolve(sandbox, rel_path)
            _delete_path(target, sandbox)
            deleted.append(rel_path)
        except HTTPException as exc:
            errors.append(f"{rel_path}: {exc.detail}")

    return BatchDeleteResult(deleted=deleted, errors=errors)


# ──────────────────────────────────────────────────────────────────────────────
# Crear carpeta
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/mkdir", response_model=OperationResult)
async def make_directory(body: MkdirRequest, db: AsyncSession = Depends(get_db)):
    """Crea una nueva carpeta dentro del sandbox."""
    sandbox = await _resolve_sandbox(body.institution_id, body.period_id, db)
    _validate_entry_name(body.name)

    parent = _safe_resolve(sandbox, body.path) if body.path else sandbox
    if not parent.is_dir():
        raise HTTPException(400, "La carpeta padre no existe")

    new_dir = parent / body.name
    _safe_resolve(sandbox, str(new_dir.relative_to(sandbox)))  # confirmar sandbox

    if new_dir.exists():
        raise HTTPException(400, f'Ya existe una carpeta con el nombre "{body.name}"')

    try:
        new_dir.mkdir(parents=False)
    except OSError as e:
        logger.error("Error al crear la carpeta %s: %s", new_dir, e)
        raise HTTPException(500, "Error al crear la carpeta")

    rel = str(new_dir.relative_to(sandbox)).replace("\\", "/")
    return OperationResult(ok=True, message=f'Carpeta "{body.name}" creada en {rel}')


# ──────────────────────────────────────────────────────────────────────────────
# Subir archivos (PDFs)
# ──────────────────────────────────────────────────────────────────────────────

_MAX_UPLOAD_SIZE = 200 * 1024 * 1024  # 200 MB per file


@router.post("/upload", response_model=UploadResult)
async def upload_files(
    institution_id: int = Form(...),
    period_id: int = Form(...),
    path: str = Form(""),
    relative_paths_json: str = Form("[]"),
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Sube uno o más PDFs al directorio indicado.

    `relative_paths_json` es un array JSON de rutas relativas (para preservar
    la estructura de subcarpetas cuando se sube una carpeta completa).
    Si está vacío o no coincide en longitud con `files`, se usa solo el nombre
    de cada archivo.
    """
    sandbox = await _resolve_sandbox(institution_id, period_id, db)

    # Carpeta destino base
    dest_base = _safe_resolve(sandbox, path) if path else sandbox
    if not dest_base.is_dir():
        raise HTTPException(400, "La carpeta destino no existe")

    # Parsear rutas relativas (presentes solo en upload de carpeta completa)
    try:
        relative_paths: list[str] = json.loads(relative_paths_json)
        if not isinstance(relative_paths, list):
            relative_paths = []
    except (json.JSONDecodeError, ValueError):
        relative_paths = []

    is_folder_upload = len(relative_paths) == len(files) and any(relative_paths)

    # Para upload de carpeta: verificar conflicto de nombre antes de escribir nada
    if is_folder_upload:
        root_name = Path(relative_paths[0]).parts[0]
        root_dest = dest_base / root_name
        _safe_resolve(sandbox, str(root_dest.relative_to(sandbox)))  # validar sandbox
        if root_dest.exists():
            raise HTTPException(
                409,
                f'Ya existe una carpeta llamada "{root_name}" en este directorio. '
                "Renómbrala o elimínala antes de subir.",
            )

    uploaded: list[str] = []
    skipped: list[str] = []
    non_pdf_count: int = 0

    for i, upload in enumerate(files):
        filename = upload.filename or f"archivo_{i}"

        # Determinar ruta destino
        if is_folder_upload and relative_paths[i]:
            # Preservar estructura completa incluyendo la carpeta raíz
            dest_file = dest_base / Path(relative_paths[i])
        else:
            dest_file = dest_base / filename

        # Solo aceptar PDFs — contar no-PDFs por separado para advertencia
        if dest_file.suffix.lower() != ".pdf":
            non_pdf_count += 1
            skipped.append(filename)
            continue

        # Validar sandbox
        try:
            _safe_resolve(sandbox, str(dest_file.relative_to(sandbox)))
        except HTTPException:
            skipped.append(filename)
            continue

        # Crear directorios intermedios (subcarpetas anidadas)
        dest_file.parent.mkdir(parents=True, exist_ok=True)

        # Escribir
        try:
            content = await upload.read(_MAX_UPLOAD_SIZE + 1)
            if len(content) > _MAX_UPLOAD_SIZE:
                skipped.append(f"{filename}: excede el tamaño máximo permitido (200 MB)")
                continue
            async with aiofiles.open(dest_file, "wb") as f:
                await f.write(content)
            uploaded.append(str(dest_file.relative_to(sandbox)).replace("\\", "/"))
        except OSError as e:
            skipped.append(f"{filename}: {e}")

    return UploadResult(uploaded=uploaded, skipped=skipped, non_pdf_count=non_pdf_count)


# ──────────────────────────────────────────────────────────────────────────────
# Descarga de archivos individuales y ZIP
# ──────────────────────────────────────────────────────────────────────────────


@router.get("/download-file")
async def download_file(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    rel_path: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Descarga un archivo individual como attachment."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)
    abs_path = _safe_resolve(sandbox, rel_path)

    if not abs_path.is_file():
        raise HTTPException(404, "Archivo no encontrado")

    async def _iter():
        async with aiofiles.open(abs_path, "rb") as f:
            while chunk := await f.read(65536):
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{abs_path.name}"'},
    )


@router.get("/download-zip")
async def download_zip(
    institution_id: int = Query(...),
    period_id: int = Query(...),
    rel_paths: list[str] = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Empaqueta una lista de rutas (archivos y/o carpetas) en un ZIP en memoria."""
    sandbox = await _resolve_sandbox(institution_id, period_id, db)

    def _build() -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for rel in rel_paths:
                abs_p = _safe_resolve(sandbox, rel)
                if abs_p.is_file():
                    zf.write(abs_p, abs_p.name)
                elif abs_p.is_dir():
                    for f in abs_p.rglob("*"):
                        if f.is_file():
                            zf.write(f, f.relative_to(abs_p.parent))
        return buf.getvalue()

    loop = asyncio.get_running_loop()
    zip_bytes = await loop.run_in_executor(None, _build)

    zip_name = (Path(rel_paths[0]).name + ".zip") if len(rel_paths) == 1 else "seleccion.zip"

    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Ocultar consola Windows
# ──────────────────────────────────────────────────────────────────────────────


@router.post("/console-hide", response_model=OperationResult)
async def console_hide():
    """Oculta la ventana de consola de Windows."""
    try:
        import ctypes

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
        return OperationResult(ok=True, message="Consola ocultada")
    except Exception as e:
        return OperationResult(ok=False, message=f"No se pudo ocultar la consola: {e}")
