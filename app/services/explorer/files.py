"""Folder and file actions of the explorer. Blocking filesystem work: call from a worker thread."""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Protocol

import aiofiles

from app.schemas.explorer import BatchDeleteResult, FileNode, UploadResult
from app.services.explorer.errors import ExplorerError
from app.services.explorer.sandbox import relative, resolve_in, validate_entry_name

logger = logging.getLogger(__name__)

SEARCH_MAX_RESULTS = 100
_UPLOAD_CHUNK = 1024 * 1024


class IncomingFile(Protocol):
    """What an uploaded file must offer (Starlette's ``UploadFile`` does)."""

    filename: str | None

    async def read(self, size: int = -1, /) -> bytes: ...


def list_nodes(sandbox: Path, target: Path) -> list[FileNode]:
    """Folders first, then files, by name; non-PDF files are flagged ``is_pdf=False``."""
    found: list[FileNode] = []
    try:
        for item in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if item.is_dir():
                found.append(
                    FileNode(
                        name=item.name,
                        path=relative(sandbox, item),
                        is_dir=True,
                        file_count=sum(1 for f in item.rglob("*") if f.is_file()),
                    )
                )
            elif item.is_file():
                found.append(
                    FileNode(
                        name=item.name,
                        path=relative(sandbox, item),
                        is_dir=False,
                        is_pdf=item.suffix.lower() == ".pdf",
                        size=item.stat().st_size,
                    )
                )
    except PermissionError as exc:
        raise ExplorerError(403, f"Sin permiso para leer el directorio: {exc}") from exc
    return found


def search_nodes(sandbox: Path, base: Path, query: str, limit: int = SEARCH_MAX_RESULTS) -> list[FileNode]:
    """Recursive, case-insensitive name search under ``base`` (folders first), capped at ``limit`` results."""
    needle = query.lower()
    found: list[FileNode] = []
    for item in sorted(base.rglob("*"), key=lambda x: (not x.is_dir(), x.name.lower())):
        if needle not in item.name.lower():
            continue
        if not item.is_dir() and not item.is_file():
            continue
        found.append(
            FileNode(
                name=item.name,
                path=relative(sandbox, item),
                is_dir=item.is_dir(),
                is_pdf=item.is_dir() or item.suffix.lower() == ".pdf",
                size=item.stat().st_size if item.is_file() else None,
            )
        )
        if len(found) >= limit:
            break
    return found


def rename(sandbox: Path, rel: str, new_name: str) -> str:
    src = resolve_in(sandbox, rel)
    if not src.exists():
        raise ExplorerError(404, "Archivo o carpeta no encontrado")
    validate_entry_name(new_name)
    if src.is_file() and Path(new_name).suffix.lower() != src.suffix.lower():
        raise ExplorerError(400, "No se puede cambiar la extensión del archivo")

    dst = src.parent / new_name
    resolve_in(sandbox, relative(sandbox, dst))  # the new name must stay inside the sandbox too
    try:
        src.rename(dst)
    except OSError as exc:
        logger.error("Error al renombrar %s a %s: %s", src, dst, exc)
        raise ExplorerError(500, "Error al renombrar el archivo o carpeta") from exc
    return f"Renombrado a {new_name}"


def move(sandbox: Path, src_rel: str, dst_folder_rel: str) -> str:
    src = resolve_in(sandbox, src_rel)
    dst_folder = resolve_in(sandbox, dst_folder_rel)
    if not src.exists():
        raise ExplorerError(404, "Origen no encontrado")
    if not dst_folder.is_dir():
        raise ExplorerError(400, "Destino no es un directorio")

    dst = dst_folder / src.name
    try:
        src.rename(dst)
    except OSError as exc:
        logger.error("Error al mover %s a %s: %s", src, dst, exc)
        raise ExplorerError(500, "Error al mover el archivo o carpeta") from exc
    return f"Movido a {dst_folder.name}/{src.name}"


def copy(sandbox: Path, src_rel: str, dst_folder_rel: str) -> str:
    src = resolve_in(sandbox, src_rel)
    dst_folder = resolve_in(sandbox, dst_folder_rel)
    if not src.exists():
        raise ExplorerError(404, "Origen no encontrado")
    if not dst_folder.is_dir():
        raise ExplorerError(400, "Destino no es un directorio")
    if dst_folder == src.parent:
        raise ExplorerError(400, "El origen ya se encuentra en esa carpeta")
    if src.is_dir() and (dst_folder == src or dst_folder.is_relative_to(src)):
        raise ExplorerError(400, "No puedes copiar una carpeta dentro de sí misma")

    dst = dst_folder / src.name
    try:
        if src.is_file():
            shutil.copy2(src, dst)
        else:
            shutil.copytree(src, dst, dirs_exist_ok=True)
    except OSError as exc:
        logger.error("Error al copiar %s a %s: %s", src, dst, exc)
        raise ExplorerError(500, "Error al copiar el archivo o carpeta") from exc
    return f'"{src.name}" copiado a {dst_folder.name}/'


def delete_path(target: Path, sandbox: Path) -> str:
    """Delete a file or a folder with everything in it; the period root itself can never be deleted."""
    if target == sandbox:
        raise ExplorerError(400, "No se puede eliminar la raíz del período")
    if not target.exists():
        raise ExplorerError(404, "Archivo o carpeta no encontrado")
    try:
        if target.is_file():
            target.unlink()
            return f'"{target.name}" eliminado'
        shutil.rmtree(target)
        return f'Carpeta "{target.name}" eliminada'
    except OSError as exc:
        logger.error("Error al eliminar %s: %s", target, exc)
        raise ExplorerError(500, "Error al eliminar el archivo o carpeta") from exc


def delete_one(sandbox: Path, rel: str) -> str:
    return delete_path(resolve_in(sandbox, rel), sandbox)


def delete_many(sandbox: Path, rel_paths: Sequence[str]) -> BatchDeleteResult:
    """Delete every path it can; the ones that fail are reported, not raised."""
    deleted: list[str] = []
    errors: list[str] = []
    for rel in rel_paths:
        try:
            delete_one(sandbox, rel)
            deleted.append(rel)
        except ExplorerError as exc:
            errors.append(f"{rel}: {exc.detail}")
    return BatchDeleteResult(deleted=deleted, errors=errors)


def make_directory(sandbox: Path, parent_rel: str, name: str) -> str:
    validate_entry_name(name)
    parent = resolve_in(sandbox, parent_rel) if parent_rel else sandbox
    if not parent.is_dir():
        raise ExplorerError(400, "La carpeta padre no existe")

    new_dir = parent / name
    resolve_in(sandbox, relative(sandbox, new_dir))
    if new_dir.exists():
        raise ExplorerError(400, f'Ya existe una carpeta con el nombre "{name}"')
    try:
        new_dir.mkdir(parents=False)
    except OSError as exc:
        logger.error("Error al crear la carpeta %s: %s", new_dir, exc)
        raise ExplorerError(500, "Error al crear la carpeta") from exc
    return f'Carpeta "{name}" creada en {relative(sandbox, new_dir)}'


def zip_entries(sandbox: Path, rel_paths: Sequence[str]) -> Iterator[tuple[Path, str]]:
    """``(file, name inside the zip)`` for every selected file and for every file under a selected folder."""
    for rel in rel_paths:
        path = resolve_in(sandbox, rel)
        if path.is_file():
            yield path, path.name
        elif path.is_dir():
            for f in path.rglob("*"):
                if f.is_file():
                    yield f, str(f.relative_to(path.parent))


async def save_uploads(
    sandbox: Path,
    dest_rel: str,
    files: Sequence[IncomingFile],
    relative_paths_json: str,
    max_bytes: int,
) -> UploadResult:
    """Store uploaded PDFs (streamed in chunks, so nothing big sits in memory) and report what was skipped.

    ``relative_paths_json`` is a JSON array of relative paths, present when a whole folder was dropped, to keep its
    structure. If it is empty or does not match ``files`` in length, only the file names are used.
    ``max_bytes`` caps each file (0 = unlimited).
    """
    dest_base = resolve_in(sandbox, dest_rel) if dest_rel else sandbox
    if not dest_base.is_dir():
        raise ExplorerError(400, "La carpeta destino no existe")

    try:
        relative_paths = json.loads(relative_paths_json)
        if not isinstance(relative_paths, list):
            relative_paths = []
    except (json.JSONDecodeError, ValueError):
        relative_paths = []

    is_folder_upload = len(relative_paths) == len(files) and any(relative_paths)
    if is_folder_upload:  # refuse a name clash before writing anything
        root_name = Path(relative_paths[0]).parts[0]
        root_dest = dest_base / root_name
        resolve_in(sandbox, relative(sandbox, root_dest))
        if root_dest.exists():
            raise ExplorerError(
                409,
                f'Ya existe una carpeta llamada "{root_name}" en este directorio. '
                "Renómbrala o elimínala antes de subir.",
            )

    uploaded: list[str] = []
    skipped: list[str] = []
    non_pdf_count = 0
    for i, upload in enumerate(files):
        filename = upload.filename or f"archivo_{i}"
        keep_structure = is_folder_upload and relative_paths[i]
        dest_file = dest_base / Path(relative_paths[i]) if keep_structure else dest_base / filename

        if dest_file.suffix.lower() != ".pdf":  # only PDFs are accepted; non-PDFs are counted for the warning
            non_pdf_count += 1
            skipped.append(filename)
            continue
        try:
            resolve_in(sandbox, relative(sandbox, dest_file))
        except ExplorerError:
            skipped.append(filename)
            continue

        dest_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            size = 0
            too_large = False
            async with aiofiles.open(dest_file, "wb") as out:
                while chunk := await upload.read(_UPLOAD_CHUNK):
                    size += len(chunk)
                    if max_bytes and size > max_bytes:
                        too_large = True
                        break
                    await out.write(chunk)
            if too_large:
                dest_file.unlink(missing_ok=True)
                skipped.append(f"{filename}: excede el tamaño máximo permitido ({max_bytes} bytes)")
                continue
            uploaded.append(relative(sandbox, dest_file))
        except OSError as exc:
            skipped.append(f"{filename}: {exc}")

    return UploadResult(uploaded=uploaded, skipped=skipped, non_pdf_count=non_pdf_count)
