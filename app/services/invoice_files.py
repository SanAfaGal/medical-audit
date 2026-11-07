"""File actions on an invoice's folder: surplus files and annulled folders.

Every path is checked against the sandbox of the invoice's own period. Blocking disk work: call from a thread.
Raises :class:`ServiceError` for what the user can fix.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from app.paths import is_plain_filename, safe_resolve
from app.services.errors import ServiceError


def invoice_folder(sandbox: Path, folder_path: str) -> Path:
    """A client-supplied folder inside the period sandbox, at least one level below DRIVE/STAGE/AUDIT.

    The sandbox itself, a phase folder, an empty path or ``.`` are refused: deleting or renaming inside them
    would reach far more than one invoice.
    """
    try:
        folder = safe_resolve(sandbox, folder_path)
    except ValueError as exc:
        raise ServiceError(400, "Ruta fuera del directorio del período") from exc
    if len(folder.relative_to(sandbox).parts) < 2:
        raise ServiceError(400, "Ruta fuera del directorio del período")
    return folder


def require_plain_filename(name: str) -> None:
    if not is_plain_filename(name):
        raise ServiceError(400, "Nombre de archivo inválido")


def surplus_folder(sandbox: Path, folder_path: str) -> Path:
    """The invoice folder a surplus action targets; it must exist."""
    folder = invoice_folder(sandbox, folder_path)
    if not folder.is_dir():
        raise ServiceError(400, "Carpeta no encontrada")
    return folder


def rename_surplus(folder: Path, old_filename: str, prefix: str) -> str:
    """Give a surplus file the document prefix it should have (the wrong prefix before the first ``_`` is replaced)."""
    old_path = folder / old_filename
    if not old_path.exists():
        raise ServiceError(404, "Archivo no encontrado en disco")
    stem = old_filename.split("_", 1)[1] if "_" in old_filename else old_filename
    new_filename = f"{prefix}_{stem}"
    old_path.rename(folder / new_filename)
    return new_filename


def delete_surplus(folder: Path, filename: str) -> None:
    try:
        target = safe_resolve(folder, filename)  # also rejects a symlink pointing outside the folder
    except ValueError as exc:
        raise ServiceError(400, "Ruta fuera del directorio del período") from exc
    if not target.is_file():
        raise ServiceError(404, "Archivo no encontrado en disco")
    target.unlink()


def remove_folder(folder: Path) -> None:
    """Delete an annulled invoice folder with everything in it (nothing happens if it is already gone)."""
    if folder.is_dir():
        shutil.rmtree(folder)
