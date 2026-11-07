"""File actions the pipeline screens ask for: review decisions, previews, the AUDIT zip and the Drive zip upload.

Blocking disk work: call these from a worker thread. They raise :class:`ServiceError` for what the user can fix.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from app.paths import safe_resolve
from app.services.archive import ArchiveError, ArchiveToolMissing, build_zip_file, extract_archive
from app.services.errors import ServiceError

logger = logging.getLogger(__name__)

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


@dataclass
class NonPdfOutcome:
    deleted: int = 0
    converted: int = 0
    errors: list[str] = field(default_factory=list)


def apply_non_pdf_decisions(stage_path: Path, decisions: Sequence[tuple[str, str]]) -> NonPdfOutcome:
    """Apply the auditor's ``(relative path, "delete" | "convert")`` decisions on the files REMOVE_NON_PDF found.

    A problem with one file is reported in ``errors`` and the rest are still processed.
    """
    from app.core.ops import IMAGE_EXTENSIONS, convert_image_to_pdf

    if not stage_path.is_dir():
        raise ServiceError(400, "Directorio STAGE no existe")

    outcome = NonPdfOutcome()
    for rel_path, action in decisions:
        try:
            target = safe_resolve(stage_path, rel_path)
        except (ValueError, OSError):
            outcome.errors.append(f"Ruta inválida: {rel_path}")
            continue
        if not target.exists():
            outcome.errors.append(f"Archivo no encontrado: {rel_path}")
            continue

        if action == "delete":
            try:
                target.unlink()
                outcome.deleted += 1
            except OSError as exc:
                logger.error("No se pudo eliminar %s: %s", target, exc)
                outcome.errors.append(f"No se pudo eliminar {target.name}")
        elif action == "convert":
            if target.suffix.lstrip(".").lower() not in IMAGE_EXTENSIONS:
                outcome.errors.append(f"Formato no convertible: {target.name}")
                continue
            try:
                convert_image_to_pdf(target)
                outcome.converted += 1
            except Exception as exc:  # noqa: BLE001 - one bad image must not abort the batch
                logger.error("Error convirtiendo %s: %s", target, exc)
                outcome.errors.append(f"Error convirtiendo {target.name}")
    return outcome


def preview_target(stage_path: Path, rel_path: str) -> tuple[Path, str]:
    """The STAGE file to preview and its media type (``application/octet-stream`` when unknown)."""
    try:
        target = safe_resolve(stage_path, rel_path)
    except (ValueError, OSError) as exc:
        raise ServiceError(400, "Ruta inválida") from exc
    if not target.is_file():
        raise ServiceError(404, "Archivo no encontrado")
    return target, _PREVIEW_MIME.get(target.suffix.lstrip(".").lower(), "application/octet-stream")


def audit_zip(audit_path: Path, institution_name: str, period_label: str) -> tuple[Path, str]:
    """Pack AUDIT into a temporary zip on disk (so size is no limit). Returns the zip and its download name."""
    if not audit_path.is_dir():
        raise ServiceError(404, "Directorio AUDIT no encontrado")
    files = [f for f in audit_path.rglob("*") if f.is_file()]
    if not files:
        raise ServiceError(404, "No hay archivos en AUDIT")
    zip_path = build_zip_file((f, f.relative_to(audit_path)) for f in files)
    safe_institution = re.sub(r"[^\w]", "_", institution_name)
    safe_period = re.sub(r"[^\w]", "_", period_label)
    return zip_path, f"{safe_institution}_{safe_period}.zip"


def extract_drive_archive(drive_path: Path, source: BinaryIO, filename: str, size: int | None, max_bytes: int) -> int:
    """Extract an uploaded .zip/.7z/.rar into DRIVE (created if missing). Returns how many files came out."""
    drive_path.mkdir(parents=True, exist_ok=True)
    if max_bytes and (size or 0) > max_bytes:
        raise ServiceError(413, f"Archivo demasiado grande (máx {max_bytes} bytes)")
    try:
        return len(extract_archive(source, filename, drive_path))
    except ArchiveToolMissing as exc:
        raise ServiceError(500, str(exc)) from exc
    except ArchiveError as exc:
        raise ServiceError(400, str(exc)) from exc
