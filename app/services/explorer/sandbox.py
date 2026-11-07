"""Path rules shared by every explorer operation: everything stays inside the period's sandbox."""

from __future__ import annotations

from pathlib import Path

from app.paths import is_plain_filename, safe_resolve
from app.services.explorer.errors import ExplorerError


def resolve_in(sandbox: Path, rel: str) -> Path:
    """Resolve a client-supplied relative path inside the sandbox (a leading ``/`` is read as relative)."""
    try:
        return safe_resolve(sandbox, rel.lstrip("/\\"))
    except ValueError as exc:
        raise ExplorerError(400, "Ruta fuera del directorio permitido") from exc


def relative(sandbox: Path, path: Path) -> str:
    """``path`` relative to the sandbox with forward slashes, as the UI expects."""
    return str(path.relative_to(sandbox)).replace("\\", "/")


def validate_entry_name(name: str) -> None:
    """A file/folder name must not be empty or carry path separators."""
    if not name or not name.strip():
        raise ExplorerError(400, "El nombre no puede estar vacío")
    if not is_plain_filename(name):
        raise ExplorerError(400, "El nombre no puede contener separadores de ruta")


def require_pdf(path: Path, detail: str = "PDF no encontrado") -> None:
    if not path.exists() or path.suffix.lower() != ".pdf":
        raise ExplorerError(404, detail)
