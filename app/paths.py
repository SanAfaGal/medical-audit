"""Path utilities: resolve audit data root to a filesystem path."""

from pathlib import Path

from app.config import settings


def to_container_path(path_str: str) -> Path:
    """Return the filesystem Path for the given string.

    Kept for backward-compatibility with call sites that still pass a string.
    Prefer using `audit_data_root` directly.
    """
    return Path(path_str)


# Convenience: resolved Path for the configured audit data root.
audit_data_root: Path = Path(settings.audit_data_root)


def safe_resolve(path: Path, base: Path) -> Path:
    """Resolve *path* and verify it lies within *base*.

    Raises:
        ValueError: if the resolved path is not *base* itself or a descendant of it.
    """
    resolved = path.resolve()
    base_resolved = base.resolve()
    if resolved != base_resolved and base_resolved not in resolved.parents:
        raise ValueError(f"Path escapes base directory: {path}")
    return resolved


def safe_join(base: Path, rel: str) -> Path:
    """Join *rel* onto *base* and verify the result lies within *base*."""
    return safe_resolve(base / rel, base)
