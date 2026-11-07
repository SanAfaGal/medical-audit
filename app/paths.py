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


def safe_resolve(base: Path, target: str | Path) -> Path:
    """Resolve *target* against *base* and verify the result lies within *base*.

    Single contract for every client-supplied path: ``target`` may be relative (joined onto
    *base*) or absolute (kept as is). In both cases the fully resolved path - symlinks and
    ``..`` included - must be *base* itself or a descendant of it.

    Raises:
        ValueError: if the resolved path escapes *base*.
    """
    base_resolved = base.resolve()
    resolved = (base_resolved / target).resolve()
    if resolved != base_resolved and base_resolved not in resolved.parents:
        raise ValueError(f"Path escapes base directory: {target}")
    return resolved
