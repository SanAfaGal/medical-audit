"""Path utilities: resolve audit data root to a filesystem path."""

from dataclasses import dataclass
from pathlib import Path

from app.config import settings


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


@dataclass(frozen=True)
class PeriodPaths:
    """Folder layout of one audit period: <audit root>/<institution>/<period>/{DRIVE,STAGE,AUDIT}."""

    base: Path

    @property
    def drive(self) -> Path:
        return self.base / "DRIVE"

    @property
    def stage(self) -> Path:
        return self.base / "STAGE"

    @property
    def audit(self) -> Path:
        return self.base / "AUDIT"


def period_paths(institution_name: str, period_label: str) -> PeriodPaths:
    """The one place that knows where a period lives on disk (the audit root is read at call time)."""
    return PeriodPaths(audit_data_root / institution_name / period_label)
