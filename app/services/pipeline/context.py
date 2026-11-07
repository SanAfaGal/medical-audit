"""Typed execution context handed to every stage."""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.paths import audit_data_root


class StageContext(TypedDict, total=False):
    """Everything a stage can read. The first group is always present; the rest are per-request extras."""

    # Always set by build_context
    institution: Institution
    period: AuditPeriod
    db: AsyncSession
    base_path: Path
    drive_path: Path
    stage_path: Path
    audit_path: Path
    # Optional extras passed by the caller (router / task manager)
    file_bytes: bytes  # LOAD_AND_PROCESS: uploaded SIHOS Excel
    period_code: str  # LOAD_AND_PROCESS
    invoice_numbers: list[str]  # DOWNLOAD_DRIVE: manual list
    doc_type_id: int  # SIHOS download stages
    cancel_event: Any  # threading.Event set when the task is cancelled


def build_context(
    institution: Institution, period: AuditPeriod, db: AsyncSession, extra: dict[str, Any]
) -> StageContext:
    base = audit_data_root / institution.name / period.period_label
    ctx: StageContext = {
        "institution": institution,
        "period": period,
        "db": db,
        "base_path": base,
        "drive_path": base / "DRIVE",
        "stage_path": base / "STAGE",
        "audit_path": base / "AUDIT",
    }
    ctx.update(extra)  # type: ignore[typeddict-item]  # extras are validated by the stage that reads them
    return ctx
