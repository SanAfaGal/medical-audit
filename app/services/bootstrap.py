"""Startup checks for data the application cannot work without."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.rules import FolderStatus

# The invoice-status triggers in the database and several pipeline stages look these up by name.
# A missing row makes them fail in confusing ways (e.g. NULL ids), so the app creates them on boot.
REQUIRED_FOLDER_STATUSES: tuple[str, ...] = (
    "PRESENTE",
    "PENDIENTE",
    "FALTANTE",
    "AUDITADA",
    "ANULAR",
    "REVISAR",
)


async def ensure_folder_statuses(db: AsyncSession) -> list[str]:
    """Insert any missing required folder statuses; returns the ones that were added."""
    existing = set((await db.execute(select(FolderStatus.status))).scalars().all())
    missing = [name for name in REQUIRED_FOLDER_STATUSES if name not in existing]
    for name in missing:
        db.add(FolderStatus(status=name))
    if missing:
        await db.commit()
    return missing
