"""API router for audit periods."""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.paths import period_paths
from app.routers.deps import PeriodScope, institution_or_404, period_scope_by_id
from app.models.institution import Institution
from app.repositories.institution_repo import InstitutionRepo
from app.repositories.invoice_repo import InvoiceRepo
from app.schemas.invoice import PeriodCreate, PeriodOut

router = APIRouter(prefix="/institutions", tags=["periods"])
logger = logging.getLogger(__name__)

_PERIOD_SUBDIRS = ("DRIVE", "STAGE", "AUDIT")


def _create_period_dirs(base_path: str, period_label: str) -> list[str]:
    """Create DRIVE, STAGE and AUDIT under base_path/period_label.

    Returns the paths that were created (or already existed).
    Raises OSError if the filesystem operation fails.
    """
    root = Path(base_path) / period_label
    created = []
    for name in _PERIOD_SUBDIRS:
        d = root / name
        d.mkdir(parents=True, exist_ok=True)
        created.append(str(d))
    return created


def _dir_size(path: Path) -> int:
    """Tamaño total en bytes de todos los archivos bajo path."""
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) if path.exists() else 0


@router.get("/{institution_id}/periods", response_model=list[PeriodOut])
async def list_periods(institution: Institution = Depends(institution_or_404), db: AsyncSession = Depends(get_db)):
    return await InvoiceRepo(db).get_periods(institution.id)


@router.post("/{institution_id}/periods", response_model=PeriodOut, status_code=201)
async def create_period(
    data: PeriodCreate,
    institution: Institution = Depends(institution_or_404),
    db: AsyncSession = Depends(get_db),
):
    inv_repo = InvoiceRepo(db)
    period = await inv_repo.get_or_create_period(institution.id, data.date_from, data.date_to, data.period_label)
    await db.commit()

    inst_root = str(period_paths(institution.name, data.period_label).base.parent)
    try:
        created = await asyncio.to_thread(_create_period_dirs, inst_root, data.period_label)
        for path in created:
            logger.info("Carpeta creada: %s", path)
    except OSError as exc:
        # Non-fatal: log and continue — period is already persisted
        logger.warning("No se pudieron crear las carpetas del período %s: %s", data.period_label, exc)

    return period


@router.get("/periods/{period_id}/disk-usage")
async def get_period_disk_usage(scope: PeriodScope = Depends(period_scope_by_id)):
    """Calcula el uso de disco de un período (DRIVE + STAGE + AUDIT)."""
    base = scope.paths.base
    sizes = await asyncio.to_thread(lambda: {name: _dir_size(base / name) for name in _PERIOD_SUBDIRS})
    return {
        "total_bytes": sum(sizes.values()),
        "drive_bytes": sizes["DRIVE"],
        "stage_bytes": sizes["STAGE"],
        "audit_bytes": sizes["AUDIT"],
        "exists": base.exists(),
    }


@router.post("/periods/{period_id}/purge-disk", status_code=200)
async def purge_period_disk(scope: PeriodScope = Depends(period_scope_by_id)):
    """Elimina las carpetas de disco del período; mantiene el período y sus datos en BD."""
    period_path = scope.paths.base
    if period_path.exists():
        await asyncio.to_thread(shutil.rmtree, period_path, True)
        logger.info("Carpetas de período purgadas: %s", period_path)

    return {"ok": True}


@router.delete("/periods/{period_id}", status_code=204)
async def delete_period(period_id: int, db: AsyncSession = Depends(get_db)):
    """Elimina el período de BD (cascade: facturas + hallazgos) y sus carpetas en disco."""
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if not period:
        return

    institution = await InstitutionRepo(db).get_by_id(period.institution_id)
    # Guardar antes del delete (el objeto expira tras el flush)
    period_path = period_paths(institution.name, period.period_label).base if institution else None

    await InvoiceRepo(db).delete_period(period_id)
    await db.commit()

    if period_path is not None:
        if period_path.exists():
            await asyncio.to_thread(shutil.rmtree, period_path, True)
            logger.info("Carpetas de período eliminadas: %s", period_path)
