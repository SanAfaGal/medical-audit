"""Shared FastAPI dependencies: load the institution / period a request refers to, or answer 404.

Query-string and path-parameter endpoints use the ``Depends`` versions. Endpoints whose ids arrive in a
form or a JSON body call the plain ``load_*`` functions instead.
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.paths import PeriodPaths, period_paths
from app.repositories.institution_repo import InstitutionRepo
from app.repositories.invoice_repo import InvoiceRepo


@dataclass(frozen=True)
class PeriodScope:
    """An institution together with one of its periods, and where that period lives on disk."""

    institution: Institution
    period: AuditPeriod

    @property
    def paths(self) -> PeriodPaths:
        return period_paths(self.institution.name, self.period.period_label)


async def load_institution(
    db: AsyncSession, institution_id: int, detail: str = "Institución no encontrada"
) -> Institution:
    institution = await InstitutionRepo(db).get_by_id(institution_id)
    if institution is None:
        raise HTTPException(404, detail)
    return institution


async def load_period_scope(db: AsyncSession, institution_id: int, period_id: int) -> PeriodScope:
    """The period must exist *and* belong to the institution: a mismatched pair never reaches the disk."""
    institution = await load_institution(db, institution_id)
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if period is None or period.institution_id != institution_id:
        raise HTTPException(404, "Período no encontrado")
    return PeriodScope(institution, period)


async def load_period_scope_by_id(db: AsyncSession, period_id: int) -> PeriodScope:
    """Same, for endpoints that only know the period (its institution is derived from it)."""
    period = await InvoiceRepo(db).get_period_by_id(period_id)
    if period is None:
        raise HTTPException(404, "Período no encontrado")
    return PeriodScope(await load_institution(db, period.institution_id), period)


async def institution_or_404(institution_id: int, db: AsyncSession = Depends(get_db)) -> Institution:
    return await load_institution(db, institution_id)


async def period_scope(institution_id: int, period_id: int, db: AsyncSession = Depends(get_db)) -> PeriodScope:
    return await load_period_scope(db, institution_id, period_id)


async def period_scope_by_id(period_id: int, db: AsyncSession = Depends(get_db)) -> PeriodScope:
    return await load_period_scope_by_id(db, period_id)
