"""Small helpers shared by several pipeline stages."""

from __future__ import annotations

import asyncio
import re

from app.models.institution import Institution
from app.models.period import AuditPeriod


def sihos_excel_name(institution: Institution, period: AuditPeriod) -> str:
    """Return the canonical filename for a saved SIHOS Excel: SIHOS_{name}_{period}.xlsx"""
    name = re.sub(r"[^\w\-]", "_", institution.name).strip("_")
    return f"SIHOS_{name}_{period.period_label}.xlsx"


def get_executor():
    """Return a callable that runs a sync function in the default thread executor."""
    loop = asyncio.get_running_loop()

    async def run(fn, *args, **kwargs):
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    return run


INVOICE_NUMBER_RE = re.compile(r"[\w.\-]{1,50}")


def sihos_url_error(institution) -> str | None:
    """Re-validate the stored SIHOS URL (with DNS) right before the server's browser opens it."""
    from app.core.urlcheck import validate_portal_url

    url = institution.sihos_base_url or ""
    if not url.strip():
        return "La institución no tiene URL de SIHOS configurada."
    try:
        validate_portal_url(url, resolve=True)
    except ValueError as exc:
        return f"URL de SIHOS no permitida: {exc}"
    return None


async def build_ct_map(db, period) -> dict[str, str]:
    """Return {invoice_number: contract_type_name} for the period."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.models.invoice import Invoice
    from app.models.institution import Agreement

    q = (
        select(Invoice)
        .where(Invoice.audit_period_id == period.id)
        .options(selectinload(Invoice.agreement).selectinload(Agreement.contract_type))
    )
    rows = (await db.execute(q)).scalars().all()
    return {
        inv.invoice_number: inv.agreement.contract_type.name
        for inv in rows
        if inv.agreement and inv.agreement.contract_type
    }


def ct_for_folder(folder: str, ct_map: dict[str, str]) -> str | None:
    """Resolve contract_type name for a folder (exact match, then substring)."""
    if folder in ct_map:
        return ct_map[folder]
    for num, ct in ct_map.items():
        if num in folder:
            return ct
    return None
