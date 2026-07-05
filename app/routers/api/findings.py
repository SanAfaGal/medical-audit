"""API router for missing files (audit findings)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.repositories.finding_repo import MissingFileRepo
from app.schemas.finding import MissingFileCreate, MissingFileOut, MissingFileResolve

router = APIRouter(prefix="/missing-files", tags=["missing-files"])


@router.get("/{invoice_id}", response_model=list[MissingFileOut])
async def get_missing_files(invoice_id: int, db: AsyncSession = Depends(get_db)):
    repo = MissingFileRepo(db)
    return await repo.get_for_invoice(invoice_id)


@router.post("", response_model=MissingFileOut, status_code=201)
async def record_missing_file(data: MissingFileCreate, db: AsyncSession = Depends(get_db)):
    repo = MissingFileRepo(db)
    missing = await repo.record_missing_file(data.invoice_id, data.doc_type_id, data.expected_path)
    await db.commit()
    return missing


@router.patch("/{invoice_id}/{doc_type_id}/resolve", status_code=200)
async def resolve_missing_file(
    invoice_id: int,
    doc_type_id: int,
    data: MissingFileResolve,
    db: AsyncSession = Depends(get_db),
):
    repo = MissingFileRepo(db)
    await repo.resolve_missing_file(invoice_id, doc_type_id)
    await db.commit()
    files = await repo.get_for_invoice(invoice_id)
    resolved = next((f for f in files if f.doc_type_id == doc_type_id), None)
    if not resolved:
        raise HTTPException(404, "Archivo no encontrado")
    return MissingFileOut.model_validate(resolved)


@router.delete("/{invoice_id}/{doc_type_id}", status_code=204)
async def delete_missing_file(invoice_id: int, doc_type_id: int, db: AsyncSession = Depends(get_db)):
    repo = MissingFileRepo(db)
    await repo.delete_missing_file(invoice_id, doc_type_id)
    await db.commit()


@router.delete("/{invoice_id}", status_code=204)
async def delete_all_missing_files(invoice_id: int, db: AsyncSession = Depends(get_db)):
    repo = MissingFileRepo(db)
    await repo.delete_all_for_invoice(invoice_id)
    await db.commit()


@router.post("/batch-delete", status_code=200)
async def batch_delete_findings(data: dict, db: AsyncSession = Depends(get_db)):
    """Delete all findings for multiple invoices at once."""
    invoice_ids = data.get("invoice_ids", [])
    if not invoice_ids:
        return {"deleted": 0}
    repo = MissingFileRepo(db)
    deleted = await repo.delete_all_for_invoices(invoice_ids)
    await db.commit()
    return {"deleted": deleted}
