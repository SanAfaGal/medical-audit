"""API router for invoices: list, filter, batch-update, ingest, export."""

from __future__ import annotations

from typing import Any
import asyncio
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.repositories.finding_repo import MissingFileRepo
from app.routers.deps import load_institution, load_period_scope, load_period_scope_by_id
from app.models.invoice import Invoice
from app.repositories.invoice_repo import InvoiceRepo
from app.repositories.rules_repo import RulesRepo
from app.routers.errors import http_errors
from app.services import invoice_files
from app.services.errors import ServiceError
from app.services.invoice_export import XLSX_MEDIA_TYPE, build_workbook
from app.schemas.invoice import (
    BatchStatusUpdate,
    InvoiceListItem,
    InvoiceOut,
    InvoiceStatusUpdate,
)

router = APIRouter(prefix="/invoices", tags=["invoices"])


class BatchDeleteRequest(BaseModel):
    invoice_ids: list[int] = Field(..., min_length=1, max_length=1000)


@router.get("/ids", response_model=list[int])
async def get_invoice_ids(
    period_id: int,
    folder_status_id: int | None = None,
    service_type_id: int | None = None,
    admin_canonical: str | None = None,
    admin_type: str | None = None,
    contract_canonical: str | None = None,
    search: str | None = None,
    has_finding_doc_type_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    repo = InvoiceRepo(db)
    return await repo.get_invoice_ids(
        audit_period_id=period_id,
        folder_status_id=folder_status_id,
        service_type_id=service_type_id,
        admin_canonical=admin_canonical,
        admin_type=admin_type,
        contract_canonical=contract_canonical,
        search=search,
        has_finding_doc_type_id=has_finding_doc_type_id,
    )


@router.get("/stats", response_model=dict)
async def get_stats(period_id: int, db: AsyncSession = Depends(get_db)):
    repo = InvoiceRepo(db)
    return await repo.get_stats(period_id)


@router.get("/findings-summary", response_model=list[dict[str, Any]])
async def get_findings_summary(period_id: int, db: AsyncSession = Depends(get_db)):
    repo = MissingFileRepo(db)
    return await repo.get_findings_summary(period_id)


@router.get("", response_model=dict)
async def list_invoices(
    period_id: int,
    folder_status_id: int | None = None,
    service_type_id: int | None = None,
    admin_canonical: str | None = None,
    admin_type: str | None = None,
    contract_canonical: str | None = None,
    search: str | None = None,
    has_finding_doc_type_id: int | None = None,
    page: int = 1,
    page_size: int = 50,
    db: AsyncSession = Depends(get_db),
):
    inv_repo = InvoiceRepo(db)
    invoices, total = await inv_repo.filter_invoices(
        audit_period_id=period_id,
        folder_status_id=folder_status_id,
        service_type_id=service_type_id,
        admin_canonical=admin_canonical,
        admin_type=admin_type,
        contract_canonical=contract_canonical,
        search=search,
        has_finding_doc_type_id=has_finding_doc_type_id,
        page=page,
        page_size=page_size,
    )

    items = []
    for inv in invoices:
        ic = inv.agreement
        items.append(
            InvoiceListItem(
                id=inv.id,
                invoice_number=inv.invoice_number,
                patient_name=inv.patient_name,
                agreement_id=inv.agreement_id,
                admin_canonical=ic.administrator.canonical_name if ic and ic.administrator else None,
                admin_type=ic.contract_type.name if ic and ic.contract_type else None,
                contract_canonical=ic.contract.canonical_name if ic and ic.contract else None,
                folder_status=inv.folder_status.status,
                folder_status_id=inv.folder_status_id,
                service_type_code=inv.service_type.code if inv.service_type else None,
                service_type_id=inv.service_type_id,
                missing_file_count=len([mf for mf in inv.missing_files if mf.resolved_at is None]),
                date=inv.date,
                admission=inv.admission,
            )
        )

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [item.model_dump() for item in items],
    }


@router.patch("/{invoice_id}/status", response_model=InvoiceOut)
async def update_status(
    invoice_id: int,
    data: InvoiceStatusUpdate,
    db: AsyncSession = Depends(get_db),
):
    repo = InvoiceRepo(db)
    invoice = await repo.get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Factura no encontrada")
    await repo.update_folder_status(invoice_id, data.folder_status_id)
    await db.commit()
    await db.refresh(invoice)
    return invoice


@router.post("/batch-status", status_code=200)
async def batch_update_status(data: BatchStatusUpdate, db: AsyncSession = Depends(get_db)):
    repo = InvoiceRepo(db)
    await repo.batch_update_status(data.invoice_ids, data.folder_status_id)
    await db.commit()
    return {"updated": len(data.invoice_ids)}


@router.post("/batch-delete", status_code=200)
async def batch_delete_invoices(data: BatchDeleteRequest, db: AsyncSession = Depends(get_db)):
    repo = InvoiceRepo(db)
    deleted = await repo.batch_delete_invoices(data.invoice_ids)
    await db.commit()
    return {"deleted": deleted}


@router.delete("/{invoice_id}", status_code=204)
async def delete_invoice(invoice_id: int, db: AsyncSession = Depends(get_db)):
    repo = InvoiceRepo(db)
    deleted = await repo.delete_invoice(invoice_id)
    if not deleted:
        raise HTTPException(404, "Factura no encontrada")
    await db.commit()


@router.post("/ingest", status_code=200)
async def ingest_excel(
    institution_id: int = Form(...),
    period_id: int = Form(...),
    file: UploadFile = File(...),
    scan_only: bool = Form(False),
    save_mappings_only: bool = Form(False),
    db: AsyncSession = Depends(get_db),
):
    """Upload and ingest a SIHOS Excel export."""
    from app.services.billing import ingest

    institution = await load_institution(db, institution_id)

    file_bytes = await file.read()
    result = await ingest(
        file_bytes,
        institution,
        period_id,
        db,
        scan_only=scan_only or save_mappings_only,
        save_mappings_only=save_mappings_only,
    )
    return result


@router.get("/export")
async def export_invoices(period_id: int, db: AsyncSession = Depends(get_db)):
    """Export all invoices for a period to Excel (.xlsx)."""
    invoices = await InvoiceRepo(db).get_all_for_export(period_id)
    content = await asyncio.to_thread(build_workbook, invoices)
    return Response(
        content=content,
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="facturas_{period_id}.xlsx"'},
    )


async def _invoice_sandbox(db: AsyncSession, invoice: Invoice) -> Path:
    """Sandbox of the period the invoice belongs to (the client cannot choose another one)."""
    scope = await load_period_scope_by_id(db, invoice.audit_period_id)
    return scope.paths.base.resolve()


class RenameSurplusRequest(BaseModel):
    folder_path: str
    old_filename: str
    doc_type_id: int


@router.post("/{invoice_id}/rename-surplus")
@http_errors
async def rename_surplus_file(
    invoice_id: int,
    data: RenameSurplusRequest,
    db: AsyncSession = Depends(get_db),
):
    """Rename a surplus file to match the required doc-type prefix and resolve the finding."""
    invoice = await InvoiceRepo(db).get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Factura no encontrada")
    invoice_files.require_plain_filename(data.old_filename)
    folder = invoice_files.surplus_folder(await _invoice_sandbox(db, invoice), data.folder_path)

    doc_type = await RulesRepo(db).get_doc_type_by_id(data.doc_type_id)
    if not doc_type or not doc_type.prefix:
        raise HTTPException(400, "Tipo de documento sin prefijo configurado")

    new_filename = await asyncio.to_thread(invoice_files.rename_surplus, folder, data.old_filename, doc_type.prefix)

    await MissingFileRepo(db).resolve_missing_file(invoice_id, data.doc_type_id)
    await db.commit()
    return {"ok": True, "new_filename": new_filename}


class DeleteSurplusRequest(BaseModel):
    folder_path: str
    filename: str


@router.post("/{invoice_id}/delete-surplus")
@http_errors
async def delete_surplus_file(
    invoice_id: int,
    data: DeleteSurplusRequest,
    db: AsyncSession = Depends(get_db),
):
    """Delete a surplus file from disk."""
    invoice = await InvoiceRepo(db).get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Factura no encontrada")
    invoice_files.require_plain_filename(data.filename)
    folder = invoice_files.invoice_folder(await _invoice_sandbox(db, invoice), data.folder_path)

    await asyncio.to_thread(invoice_files.delete_surplus, folder, data.filename)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Annul folders (CHECK_FOLDERS_WITH_EXTRA_TEXT interactive action)
# ---------------------------------------------------------------------------


class FolderAnnulDecision(BaseModel):
    folder_path: str
    invoice_id: int | None = None
    action: str  # "annul" | "skip"


class BulkAnnulRequest(BaseModel):
    institution_id: int
    period_id: int
    decisions: list[FolderAnnulDecision]


@router.post("/annul-folders", status_code=200)
async def annul_folders(data: BulkAnnulRequest, db: AsyncSession = Depends(get_db)):
    """Apply auditor decisions for malformed folders: annul+delete or skip.

    For each decision with action="annul":
    - Updates the invoice's folder_status to ANULAR in the DB (if invoice_id provided).
    - Deletes the folder and all its contents from disk.
    """
    inv_repo = InvoiceRepo(db)

    # Scope every deletion to this period's sandbox (audit_root/<institution>/<period>), never the whole root.
    sandbox = (await load_period_scope(db, data.institution_id, data.period_id)).paths.base.resolve()
    anular_status = await inv_repo.get_or_create_folder_status("ANULAR")

    annulled = 0
    skipped = 0
    errors: list[str] = []
    for decision in data.decisions:
        if decision.action != "annul":
            skipped += 1
            continue
        try:
            folder = invoice_files.invoice_folder(sandbox, decision.folder_path)
        except ServiceError:
            errors.append(f"Ruta fuera del directorio del período: {decision.folder_path}")
            continue

        if decision.invoice_id is not None:
            await inv_repo.update_folder_status(decision.invoice_id, anular_status.id)
        await asyncio.to_thread(invoice_files.remove_folder, folder)
        annulled += 1

    await db.commit()
    return {"annulled": annulled, "skipped": skipped, "errors": errors}
