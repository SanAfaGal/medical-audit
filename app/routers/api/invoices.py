"""API router for invoices: list, filter, batch-update, ingest, export."""

from __future__ import annotations

import io
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.repositories.finding_repo import MissingFileRepo
from app.repositories.invoice_repo import InvoiceRepo
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


@router.get("/findings-summary", response_model=list[dict])
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
    from app.repositories.institution_repo import InstitutionRepo

    inst_repo = InstitutionRepo(db)
    institution = await inst_repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")

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
    import openpyxl

    repo = InvoiceRepo(db)
    invoices = await repo.get_all_for_export(period_id)

    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet("Facturas")
    ws.append(
        [
            "Período",
            "Hospital",
            "Factura",
            "Fecha",
            "Tipo Doc.",
            "Número Doc.",
            "Paciente",
            "Admisión",
            "Administradora",
            "Tipo Contrato",
            "Contrato",
            "Servicio",
            "Operación",
            "Estado",
            "Hallazgos",
        ]
    )

    for inv in invoices:
        period_label = inv.period.period_label if inv.period else ""
        hospital = inv.period.institution.display_name if inv.period and inv.period.institution else ""
        ic = inv.agreement
        administrator = ""
        contract_type = ""
        contract = ""
        if ic:
            administrator = ic.administrator.canonical_name or ic.administrator.raw_name if ic.administrator else ""
            contract_type = ic.contract_type.name if ic.contract_type else ""
            contract = ic.contract.canonical_name or ic.contract.raw_name if ic.contract else ""
        service = inv.service_type.display_name if inv.service_type else ""
        status = inv.folder_status.status if inv.folder_status else ""
        findings = ", ".join(mf.doc_type.code for mf in inv.missing_files if mf.resolved_at is None and mf.doc_type)

        ws.append(
            [
                period_label,
                hospital,
                inv.invoice_number,
                inv.date.isoformat() if inv.date else "",
                inv.id_type,
                inv.id_number,
                inv.patient_name,
                inv.admission or "",
                administrator,
                contract_type,
                contract,
                service,
                inv.employee or "",
                status,
                findings,
            ]
        )

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"facturas_{period_id}.xlsx"
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


class RenameSurplusRequest(BaseModel):
    folder_path: str
    old_filename: str
    doc_type_id: int


@router.post("/{invoice_id}/rename-surplus")
async def rename_surplus_file(
    invoice_id: int,
    data: RenameSurplusRequest,
    db: AsyncSession = Depends(get_db),
):
    """Rename a surplus file to match the required doc-type prefix and resolve the finding."""
    import os

    from app.paths import audit_data_root, safe_resolve
    from app.repositories.rules_repo import RulesRepo

    repo = InvoiceRepo(db)
    invoice = await repo.get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Factura no encontrada")

    if "/" in data.old_filename or "\\" in data.old_filename or ".." in data.old_filename:
        raise HTTPException(400, "Nombre de archivo inválido")

    rules_repo = RulesRepo(db)
    try:
        folder = safe_resolve(Path(data.folder_path), audit_data_root)
    except ValueError:
        raise HTTPException(400, "Ruta fuera del directorio de auditoría")

    if not folder.is_dir():
        raise HTTPException(400, "Carpeta no encontrada")

    doc_type = await rules_repo.get_doc_type_by_id(data.doc_type_id)
    if not doc_type or not doc_type.prefix:
        raise HTTPException(400, "Tipo de documento sin prefijo configurado")

    old_path = folder / data.old_filename
    if not old_path.exists():
        raise HTTPException(404, "Archivo no encontrado en disco")

    # Strip the existing wrong prefix (everything before the first "_") and replace it
    stem = data.old_filename.split("_", 1)[1] if "_" in data.old_filename else data.old_filename
    new_filename = f"{doc_type.prefix}_{stem}"
    new_path = folder / new_filename
    os.rename(old_path, new_path)

    finding_repo = MissingFileRepo(db)
    await finding_repo.resolve_missing_file(invoice_id, data.doc_type_id)
    await db.commit()

    return {"ok": True, "new_filename": new_filename}


class DeleteSurplusRequest(BaseModel):
    folder_path: str
    filename: str


@router.post("/{invoice_id}/delete-surplus")
async def delete_surplus_file(
    invoice_id: int,
    data: DeleteSurplusRequest,
    db: AsyncSession = Depends(get_db),
):
    """Delete a surplus file from disk."""
    import os

    from app.paths import audit_data_root, safe_resolve

    repo = InvoiceRepo(db)
    invoice = await repo.get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Factura no encontrada")

    if "/" in data.filename or "\\" in data.filename or ".." in data.filename:
        raise HTTPException(400, "Nombre de archivo inválido")

    try:
        folder = safe_resolve(Path(data.folder_path), audit_data_root)
    except ValueError:
        raise HTTPException(400, "Ruta fuera del directorio de auditoría")

    target = folder / data.filename
    if not target.exists():
        raise HTTPException(404, "Archivo no encontrado en disco")

    os.remove(target)
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
    import shutil

    from sqlalchemy import select, update

    from app.models.invoice import Invoice
    from app.models.rules import FolderStatus
    from app.paths import audit_data_root, safe_resolve

    audit_root = audit_data_root

    # Get-or-create ANULAR folder status
    anular_status = (await db.execute(select(FolderStatus).where(FolderStatus.status == "ANULAR"))).scalar_one_or_none()
    if anular_status is None:
        anular_status = FolderStatus(status="ANULAR")
        db.add(anular_status)
        await db.flush()

    annulled = 0
    skipped = 0
    errors: list[str] = []

    for decision in data.decisions:
        if decision.action != "annul":
            skipped += 1
            continue

        # Security: folder must resolve to a path inside the audit data root
        try:
            folder = safe_resolve(Path(decision.folder_path), audit_root)
        except ValueError:
            errors.append(f"Ruta fuera del directorio de auditoría: {decision.folder_path}")
            continue

        # Update DB status
        if decision.invoice_id is not None:
            await db.execute(
                update(Invoice).where(Invoice.id == decision.invoice_id).values(folder_status_id=anular_status.id)
            )

        # Delete folder from disk
        if folder.exists() and folder.is_dir():
            shutil.rmtree(folder)

        annulled += 1

    await db.commit()
    return {"annulled": annulled, "skipped": skipped, "errors": errors}
