"""Stages that read the invoice PDFs and their names: invoice number in the text, file/folder match and CUFE."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import get_executor, invoice_pdfs
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.stage_events import ProgressJob


@stage("VERIFY_INVOICE_CODE", requires="stage")
async def _verify_invoice_code(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify that each invoice PDF contains its own invoice number in the text."""
    from app.core.validator import InvoiceValidator

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    invoices = await invoice_pdfs(ctx)

    validator = InvoiceValidator(stage_path, institution.invoice_id_prefix or "")
    job = ProgressJob(
        lambda on_progress, cancel: validator.validate_invoice_files(invoices, on_progress, cancel),
        ctx,
        label="Facturas",
        total=len(invoices),
    )
    async for line in job:
        yield line
    missing_code, _ = job.result
    for f in missing_code:
        yield plog("WARN", f"Sin número de factura en PDF: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin código: {len(missing_code)}")


@stage("CHECK_INVOICE_NUMBER_ON_FILES", requires="stage")
async def _check_invoice_number_on_files(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify that files inside each invoice folder match the folder name."""
    from app.core.inspector import FolderInspector

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    job = ProgressJob(
        lambda on_progress, cancel: inspector.find_mismatched_files(None, on_progress, cancel), ctx, label="Carpetas"
    )
    async for line in job:
        yield line
    mismatched = job.result
    for f in mismatched:
        yield plog("WARN", f"Archivo desajustado: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Archivos desajustados: {len(mismatched)}")


@stage("VERIFY_CUFE", requires="stage")
async def _verify_cufe(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify CUFE presence and tag folders missing a CUFE in invoice PDFs."""
    from app.core.ops import DocumentOps
    from app.core.validator import InvoiceValidator

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    invoices = await invoice_pdfs(ctx)

    validator = InvoiceValidator(stage_path, institution.invoice_id_prefix or "")
    job = ProgressJob(
        lambda on_progress, cancel: validator.validate_invoice_files(invoices, on_progress, cancel),
        ctx,
        label="Facturas",
        total=len(invoices),
    )
    async for line in job:
        yield line
    _, missing_cufe = job.result
    for f in missing_cufe:
        yield plog("WARN", f"Sin CUFE: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin CUFE: {len(missing_cufe)}")

    if missing_cufe:
        ops = DocumentOps(stage_path, institution.invoice_id_prefix or "")
        marked = await executor(ops.tag_dirs_missing_cufe, missing_cufe)
        yield plog("INFO", f"Carpetas marcadas sin CUFE: {marked}")
