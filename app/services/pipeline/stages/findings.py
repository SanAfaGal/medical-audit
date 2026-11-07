"""Stages that turn what is on disk into findings: missing required documents and surplus files."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from pathlib import Path

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import build_ct_map, ct_for_folder
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.pipeline.required_docs import find_missing_documents
from app.services.pipeline.surplus import scan_surplus, snapshot_invoices
from app.services.stage_events import ProgressJob


@stage("CHECK_REQUIRED_DOCS", requires="stage")
async def _check_required_docs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify required documents per service type; record findings for missing ones."""
    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    inv_repo = InvoiceRepo(db)
    rules_repo = RulesRepo(db)
    finding_repo = MissingFileRepo(db)

    service_docs = await rules_repo.get_service_type_docs_map(institution.id)
    prefixes = await rules_repo.get_active_doc_types_map()
    doc_names = {dt.id: dt.description for dt in await rules_repo.get_doc_types()}

    presente_invoices = await inv_repo.get_invoices_by_status_code(period.id, "PRESENTE")
    yield plog("INFO", f"Facturas PRESENTE a verificar: {len(presente_invoices)}")

    # Plain data for the worker thread (ORM objects must not cross threads)
    invoice_data = [(inv.id, inv.invoice_number, inv.service_type_id) for inv in presente_invoices]
    id_prefix = institution.invoice_id_prefix or ""

    job = ProgressJob(
        lambda on_progress, cancel: find_missing_documents(
            stage_path, id_prefix, invoice_data, service_docs, prefixes, on_progress, cancel
        ),
        ctx,
        label="Facturas",
        total=len(invoice_data),
    )
    async for line in job:
        yield line
    findings_map = job.result

    # Bulk upsert all findings in one DB call
    all_findings = [(inv_id, dt_id) for inv_id, (_, dt_ids) in findings_map.items() for dt_id in dt_ids]
    await finding_repo.bulk_upsert_findings(all_findings)

    ct_map = await build_ct_map(db, period)
    for _inv_id, (inv_number, dt_ids) in findings_map.items():
        doc_str = ", ".join(doc_names.get(dt_id, str(dt_id)) for dt_id in dt_ids)
        yield plog("WARN", f"Faltantes ({doc_str})", folder=inv_number, contract_type=ct_for_folder(inv_number, ct_map))

    invoices_with_findings = [inv_number for inv_number, _ in findings_map.values()]
    if invoices_with_findings:
        updated = await inv_repo.batch_update_folder_status(period.id, invoices_with_findings, "PENDIENTE")
        yield plog("INFO", f"Facturas marcadas PENDIENTE: {updated}")

    await db.commit()
    yield plog("INFO", f"Hallazgos totales registrados: {len(all_findings)}")


@stage("REVISAR_SOBRANTES", requires="stage", missing="ERROR")
async def _revisar_sobrantes(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Identify surplus files per invoice folder and suggest doc-type renames."""
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    rules_repo = RulesRepo(db)
    service_docs = await rules_repo.get_service_type_docs_map(institution.id)
    prefixes = {
        dt_id: (names[0] if names else None) for dt_id, names in (await rules_repo.get_active_doc_types_map()).items()
    }
    invoices = snapshot_invoices(await InvoiceRepo(db).get_with_findings(period.id))

    job = ProgressJob(
        lambda on_progress, cancel: scan_surplus(stage_path, invoices, service_docs, prefixes, on_progress, cancel),
        ctx,
        label="Carpetas",
    )
    async for line in job:
        yield line
    scan = job.result

    for line in scan.log_lines:
        yield line
    yield plog("INFO", f"Total: {scan.total} archivos sobrantes en {len(scan.items)} carpetas")
    if scan.items:
        yield f"[DATA] {json.dumps(scan.items, ensure_ascii=False)}"
    else:
        yield plog("INFO", "No se encontraron archivos sobrantes")
