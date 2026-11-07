"""Verification stages: invoice numbers in PDFs, folder names, required documents, leftovers and CUFE."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
import json
import threading

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.helpers import ProgressFn
from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import build_ct_map, ct_for_folder, get_executor
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.stage_events import progress_line, run_with_progress

import logging

logger = logging.getLogger(__name__)


@stage("VERIFY_INVOICE_CODE")
async def _verify_invoice_code(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify that each invoice PDF contains its own invoice number in the text."""
    from app.core.scanner import DocumentScanner
    from app.core.validator import InvoiceValidator

    from app.repositories.rules_repo import RulesRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    rules_repo = RulesRepo(db)
    invoice_doc = await rules_repo.get_doc_type_by_code("FACTURA")
    invoice_prefix = invoice_doc.prefix if invoice_doc and invoice_doc.prefix else (institution.invoice_id_prefix or "")

    scanner = DocumentScanner(stage_path)
    invoices = await executor(scanner.find_by_prefix, invoice_prefix)

    validator = InvoiceValidator(stage_path, institution.invoice_id_prefix or "")
    yield progress_line(0, len(invoices), label="Facturas")
    out: list = []
    async for line in run_with_progress(
        lambda on_progress, cancel: validator.validate_invoice_files(invoices, on_progress, cancel),
        ctx,
        out,
        label="Facturas",
    ):
        yield line
    missing_code, _ = out[0]
    for f in missing_code:
        yield plog("WARN", f"Sin número de factura en PDF: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin código: {len(missing_code)}")


@stage("CHECK_INVOICE_NUMBER_ON_FILES")
async def _check_invoice_number_on_files(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify that files inside each invoice folder match the folder name."""
    from app.core.inspector import FolderInspector

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    out: list = []
    async for line in run_with_progress(
        lambda on_progress, cancel: inspector.find_mismatched_files(None, on_progress, cancel),
        ctx,
        out,
        label="Carpetas",
    ):
        yield line
    mismatched = out[0]
    for f in mismatched:
        yield plog("WARN", f"Archivo desajustado: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Archivos desajustados: {len(mismatched)}")


@stage("CHECK_FOLDERS_WITH_EXTRA_TEXT")
async def _check_folders_with_extra_text(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Detect folders with extra text; send interactive payload for auditor review."""

    from sqlalchemy import select

    from app.models.invoice import Invoice
    from app.core.inspector import FolderInspector

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db: AsyncSession = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    malformed: list[Path] = await executor(inspector.find_malformed_dirs)

    yield plog("INFO", f"Carpetas con texto extra encontradas: {len(malformed)}")

    if not malformed:
        yield plog("INFO", "No se encontraron carpetas con texto extra")
        return

    # Extract invoice numbers and batch-query the DB for invoice IDs
    number_to_folder: dict[str, Path] = {}
    no_number: list[Path] = []
    for folder in malformed:
        num = inspector.extract_invoice_number(folder.name)
        if num:
            number_to_folder[num] = folder
        else:
            no_number.append(folder)

    invoice_number_to_id: dict[str, int] = {}
    if number_to_folder:
        result = await db.execute(
            select(Invoice.invoice_number, Invoice.id).where(
                Invoice.audit_period_id == period.id,
                Invoice.invoice_number.in_(list(number_to_folder.keys())),
            )
        )
        for inv_num, inv_id in result.all():
            invoice_number_to_id[inv_num] = inv_id

    # Build payload — one entry per malformed folder
    payload: list[dict] = []
    for num, folder in sorted(number_to_folder.items()):
        yield plog("WARN", "Carpeta con texto extra", folder=folder.name)
        payload.append(
            {
                "folder_name": folder.name,
                "folder_path": str(folder),
                "invoice_number": num,
                "invoice_id": invoice_number_to_id.get(num),
                "action": "skip",
            }
        )
    for folder in sorted(no_number, key=lambda p: p.name):
        yield plog("WARN", "Carpeta sin número de factura reconocible", folder=folder.name)
        payload.append(
            {
                "folder_name": folder.name,
                "folder_path": str(folder),
                "invoice_number": None,
                "invoice_id": None,
                "action": "skip",
            }
        )

    yield f"[DATA] {json.dumps(payload, ensure_ascii=False)}"


@stage("NORMALIZE_DIR_NAMES")
async def _normalize_dir_names(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Rename folders with extra text to their canonical identifier."""
    from app.core.inspector import FolderInspector
    from app.core.ops import DocumentOps

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    malformed = await executor(inspector.find_malformed_dirs)
    yield plog("INFO", f"Carpetas malformadas encontradas: {len(malformed)}")

    if malformed:
        ops = DocumentOps(stage_path, institution.invoice_id_prefix or "")
        renamed = await executor(ops.standardize_dir_names, malformed)
        yield plog("INFO", f"Carpetas renombradas: {renamed}")


@stage("CHECK_DIRS")
async def _check_dirs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Compare invoice numbers in DB against folders on disk; mark missing ones FALTANTE."""
    from app.core.inspector import FolderInspector

    from app.repositories.invoice_repo import InvoiceRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inv_repo = InvoiceRepo(db)
    presente_numbers = await inv_repo.get_invoice_numbers_by_status(period.id, "PRESENTE")
    yield plog("INFO", f"Facturas PRESENTE en BD: {len(presente_numbers)}")

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    missing = await executor(inspector.find_missing_dirs, presente_numbers)
    yield plog("INFO", f"Facturas sin carpeta en disco: {len(missing)}")

    if missing:
        ct_map = await build_ct_map(db, period)
        updated = await inv_repo.batch_update_folder_status(period.id, missing, "FALTANTE")
        await db.commit()
        yield plog("INFO", f"Marcadas como FALTANTE: {updated}")
        for name in missing:
            yield plog("WARN", "Sin carpeta en disco", folder=name, contract_type=ct_for_folder(name, ct_map))


@stage("MARK_UNKNOWN_DIRS")
async def _mark_unknown_dirs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Rename STAGE folders that match the invoice pattern but have no DB record.

    Folders are prefixed with '(DESCONOCIDO)'.  Already-marked folders are
    skipped so the stage is safe to re-run.
    """
    from app.core.inspector import FolderInspector
    from app.repositories.invoice_repo import InvoiceRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inv_repo = InvoiceRepo(db)
    known_numbers = await inv_repo.get_all_invoice_numbers(period.id)
    yield plog("INFO", f"Facturas en BD para el periodo: {len(known_numbers)}")

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    unknown_dirs = await executor(inspector.find_unknown_dirs, known_numbers)

    if not unknown_dirs:
        yield plog("INFO", "No hay carpetas desconocidas.")
        return

    yield plog("INFO", f"Carpetas desconocidas encontradas: {len(unknown_dirs)}")

    def _batch_rename(folders: list[Path]) -> tuple[list[str], list[str]]:
        renamed, failed = [], []
        for folder in folders:
            new_path = folder.parent / f"(DESCONOCIDO) {folder.name}"
            try:
                folder.rename(new_path)
                renamed.append(new_path.name)
            except OSError as exc:
                logger.warning("No se pudo renombrar %s: %s", folder.name, exc)
                failed.append(folder.name)
        return renamed, failed

    renamed, failed = await executor(_batch_rename, unknown_dirs)

    for name in renamed:
        yield plog("WARN", f"Carpeta desconocida: {name}")
    for name in failed:
        yield plog("ERROR", f"Error al renombrar: {name}")

    yield plog("INFO", f"Renombradas: {len(renamed)}, errores: {len(failed)}")


@stage("CHECK_REQUIRED_DOCS")
async def _check_required_docs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify required documents per service type; record findings for missing ones."""
    from app.core.inspector import FolderInspector

    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inv_repo = InvoiceRepo(db)
    rules_repo = RulesRepo(db)
    finding_repo = MissingFileRepo(db)

    std_map = await rules_repo.get_service_type_docs_map(institution.id)
    prefix_map = await rules_repo.get_active_doc_types_map()
    doc_types = await rules_repo.get_doc_types()
    dt_name: dict[int, str] = {dt.id: dt.description for dt in doc_types}

    presente_invoices = await inv_repo.get_invoices_by_status_code(period.id, "PRESENTE")
    yield plog("INFO", f"Facturas PRESENTE a verificar: {len(presente_invoices)}")

    id_prefix = institution.invoice_id_prefix or ""
    inspector = FolderInspector(stage_path, id_prefix)

    # Snapshot data needed in executor (avoids passing SQLAlchemy objects to thread)
    invoice_data = [(inv.id, inv.invoice_number, inv.service_type_id) for inv in presente_invoices]

    def _check_all_folders(
        inv_data: list[tuple[int, str, int | None]],
        on_progress: ProgressFn,
        cancel_event: threading.Event | None,
    ) -> dict[int, tuple[str, list[int]]]:
        """Returns {invoice_id: (invoice_number, [missing_doc_type_ids])}"""
        results: dict[int, tuple[str, list[int]]] = {}
        for index, (inv_id, inv_number, svc_type_id) in enumerate(inv_data, 1):
            if cancel_event is not None and cancel_event.is_set():
                break
            on_progress(index, len(inv_data), inv_number)
            required_dt_ids = std_map.get(svc_type_id, []) if svc_type_id is not None else []
            if not required_dt_ids:
                continue
            required_prefixes = {str(dt_id): prefix_map.get(dt_id, []) for dt_id in required_dt_ids}
            folder = stage_path / (id_prefix + inv_number)
            missing_codes = inspector.check_required_docs(folder, required_prefixes)
            if missing_codes:
                results[inv_id] = (inv_number, [int(c) for c in missing_codes])
        return results

    yield progress_line(0, len(invoice_data), label="Facturas")
    out: list = []
    async for line in run_with_progress(
        lambda on_progress, cancel: _check_all_folders(invoice_data, on_progress, cancel),
        ctx,
        out,
        label="Facturas",
    ):
        yield line
    findings_map = out[0]

    # Bulk upsert all findings in one DB call
    all_findings: list[tuple[int, int]] = [
        (inv_id, dt_id) for inv_id, (_, dt_ids) in findings_map.items() for dt_id in dt_ids
    ]
    total_findings = len(all_findings)
    await finding_repo.bulk_upsert_findings(all_findings)

    ct_map = await build_ct_map(db, period)
    invoices_with_findings = [inv_number for _, (inv_number, _) in findings_map.items()]
    for inv_id, (inv_number, dt_ids) in findings_map.items():
        ct = ct_for_folder(inv_number, ct_map)
        doc_str = (
            dt_name.get(dt_ids[0], str(dt_ids[0]))
            if len(dt_ids) == 1
            else ", ".join(dt_name.get(d, str(d)) for d in dt_ids)
        )
        yield plog("WARN", f"Faltantes ({doc_str})", folder=inv_number, contract_type=ct)

    if invoices_with_findings:
        updated = await inv_repo.batch_update_folder_status(period.id, invoices_with_findings, "PENDIENTE")
        yield plog("INFO", f"Facturas marcadas PENDIENTE: {updated}")

    await db.commit()
    yield plog("INFO", f"Hallazgos totales registrados: {total_findings}")


@stage("REVISAR_SOBRANTES")
async def _revisar_sobrantes(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Identify surplus files per invoice folder and suggest doc-type renames."""

    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.models.finding import MissingFile
    from app.models.institution import Agreement
    from app.models.invoice import Invoice
    from app.repositories.rules_repo import RulesRepo

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db: AsyncSession = ctx["db"]

    if not stage_path.exists():
        yield plog("ERROR", "Directorio STAGE no existe")
        return

    rules_repo = RulesRepo(db)
    std_map = await rules_repo.get_service_type_docs_map(institution.id)
    raw_prefix_map = await rules_repo.get_active_doc_types_map()
    prefix_map = {k: v[0] if v else None for k, v in raw_prefix_map.items()}

    q = (
        select(Invoice)
        .where(Invoice.audit_period_id == period.id)
        .options(
            selectinload(Invoice.agreement).selectinload(Agreement.contract_type),
            selectinload(Invoice.service_type),
            selectinload(Invoice.missing_files).selectinload(MissingFile.doc_type),
        )
    )
    result = await db.execute(q)
    invoices = list(result.scalars().all())
    invoice_by_number = {inv.invoice_number.upper(): inv for inv in invoices}

    # Snapshot invoice data to pass to executor (avoid passing SQLAlchemy ORM objects)
    invoice_snapshot = {
        num: {
            "id": inv.id,
            "invoice_number": inv.invoice_number,
            "service_type_id": inv.service_type_id,
            "admin_type": inv.agreement.contract_type.name if inv.agreement and inv.agreement.contract_type else None,
            "service_type": inv.service_type.display_name if inv.service_type else None,
            "faltantes": [
                {
                    "doc_type_id": mf.doc_type.id,
                    "code": mf.doc_type.code,
                    "description": mf.doc_type.description,
                    "prefix": mf.doc_type.prefix if hasattr(mf.doc_type, "prefix") else None,
                    "_prefix_for_suggestion": mf.doc_type.prefix if hasattr(mf.doc_type, "prefix") else None,
                }
                for mf in inv.missing_files
                if mf.resolved_at is None and mf.doc_type
            ],
        }
        for num, inv in invoice_by_number.items()
    }

    def _scan_folders(
        s_path: Path,
        inv_snap: dict,
        s_std_map: dict,
        s_prefix_map: dict,
        on_progress: ProgressFn,
        cancel_event: threading.Event | None,
    ) -> tuple[list[dict], list[str], int]:
        from difflib import SequenceMatcher as SM

        items_out: list[dict] = []
        log_lines: list[str] = []
        total = 0

        folders = sorted(d for d in s_path.iterdir() if d.is_dir())
        for index, folder in enumerate(folders, 1):
            if cancel_event is not None and cancel_event.is_set():
                break
            on_progress(index, len(folders), folder.name)

            folder_key = folder.name.upper()
            inv_data = inv_snap.get(folder_key)
            if inv_data is None:
                inv_data = next(
                    (
                        v
                        for k, v in inv_snap.items()
                        if folder_key == k
                        or folder_key.startswith(k + "_")
                        or folder_key.startswith(k + " ")
                        or folder_key.endswith(k)
                    ),
                    None,
                )
            if inv_data is None:
                continue

            required_dt_ids = s_std_map.get(inv_data["service_type_id"], [])
            required_prefixes = [
                s_prefix_map[dt_id] for dt_id in required_dt_ids if dt_id in s_prefix_map and s_prefix_map[dt_id]
            ]

            all_files = [f for f in folder.iterdir() if f.is_file()]
            sobrantes = [
                f for f in all_files if not any(f.name.upper().startswith(p.upper()) for p in required_prefixes)
            ]
            if not sobrantes:
                continue

            faltantes_data = inv_data["faltantes"]

            # Compute suggestions inline (reimplemented to avoid passing SequenceMatcher objects)
            suggestions: dict = {}
            if faltantes_data:
                if len(sobrantes) == 1 and len(faltantes_data) == 1:
                    ft = faltantes_data[0]
                    suggestions[sobrantes[0].name] = {
                        "suggested_doc_type_id": ft["doc_type_id"],
                        "suggested_code": ft["code"],
                        "suggested_prefix": ft.get("_prefix_for_suggestion"),
                        "confidence": "high",
                    }
                else:
                    for f in sobrantes:
                        best_score, best_ft = 0.0, None
                        for ft in faltantes_data:
                            prefix = ft.get("_prefix_for_suggestion") or ""
                            if not prefix:
                                continue
                            score = SM(None, f.name.upper(), prefix.upper()).ratio()
                            if score > best_score:
                                best_score, best_ft = score, ft
                        if best_ft and best_score > 0.3:
                            suggestions[f.name] = {
                                "suggested_doc_type_id": best_ft["doc_type_id"],
                                "suggested_code": best_ft["code"],
                                "suggested_prefix": best_ft.get("_prefix_for_suggestion"),
                                "confidence": "low",
                            }

            items_out.append(
                {
                    "invoice_id": inv_data["id"],
                    "invoice_number": inv_data["invoice_number"],
                    "admin_type": inv_data["admin_type"],
                    "service_type": inv_data["service_type"],
                    "folder_path": str(folder),
                    "sobrantes": [
                        {
                            "filename": f.name,
                            **suggestions.get(
                                f.name,
                                {
                                    "suggested_doc_type_id": None,
                                    "suggested_code": None,
                                    "suggested_prefix": None,
                                    "confidence": None,
                                },
                            ),
                        }
                        for f in sobrantes
                    ],
                    "faltantes": [
                        {"doc_type_id": ft["doc_type_id"], "code": ft["code"], "description": ft["description"]}
                        for ft in faltantes_data
                    ],
                }
            )
            total += len(sobrantes)
            log_lines.append(
                plog(
                    "INFO",
                    f"{len(sobrantes)} sobrante(s), {len(faltantes_data)} faltante(s)",
                    folder=folder.name,
                    contract_type=inv_data.get("admin_type"),
                )
            )

        return items_out, log_lines, total

    out: list = []
    async for line in run_with_progress(
        lambda on_progress, cancel: _scan_folders(
            stage_path, invoice_snapshot, std_map, prefix_map, on_progress, cancel
        ),
        ctx,
        out,
        label="Carpetas",
    ):
        yield line
    items, log_lines, total_sobrantes = out[0]

    for line in log_lines:
        yield line

    yield plog("INFO", f"Total: {total_sobrantes} archivos sobrantes en {len(items)} carpetas")
    if items:
        yield f"[DATA] {json.dumps(items, ensure_ascii=False)}"
    else:
        yield plog("INFO", "No se encontraron archivos sobrantes")


@stage("VERIFY_CUFE")
async def _verify_cufe(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Verify CUFE presence and tag folders missing a CUFE in invoice PDFs."""
    from app.core.ops import DocumentOps
    from app.core.scanner import DocumentScanner
    from app.core.validator import InvoiceValidator

    from app.repositories.rules_repo import RulesRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    rules_repo = RulesRepo(db)
    invoice_doc = await rules_repo.get_doc_type_by_code("FACTURA")
    invoice_prefix = invoice_doc.prefix if invoice_doc and invoice_doc.prefix else (institution.invoice_id_prefix or "")

    scanner = DocumentScanner(stage_path)
    invoices = await executor(scanner.find_by_prefix, invoice_prefix)

    validator = InvoiceValidator(stage_path, institution.invoice_id_prefix or "")
    yield progress_line(0, len(invoices), label="Facturas")
    out: list = []
    async for line in run_with_progress(
        lambda on_progress, cancel: validator.validate_invoice_files(invoices, on_progress, cancel),
        ctx,
        out,
        label="Facturas",
    ):
        yield line
    _, missing_cufe = out[0]
    for f in missing_cufe:
        yield plog("WARN", f"Sin CUFE: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin CUFE: {len(missing_cufe)}")

    if missing_cufe:
        ops = DocumentOps(stage_path, institution.invoice_id_prefix or "")
        marked = await executor(ops.tag_dirs_missing_cufe, missing_cufe)
        yield plog("INFO", f"Carpetas marcadas sin CUFE: {marked}")
