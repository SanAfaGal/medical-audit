"""Stages that check the folders of STAGE against the invoices in the database: names, missing and unknown folders."""

from __future__ import annotations

from typing import Any
import json
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import build_ct_map, ct_for_folder, get_executor
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage

logger = logging.getLogger(__name__)


@stage("CHECK_FOLDERS_WITH_EXTRA_TEXT", requires="stage")
async def _check_folders_with_extra_text(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Detect folders with extra text; send interactive payload for auditor review."""

    from sqlalchemy import select

    from app.core.inspector import FolderInspector
    from app.models.invoice import Invoice

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db: AsyncSession = ctx["db"]

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
    payload: list[dict[str, Any]] = []
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


@stage("NORMALIZE_DIR_NAMES", requires="stage")
async def _normalize_dir_names(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Rename folders with extra text to their canonical identifier."""
    from app.core.inspector import FolderInspector
    from app.core.ops import DocumentOps

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    malformed = await executor(inspector.find_malformed_dirs)
    yield plog("INFO", f"Carpetas malformadas encontradas: {len(malformed)}")

    if malformed:
        ops = DocumentOps(stage_path, institution.invoice_id_prefix or "")
        renamed = await executor(ops.standardize_dir_names, malformed)
        yield plog("INFO", f"Carpetas renombradas: {renamed}")


@stage("CHECK_DIRS", requires="stage")
async def _check_dirs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Compare invoice numbers in DB against folders on disk; mark missing ones FALTANTE."""
    from app.core.inspector import FolderInspector
    from app.repositories.invoice_repo import InvoiceRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

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


@stage("MARK_UNKNOWN_DIRS", requires="stage")
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
