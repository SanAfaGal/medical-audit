"""PDF quality stages: blank documents, documents without a text layer, and the quarantine they share."""

from __future__ import annotations

import shutil
from collections.abc import AsyncGenerator
from pathlib import Path

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import get_executor, invoice_pdfs
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.stage_events import ProgressJob, progress_line


@stage("LIST_UNREADABLE_PDFS", requires="stage")
async def _list_unreadable_pdfs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """List invoice PDFs without a text layer (scanned, unreadable)."""
    from app.core.reader import DocumentReader

    invoices = await invoice_pdfs(ctx)
    yield plog("INFO", f"PDFs de factura encontrados: {len(invoices)}")

    job = ProgressJob(
        lambda on_progress, cancel: DocumentReader.find_needing_ocr(invoices, on_progress, cancel),
        ctx,
        label="PDFs",
        total=len(invoices),
    )
    async for line in job:
        yield line
    no_text = job.result
    for f in no_text:
        yield plog("WARN", f"Sin capa de texto: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin texto: {len(no_text)}")


_BLANK_QUARANTINE = "_CUARENTENA_EN_BLANCO"


class _Quarantine:
    """Moves files to ``STAGE/<root_name>/<carpeta>/<archivo>`` (reversible) and logs each move.

    ``async for line in quarantine.move(files)`` yields the progress and log lines; afterwards ``moved`` holds how
    many files went through. A file that cannot be moved is an ``[ERROR]`` line and the rest carry on.
    """

    def __init__(self, stage_path: Path, root_name: str) -> None:
        self._stage_path = stage_path
        self._root = stage_path / root_name
        self.moved = 0

    def _move_one(self, f: Path) -> Path:
        dest_dir = self._root / f.parent.name
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f.name
        shutil.move(str(f), str(dest))
        return dest

    async def move(self, files: list[Path]) -> AsyncGenerator[str, None]:
        executor = get_executor()
        for index, f in enumerate(files, 1):
            yield progress_line(index - 1, len(files), f.name, "PDFs")
            try:
                dest = await executor(self._move_one, f)
            except OSError as exc:
                yield plog("ERROR", f"No se pudo mover a cuarentena {f.name}: {exc}", folder=f.parent.name)
                continue
            self.moved += 1
            yield plog(
                "INFO",
                f"Movido a cuarentena: {f.name} → {dest.relative_to(self._stage_path)}",
                folder=f.parent.name,
            )


def _stage_pdfs(stage_path: Path) -> list[Path]:
    """Every PDF under STAGE except the ones already moved to a ``_CUARENTENA*`` folder."""
    from app.core.scanner import DocumentScanner

    return [
        f
        for f in DocumentScanner(stage_path).find_by_extension("pdf")
        if not f.relative_to(stage_path).parts[0].startswith("_CUARENTENA")
    ]


@stage("DETECT_BLANK_DOCS", requires="stage")
async def _detect_blank_docs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Find blank PDFs of any document type, quarantine them and record a finding so they get fetched again.

    Files go to ``STAGE/_CUARENTENA_EN_BLANCO/<carpeta>/<archivo>`` (reversible). When the folder is an invoice and
    the file prefix is a known document type, a ``MissingFile`` finding is stored: the invoice turns PENDIENTE and
    the download stages treat it as a document to fetch again. Anything else is only reported and quarantined.
    """
    import re

    from app.core.inspector import FolderInspector
    from app.core.reader import DocumentReader
    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    period = ctx["period"]
    db = ctx["db"]

    files = await executor(_stage_pdfs, stage_path)
    yield plog("INFO", f"PDFs a revisar: {len(files)}")

    job = ProgressJob(
        lambda on_progress, cancel: DocumentReader.find_blank(files, on_progress, cancel),
        ctx,
        label="PDFs",
        total=len(files),
    )
    async for line in job:
        yield line
    blanks = job.result
    for f in blanks:
        yield plog("WARN", f"PDF en blanco: {f.name}", folder=f.parent.name)
    if not blanks:
        yield plog("INFO", "No se encontraron PDFs en blanco")
        return

    # Which (invoice, document type) does each blank file stand for?
    inv_repo = InvoiceRepo(db)
    ids_by_number = await inv_repo.get_ids_by_number(period.id)
    type_by_prefix = {
        prefix.upper(): dt_id
        for dt_id, prefixes in (await RulesRepo(db).get_active_doc_types_map()).items()
        for prefix in prefixes
    }
    inspector = FolderInspector(stage_path, ctx["institution"].invoice_id_prefix or "")
    findings: set[tuple[int, int]] = set()
    numbers: set[str] = set()
    for f in blanks:
        number = inspector.extract_invoice_number(f.parent.name)
        prefix = re.match(r"[A-Za-z]+", f.name)
        invoice_id = ids_by_number.get(number) if number else None
        doc_type_id = type_by_prefix.get(prefix.group(0).upper()) if prefix else None
        if number and invoice_id is not None and doc_type_id is not None:
            findings.add((invoice_id, doc_type_id))
            numbers.add(number)
        else:
            yield plog(
                "WARN", f"Sin hallazgo (factura o tipo de documento no reconocidos): {f.name}", folder=f.parent.name
            )

    if findings:
        await MissingFileRepo(db).bulk_upsert_findings(sorted(findings))
        updated = await inv_repo.batch_update_folder_status(period.id, sorted(numbers), "PENDIENTE")
        await db.commit()
        yield plog("INFO", f"Hallazgos registrados: {len(findings)} (facturas marcadas PENDIENTE: {updated})")

    quarantine = _Quarantine(stage_path, _BLANK_QUARANTINE)
    async for line in quarantine.move(blanks):
        yield line
    yield plog("INFO", f"PDFs en blanco: {len(blanks)}, movidos a cuarentena: {quarantine.moved}")


@stage("DELETE_UNREADABLE_PDFS", requires="stage")
async def _delete_unreadable_pdfs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Move invoice PDFs without a text layer to a quarantine folder (reversible).

    Files are moved to ``STAGE/_CUARENTENA_SIN_TEXTO/<carpeta>/<archivo>`` instead
    of being permanently deleted: the "no text layer" heuristic only inspects the
    first page (see core/reader.py) and can misfire on a scanned cover followed by
    real text pages, so an irreversible delete here is too risky for audit data.
    """
    from app.core.reader import DocumentReader

    stage_path: Path = ctx["stage_path"]

    invoices = await invoice_pdfs(ctx)
    job = ProgressJob(
        lambda on_progress, cancel: DocumentReader.find_needing_ocr(invoices, on_progress, cancel),
        ctx,
        label="PDFs",
        total=len(invoices),
    )
    async for line in job:
        yield line
    no_text = job.result
    yield plog("INFO", f"PDFs sin texto a mover a cuarentena: {len(no_text)}")

    quarantine = _Quarantine(stage_path, "_CUARENTENA_SIN_TEXTO")
    async for line in quarantine.move(no_text):
        yield line
    yield plog("INFO", f"Total movidos a cuarentena: {quarantine.moved}")
