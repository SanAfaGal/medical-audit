"""Final stages: move audited folders to AUDIT, compress PDFs and prepare the export."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncGenerator
from pathlib import Path

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import build_ct_map, ct_for_folder, get_executor
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.stage_events import progress_line


_MOVE_CHUNK = 25  # folders per progress report


@stage("ORGANIZE", requires="stage")
async def _organize(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Move eligible invoices (PRESENTE + no findings) to AUDIT/ADMIN[/CONTRACT]/FOLDER."""
    from app.core.helpers import safe_move

    from app.repositories.invoice_repo import InvoiceRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    audit_path: Path = ctx["audit_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    inv_repo = InvoiceRepo(db)
    organizable = await inv_repo.get_organizable_invoices(period.id)
    yield plog("INFO", f"Facturas organizables: {len(organizable)}")

    if not organizable:
        yield plog("INFO", "Nada que organizar.")
        return

    ct_map = await build_ct_map(db, period)

    # Index staging folders once (case-insensitive) for fast lookup
    def _build_staging_index(stage: Path) -> dict[str, Path]:
        return {p.name.upper(): p for p in stage.iterdir() if p.is_dir()}

    staging_index = await executor(_build_staging_index, stage_path)
    id_prefix = (institution.invoice_id_prefix or "").upper()

    # Build all (source, dest, invoice_number) triples first — no I/O yet
    planned: list[tuple[Path, Path, str]] = []
    not_found = 0

    for inv in organizable:
        folder_name = (institution.invoice_id_prefix or "") + inv.invoice_number
        source = staging_index.get(folder_name.upper())
        if source is None:
            key = folder_name.upper()
            source = next(
                (
                    p
                    for name, p in staging_index.items()
                    if (name.startswith(key) and (len(name) == len(key) or not name[len(key)].isalnum()))
                    or (id_prefix and name.endswith(inv.invoice_number.upper()))
                ),
                None,
            )
        if source is None:
            ct = ct_for_folder(inv.invoice_number, ct_map)
            yield plog("WARN", f"Carpeta no encontrada en STAGE: {folder_name}", contract_type=ct)
            not_found += 1
            continue

        ic = inv.agreement
        if ic and ic.administrator and ic.administrator.canonical_name:
            admin_name = (
                f"{ic.administrator.canonical_name} ({ic.contract_type.name})"
                if ic.contract_type
                else ic.administrator.canonical_name
            )
        else:
            admin_name = "SIN ADMINISTRADORA"
        contract_name = ic.contract.canonical_name if ic and ic.contract and ic.contract.canonical_name else None
        dest = (
            audit_path / admin_name / contract_name / source.name
            if contract_name
            else audit_path / admin_name / source.name
        )
        planned.append((source, dest, inv.invoice_number))

    def _batch_move(
        moves: list[tuple[Path, Path, str]],
    ) -> tuple[list[tuple[str, str]], list[str]]:
        """Returns ([(src_name, rel_dest), ...], [failed_src_names])."""
        moved, failed = [], []
        for src, dest, _ in moves:
            if safe_move(src, dest):
                moved.append((src.name, str(dest.relative_to(audit_path))))
            else:
                failed.append(src.name)
        return moved, failed

    # Move in chunks so the UI progress bar advances while the disk work runs in a thread.
    moved_pairs: list[tuple[str, str]] = []
    failed_names: list[str] = []
    total_moves = len(planned)
    yield progress_line(0, total_moves, label="Facturas")
    for start in range(0, total_moves, _MOVE_CHUNK):
        chunk_moved, chunk_failed = await executor(_batch_move, planned[start : start + _MOVE_CHUNK])
        moved_pairs.extend(chunk_moved)
        failed_names.extend(chunk_failed)
        done = min(start + _MOVE_CHUNK, total_moves)
        yield plog("INFO", f"Moviendo carpetas: {done}/{total_moves}")
        yield progress_line(done, total_moves, label="Facturas")

    # Derive moved_numbers from planned vs failed
    failed_set = set(failed_names)
    moved_numbers = [inv_number for src, _, inv_number in planned if src.name not in failed_set]

    for src_name, rel_dest in moved_pairs:
        ct = ct_for_folder(src_name, ct_map)
        yield plog("INFO", f"Movida: {src_name} → {rel_dest}", contract_type=ct)
    for src_name in failed_names:
        ct = ct_for_folder(src_name, ct_map)
        yield plog("WARN", f"Error al mover: {src_name}", contract_type=ct)

    yield plog("INFO", f"Movidas: {len(moved_numbers)}, no encontradas: {not_found}, fallidas: {len(failed_names)}")

    if moved_numbers:
        updated = await inv_repo.batch_update_to_auditada(period.id, moved_numbers)
        await db.commit()
        yield plog("INFO", f"Facturas marcadas AUDITADA: {updated}")


_COMPRESS_CHUNK = 50  # files per progress report


@stage("COMPRESS_AUDIT", requires="audit")
async def _compress_audit(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Compress all PDFs in AUDIT using Ghostscript (ebook quality)."""
    import os

    from app.core.processor import DocumentProcessor

    loop = asyncio.get_running_loop()
    audit_path: Path = ctx["audit_path"]

    all_pdfs = await loop.run_in_executor(None, lambda: list(audit_path.rglob("*.pdf")))
    yield plog("INFO", f"PDFs encontrados en AUDIT: {len(all_pdfs)}")

    if not all_pdfs:
        yield plog("INFO", "No hay PDFs que comprimir.")
        return

    pdfs = await loop.run_in_executor(
        None, lambda: [f for f in all_pdfs if not DocumentProcessor.is_ghostscript_compressed(f)]
    )
    skipped = len(all_pdfs) - len(pdfs)
    if skipped:
        yield plog("INFO", f"Ya comprimidos (omitidos): {skipped}")

    if not pdfs:
        yield plog("INFO", "Todos los PDFs ya estaban comprimidos.")
        return

    total = len(pdfs)
    workers = max(os.cpu_count() or 4, 4)
    yield plog("INFO", f"Por comprimir: {total} — paralelismo: {workers} workers")

    totals: dict[str, int] = {"success": 0, "failed": 0, "bytes_before": 0, "bytes_after": 0}
    chunks = [pdfs[i : i + _COMPRESS_CHUNK] for i in range(0, total, _COMPRESS_CHUNK)]
    yield progress_line(0, total, label="PDFs")

    for i, chunk in enumerate(chunks, 1):
        result: dict[str, int] = await loop.run_in_executor(
            None, functools.partial(DocumentProcessor.batch_compress, chunk, "ebook", workers)
        )
        for key in totals:
            totals[key] += result[key]
        processed = min(i * _COMPRESS_CHUNK, total)
        yield plog(
            "INFO",
            f"Progreso: {processed}/{total} ({processed / total * 100:.0f}%) — éxitos: {totals['success']}, fallos: {totals['failed']}",
        )
        yield progress_line(processed, total, label="PDFs")

    yield plog("INFO", f"Compresión completada — exitosos: {totals['success']}, fallidos: {totals['failed']}")

    if totals["bytes_before"]:
        before_mb = totals["bytes_before"] / (1024 * 1024)
        after_mb = totals["bytes_after"] / (1024 * 1024)
        saved_mb = before_mb - after_mb
        pct = saved_mb / before_mb * 100
        yield plog("INFO", f"Tamaño: {before_mb:.1f} MB → {after_mb:.1f} MB  (ahorro {saved_mb:.1f} MB · {pct:.1f}%)")

    if totals["failed"]:
        yield plog(
            "WARN",
            f"{totals['failed']} PDF(s) no pudieron comprimirse (Ghostscript no instalado o error en archivo)",
        )


@stage("EXPORTAR_AUDITADOS", requires="audit")
async def _exportar_auditados(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Prepara la exportación de AUDIT: valida y cuenta archivos.
    El ZIP se genera al vuelo en el endpoint /api/pipeline/stream-audit-zip."""
    audit_path: Path = ctx["audit_path"]

    all_files = await asyncio.to_thread(lambda: [f for f in audit_path.rglob("*") if f.is_file()])
    if not all_files:
        yield plog("INFO", "No hay archivos en AUDIT para exportar.")
        return

    total_mb = await asyncio.to_thread(lambda: sum(f.stat().st_size for f in all_files)) / (1024 * 1024)
    yield plog("INFO", f"{len(all_files)} archivos listos para descargar ({total_mb:.1f} MB)")
    yield plog("DATA", f"export_ready:{len(all_files)}")
