"""Staging and file clean-up stages: copy from DRIVE, flatten, drop non-PDFs, normalise names, unreadable PDFs."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
import asyncio
import json
import shutil

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import build_ct_map, ct_for_folder, get_executor
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage


@stage("RUN_STAGING")
async def _run_staging(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Move leaf folders from DRIVE to STAGE."""
    from app.core.organizer import FolderCopier, LeafFolderFinder

    executor = get_executor()
    drive_path: Path = ctx["drive_path"]
    stage_path: Path = ctx["stage_path"]

    if not drive_path.is_dir():
        yield plog("ERROR", f"Directorio DRIVE no existe: {drive_path}")
        return

    yield plog("INFO", f"Buscando carpetas hoja en {drive_path}...")
    leaf_finder = LeafFolderFinder()
    leaf_folders = await executor(leaf_finder.find_leaf_folders, drive_path)
    yield plog("INFO", f"Carpetas hoja encontradas: {len(leaf_folders)}")

    if leaf_folders:
        copier = FolderCopier(stage_path)
        await executor(copier.move_folders, leaf_folders, False)
        yield plog("INFO", f"Carpetas movidas a STAGE: {len(leaf_folders)}")
    else:
        yield plog("WARN", "No se encontraron carpetas hoja en DRIVE.")


@stage("CHECK_NESTED_FOLDERS")
async def _check_nested_folders(ctx: StageContext) -> AsyncGenerator[str, None]:
    """List invoice-level folders in STAGE that contain nested subdirectories."""
    stage_path: Path = ctx["stage_path"]

    if not stage_path.is_dir():
        yield plog("ERROR", f"Directorio STAGE no existe: {stage_path}")
        return

    def _find_nested(path: Path) -> list[tuple[str, list[str]]]:
        result = []
        for d in sorted(path.iterdir()):
            if d.is_dir():
                subs = [s.name for s in sorted(d.iterdir()) if s.is_dir()]
                if subs:
                    result.append((d.name, subs))
        return result

    executor = get_executor()
    nested = await executor(_find_nested, stage_path)

    if not nested:
        yield plog("INFO", "No se encontraron carpetas con subcarpetas anidadas en STAGE.")
        return

    yield plog("WARN", f"{len(nested)} carpeta(s) con subcarpetas anidadas detectadas en STAGE:")
    for name, subs in nested:
        yield plog("WARN", f"subcarpetas: {', '.join(subs)}", folder=name)
    yield plog(
        "WARN", "Revisa y aplana estas carpetas para que los archivos queden directamente en la carpeta de factura."
    )


@stage("REMOVE_NON_PDF")
async def _remove_non_pdf(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Scan STAGE for non-PDF files and corrupt PDFs; emit [DATA] for user review."""
    from app.core.ops import IMAGE_EXTENSIONS
    from app.core.reader import DocumentReader

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    def _scan(s_path: Path) -> tuple[list[dict], list[Path]]:
        """Single rglob pass → returns (non_pdf_metadata, pdf_candidates)."""
        non_pdfs: list[dict] = []
        pdf_candidates: list[Path] = []
        for f in s_path.rglob("*"):
            if not f.is_file():
                continue
            if f.suffix.lower() != ".pdf":
                ext = f.suffix.lstrip(".").lower()
                try:
                    size_kb = round(f.stat().st_size / 1024, 1)
                except OSError:
                    size_kb = 0.0
                non_pdfs.append(
                    {
                        "rel_path": f.relative_to(s_path).as_posix(),
                        "filename": f.name,
                        "extension": ext,
                        "size_kb": size_kb,
                        "is_image": ext in IMAGE_EXTENSIONS,
                    }
                )
            else:
                pdf_candidates.append(f)
        return non_pdfs, pdf_candidates

    non_pdf_list, pdf_candidates = await executor(_scan, stage_path)

    # Check each PDF for corruption in chunks to report intermediate progress.
    _PDF_CHECK_CHUNK = 50
    can_open_results: list[bool] = []
    if pdf_candidates:
        total_pdfs = len(pdf_candidates)
        for chunk_start in range(0, total_pdfs, _PDF_CHECK_CHUNK):
            chunk = pdf_candidates[chunk_start : chunk_start + _PDF_CHECK_CHUNK]
            chunk_results = await asyncio.gather(*[executor(DocumentReader._can_open, f) for f in chunk])
            can_open_results.extend(chunk_results)
            processed = min(chunk_start + _PDF_CHECK_CHUNK, total_pdfs)
            yield plog("INFO", f"Verificando PDFs: {processed}/{total_pdfs}")

    corrupt_list: list[dict] = []
    for f, ok in zip(pdf_candidates, can_open_results):
        if not ok:
            try:
                size_kb = round(f.stat().st_size / 1024, 1)
            except OSError:
                size_kb = 0.0
            corrupt_list.append(
                {
                    "rel_path": f.relative_to(stage_path).as_posix(),
                    "filename": f.name,
                    "size_kb": size_kb,
                }
            )

    yield plog("INFO", f"Archivos no-PDF encontrados: {len(non_pdf_list)}")
    yield plog("INFO", f"PDFs corruptos encontrados: {len(corrupt_list)}")

    if not non_pdf_list and not corrupt_list:
        yield plog("INFO", "No se encontraron archivos problemáticos — STAGE limpio")
        return

    data = {
        "stage": "REMOVE_NON_PDF",
        "non_pdf": non_pdf_list,
        "corrupt_pdfs": corrupt_list,
    }
    yield f"[DATA] {json.dumps(data, ensure_ascii=False)}"


def apply_prefix_corrections(
    stage_path: Path, corrections: dict[str, str]
) -> tuple[int, list[tuple[str, str, str]], list[str]]:
    """Rename PDFs whose prefix matches a known wrong prefix to the correct one.

    Returns ``(renamed_count, [(folder_name, old_name, new_name)], [error_messages])``.
    Per-file failures (permission, name collision) are logged and skipped instead
    of aborting the whole batch — matching the pattern already used by
    ``app.core.ops.DocumentOps``.
    """
    renamed = 0
    renames: list[tuple[str, str, str]] = []
    errors: list[str] = []

    for pdf in stage_path.rglob("*.pdf"):
        stem = pdf.stem
        for wrong, correct in corrections.items():
            if stem.upper().startswith(wrong.upper()):
                remainder = stem[len(wrong) :]
                new_name = correct + remainder + pdf.suffix
                new_path = pdf.parent / new_name
                if new_path.exists():
                    errors.append(f"{pdf.name}: destino ya existe ({new_name})")
                    break
                try:
                    pdf.rename(new_path)
                except OSError as exc:
                    errors.append(f"{pdf.name}: {exc}")
                    break
                renamed += 1
                renames.append((pdf.parent.name, pdf.name, new_name))
                break

    return renamed, renames, errors


@stage("NORMALIZE_FILES")
async def _normalize_files(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Rename files with invalid names to the canonical standard.

    Step 1 — apply prefix corrections from the prefix_corrections table
              (e.g. OPD → OPF, FVE → FEV).
    Step 2 — run the generic FilenameStandardizer for remaining invalid names.
    """
    from app.core.scanner import DocumentScanner
    from app.core.standardizer import FilenameStandardizer

    from app.repositories.rules_repo import RulesRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    db = ctx["db"]
    period = ctx.get("period")

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    rules_repo = RulesRepo(db)

    # -- Step 1: prefix corrections --
    corrections = await rules_repo.get_prefix_corrections_map()
    if corrections:
        renamed, renames, correction_errors = await executor(apply_prefix_corrections, stage_path, corrections)
        if renamed:
            for folder, old_name, new_name in renames:
                yield plog("INFO", f"{old_name} → {new_name}", folder=folder)
            yield plog("INFO", f"Correcciones de prefijo aplicadas: {renamed} archivo(s)")
        else:
            yield plog("INFO", "Correcciones de prefijo aplicadas: 0 archivo(s)")
        for err in correction_errors:
            yield plog("ERROR", f"Corrección de prefijo falló: {err}")
    else:
        yield plog("INFO", "Sin correcciones de prefijo configuradas")

    # -- Step 2: generic standardization --
    prefixes = await rules_repo.get_all_active_doc_type_prefixes()

    scanner = DocumentScanner(stage_path)
    invalid = await executor(scanner.find_invalid_names, prefixes, institution.invoice_id_prefix or "", institution.nit)
    yield plog("INFO", f"Archivos con nombre inválido: {len(invalid)}")

    if invalid:
        standardizer = FilenameStandardizer(
            nit=institution.nit,
            valid_prefixes=prefixes,
            suffix_const=institution.invoice_id_prefix or "",
        )
        results = await executor(standardizer.run, invalid)

        # Build invoice_number → contract_type_name map for the period (one query)
        ct_map = await build_ct_map(db, period) if period else {}

        success = 0
        for r in results:
            original_path = Path(r.original_path)
            folder = original_path.parent.name
            old_name = original_path.name
            ct = ct_for_folder(folder, ct_map)
            if r.status == "SUCCESS":
                success += 1
                yield plog("INFO", f"{old_name} → {r.new_name}", folder=folder, contract_type=ct)
            elif r.status == "REJECTED":
                if "Could not find" in r.reason:
                    yield plog("WARN", f"Sin ID de factura extraíble: {old_name}", folder=folder, contract_type=ct)
                elif "already exists" in r.reason:
                    yield plog("WARN", f"Destino ya existe: {old_name} → {r.new_name}", folder=folder, contract_type=ct)
                else:
                    # Covers empty prefix (starts with digit) and unrecognised prefixes
                    yield plog("WARN", f"Prefijo no reconocido: {old_name}", folder=folder, contract_type=ct)
            elif r.status == "ERROR":
                yield plog("ERROR", f"No se pudo renombrar: {old_name} — {r.reason}", folder=folder, contract_type=ct)
        yield plog("INFO", f"Archivos renombrados: {success}/{len(invalid)}")


@stage("LIST_UNREADABLE_PDFS")
async def _list_unreadable_pdfs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """List invoice PDFs without a text layer (scanned, unreadable)."""
    from app.core.reader import DocumentReader
    from app.core.scanner import DocumentScanner

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
    yield plog("INFO", f"PDFs de factura encontrados: {len(invoices)}")

    no_text = await executor(DocumentReader.find_needing_ocr, invoices)
    for f in no_text:
        yield plog("WARN", f"Sin capa de texto: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin texto: {len(no_text)}")


@stage("DELETE_UNREADABLE_PDFS")
async def _delete_unreadable_pdfs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Move invoice PDFs without a text layer to a quarantine folder (reversible).

    Files are moved to ``STAGE/_CUARENTENA_SIN_TEXTO/<carpeta>/<archivo>`` instead
    of being permanently deleted: the "no text layer" heuristic only inspects the
    first page (see core/reader.py) and can misfire on a scanned cover followed by
    real text pages, so an irreversible delete here is too risky for audit data.
    """
    from app.core.reader import DocumentReader
    from app.core.scanner import DocumentScanner

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
    no_text = await executor(DocumentReader.find_needing_ocr, invoices)
    yield plog("INFO", f"PDFs sin texto a mover a cuarentena: {len(no_text)}")

    quarantine_root = stage_path / "_CUARENTENA_SIN_TEXTO"

    def _quarantine(f: Path) -> Path:
        dest_dir = quarantine_root / f.parent.name
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f.name
        shutil.move(str(f), str(dest))
        return dest

    moved = 0
    for f in no_text:
        try:
            dest = await executor(_quarantine, f)
            moved += 1
            yield plog("INFO", f"Movido a cuarentena: {f.name} → {dest.relative_to(stage_path)}", folder=f.parent.name)
        except OSError as exc:
            yield plog("ERROR", f"No se pudo mover a cuarentena {f.name}: {exc}", folder=f.parent.name)
    yield plog("INFO", f"Total movidos a cuarentena: {moved}")
