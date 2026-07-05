"""Pipeline runner: async generator that executes stages and yields log lines."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
from collections.abc import AsyncGenerator, Callable
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.paths import audit_data_root

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sihos_excel_name(institution: Institution, period: AuditPeriod) -> str:
    """Return the canonical filename for a saved SIHOS Excel: SIHOS_{name}_{period}.xlsx"""
    name = re.sub(r"[^\w\-]", "_", institution.name).strip("_")
    return f"SIHOS_{name}_{period.period_label}.xlsx"


# ---------------------------------------------------------------------------
# Stage registry
# ---------------------------------------------------------------------------

_STAGE_HANDLERS: dict[str, _StageHandler] = {}


def _stage(name: str):
    """Decorator to register a stage handler."""

    def decorator(fn):
        _STAGE_HANDLERS[name] = fn
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

_StageHandler = Callable[..., AsyncGenerator[str, None]]


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------


def _build_context(
    institution: Institution, period: AuditPeriod, db: AsyncSession, extra: dict
) -> dict:
    base = audit_data_root / institution.name / period.period_label
    return {
        "institution": institution,
        "period": period,
        "db": db,
        "base_path": base,
        "drive_path": base / "DRIVE",
        "stage_path": base / "STAGE",
        "audit_path": base / "AUDIT",
        **extra,
    }


# ---------------------------------------------------------------------------
# Executor helper
# ---------------------------------------------------------------------------


def _get_executor():
    """Return a callable that runs a sync function in the default thread executor."""
    loop = asyncio.get_running_loop()

    async def run(fn, *args, **kwargs):
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    return run


# ---------------------------------------------------------------------------
# Log formatting helpers
# ---------------------------------------------------------------------------


def plog(
    level: str,
    msg: str,
    *,
    folder: str | None = None,
    contract_type: str | None = None,
) -> str:
    """Format a pipeline log line.

    Usage:
        plog("INFO", "Facturas cargadas: 154")
        plog("WARN", "PDF sin texto", folder="FAC-001234", contract_type="CAPITACIÓN")
        plog("ERROR", "Directorio no existe")
    """
    parts = [f"[{level}]"]
    if contract_type:
        parts.append(f"[{contract_type}]")
    if folder:
        parts.append(f"[{folder}]")
    parts.append(msg)
    return " ".join(parts)


async def _build_ct_map(db, period) -> dict[str, str]:
    """Return {invoice_number: contract_type_name} for the period."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.models.invoice import Invoice
    from app.models.institution import Agreement

    q = (
        select(Invoice)
        .where(Invoice.audit_period_id == period.id)
        .options(selectinload(Invoice.agreement).selectinload(Agreement.contract_type))
    )
    rows = (await db.execute(q)).scalars().all()
    return {
        inv.invoice_number: inv.agreement.contract_type.name
        for inv in rows
        if inv.agreement and inv.agreement.contract_type
    }


def _ct_for_folder(folder: str, ct_map: dict[str, str]) -> str | None:
    """Resolve contract_type name for a folder (exact match, then substring)."""
    if folder in ct_map:
        return ct_map[folder]
    for num, ct in ct_map.items():
        if num in folder:
            return ct
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def execute(
    stage: str,
    institution: Institution,
    period: AuditPeriod,
    db: AsyncSession,
    extra: dict | None = None,
) -> AsyncGenerator[str, None]:
    """Yield log lines as a pipeline stage executes."""
    ctx = _build_context(institution, period, db, extra or {})
    handler_fn = _STAGE_HANDLERS.get(stage)

    if handler_fn is None:
        yield f"[ERROR] Etapa desconocida: {stage}"
        return

    try:
        async for line in handler_fn(ctx):
            yield line
    except Exception as exc:
        logger.exception("Pipeline stage %s failed", stage)
        exc_desc = f"{type(exc).__name__}: {exc}" if str(exc).strip() else type(exc).__name__
        yield f"[ERROR] {stage} falló: {exc_desc}"


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------


@_stage("LOAD_AND_PROCESS")
async def _load_and_process(ctx: dict) -> AsyncGenerator[str, None]:
    """Load SIHOS Excel and upsert invoices. Requires ctx['file_bytes'] and ctx['period_code']."""
    from app.services.billing import ingest

    file_bytes: bytes | None = ctx.get("file_bytes")

    if not file_bytes:
        yield plog("ERROR", "No se proporcionó el archivo Excel de SIHOS.")
        return

    yield plog("INFO", "Leyendo Excel SIHOS...")
    result = await ingest(file_bytes, ctx["institution"], ctx["period"].id, ctx["db"])
    yield plog("INFO", f"Insertadas: {result['inserted']} facturas")
    if result["skipped"]:
        yield plog("WARN", f"Omitidas (admin sin mapear): {result['skipped']}")
    for admin in result["unknown_admins"]:
        yield plog("WARN", f"Administradora sin mapear: {admin}")
    for contract in result["unknown_contracts"]:
        yield plog("WARN", f"Contrato sin mapear: {contract}")
    for service in result["unknown_services"]:
        yield plog("WARN", f"Servicio sin reclasificar (GENERAL): {service}")

    # Guardar el Excel para permitir re-categorización posterior
    base_path: Path = ctx["base_path"]
    executor = _get_executor()

    def _save_excel(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    excel_name = _sihos_excel_name(ctx["institution"], ctx["period"])
    await executor(_save_excel, base_path / excel_name, file_bytes)
    yield plog("INFO", f"Excel guardado como {excel_name} para futuras re-categorizaciones.")


@_stage("RECATEGORIZE_SERVICES")
async def _recategorize_services(ctx: dict) -> AsyncGenerator[str, None]:
    """Re-apply current service mappings to all invoices in the period without reloading from SIHOS."""
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo
    from app.services.billing import load_excel, _normalize

    base_path: Path = ctx["base_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db: AsyncSession = ctx["db"]

    excel_path = base_path / _sihos_excel_name(institution, period)
    if not excel_path.exists():
        yield plog("ERROR", "No se encontró el Excel guardado. Ejecuta primero 'Cargar reporte SIHOS'.")
        return

    yield plog("INFO", f"Leyendo Excel guardado: {excel_path.name}")
    executor = _get_executor()
    file_bytes = await executor(excel_path.read_bytes)
    raw_df = await executor(load_excel, file_bytes)
    df = await executor(_normalize, raw_df)

    # Cargar mapeos actuales de servicios
    rules_repo = RulesRepo(db)

    from sqlalchemy import select as sa_select
    from app.models.institution import Service as ServiceModel

    result = await db.execute(sa_select(ServiceModel).where(ServiceModel.institution_id == institution.id))
    service_map: dict[str, int | None] = {s.raw_service: s.service_type_id for s in result.scalars().all()}

    service_types = await rules_repo.get_service_types()
    priority_map: dict[int, int] = {st.id: st.priority for st in service_types}

    # Recalcular service_type_id por factura
    invoice_service_ids: dict[str, set[int]] = {}
    for _, row in df.iterrows():
        raw_service = str(row.get("SERVICIO", "") or "").strip()
        invoice_number = str(row["FACTURA"])
        st_id = service_map.get(raw_service)
        if st_id is not None:
            invoice_service_ids.setdefault(invoice_number, set()).add(st_id)

    updates: dict[str, int | None] = {}
    for invoice_number, st_ids in invoice_service_ids.items():
        best = max(st_ids, key=lambda sid: priority_map.get(sid, 0)) if st_ids else None
        updates[invoice_number] = best

    yield plog("INFO", f"Facturas a actualizar: {len(updates)}")

    inv_repo = InvoiceRepo(db)
    updated = await inv_repo.batch_update_service_type(period.id, updates)
    await db.commit()
    yield plog("INFO", f"Tipos de servicio actualizados: {updated} factura(s).")


@_stage("RUN_STAGING")
async def _run_staging(ctx: dict) -> AsyncGenerator[str, None]:
    """Move leaf folders from DRIVE to STAGE."""
    from core.organizer import FolderCopier, LeafFolderFinder

    executor = _get_executor()
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


@_stage("CHECK_NESTED_FOLDERS")
async def _check_nested_folders(ctx: dict) -> AsyncGenerator[str, None]:
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

    executor = _get_executor()
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


@_stage("REMOVE_NON_PDF")
async def _remove_non_pdf(ctx: dict) -> AsyncGenerator[str, None]:
    """Scan STAGE for non-PDF files and corrupt PDFs; emit [DATA] for user review."""
    from core.ops import IMAGE_EXTENSIONS
    from core.reader import DocumentReader

    executor = _get_executor()
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


def _apply_prefix_corrections(
    stage_path: Path, corrections: dict[str, str]
) -> tuple[int, list[tuple[str, str, str]], list[str]]:
    """Rename PDFs whose prefix matches a known wrong prefix to the correct one.

    Returns ``(renamed_count, [(folder_name, old_name, new_name)], [error_messages])``.
    Per-file failures (permission, name collision) are logged and skipped instead
    of aborting the whole batch — matching the pattern already used by
    ``core.ops.DocumentOps``.
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


@_stage("NORMALIZE_FILES")
async def _normalize_files(ctx: dict) -> AsyncGenerator[str, None]:
    """Rename files with invalid names to the canonical standard.

    Step 1 — apply prefix corrections from the prefix_corrections table
              (e.g. OPD → OPF, FVE → FEV).
    Step 2 — run the generic FilenameStandardizer for remaining invalid names.
    """
    from core.scanner import DocumentScanner
    from core.standardizer import FilenameStandardizer

    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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
        renamed, renames, correction_errors = await executor(_apply_prefix_corrections, stage_path, corrections)
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
        ct_map = await _build_ct_map(db, period) if period else {}

        success = 0
        for r in results:
            original_path = Path(r.original_path)
            folder = original_path.parent.name
            old_name = original_path.name
            ct = _ct_for_folder(folder, ct_map)
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


@_stage("LIST_UNREADABLE_PDFS")
async def _list_unreadable_pdfs(ctx: dict) -> AsyncGenerator[str, None]:
    """List invoice PDFs without a text layer (scanned, unreadable)."""
    from core.reader import DocumentReader
    from core.scanner import DocumentScanner

    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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


@_stage("DELETE_UNREADABLE_PDFS")
async def _delete_unreadable_pdfs(ctx: dict) -> AsyncGenerator[str, None]:
    """Move invoice PDFs without a text layer to a quarantine folder (reversible).

    Files are moved to ``STAGE/_CUARENTENA_SIN_TEXTO/<carpeta>/<archivo>`` instead
    of being permanently deleted: the "no text layer" heuristic only inspects the
    first page (see core/reader.py) and can misfire on a scanned cover followed by
    real text pages, so an irreversible delete here is too risky for audit data.
    """
    from core.reader import DocumentReader
    from core.scanner import DocumentScanner

    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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
            yield plog(
                "INFO", f"Movido a cuarentena: {f.name} → {dest.relative_to(stage_path)}", folder=f.parent.name
            )
        except OSError as exc:
            yield plog("ERROR", f"No se pudo mover a cuarentena {f.name}: {exc}", folder=f.parent.name)
    yield plog("INFO", f"Total movidos a cuarentena: {moved}")


@_stage("DOWNLOAD_INVOICES_FROM_SIHOS")
async def _download_invoices_from_sihos(ctx: dict) -> AsyncGenerator[str, None]:
    """Download the factura PDF from SIHOS for PENDIENTE invoices with an unresolved finding of the chosen doc type."""
    from sqlalchemy import select

    from core.downloader import SihosDownloader

    from app.crypto import decrypt
    from app.models.finding import MissingFile
    from app.models.invoice import Invoice
    from app.models.rules import DocType, FolderStatus

    institution = ctx["institution"]
    stage_path: Path = ctx["stage_path"]
    db = ctx["db"]
    period = ctx["period"]
    doc_type_id: int = ctx.get("doc_type_id", 0)

    if not doc_type_id:
        yield plog(
            "ERROR", "No se seleccionó un tipo de documento. Elige uno en el panel antes de ejecutar esta etapa."
        )
        return

    if not institution.sihos_user or not institution.sihos_password:
        yield plog("ERROR", "Credenciales SIHOS no configuradas en la institución.")
        return

    doc_type_result = await db.execute(select(DocType).where(DocType.id == doc_type_id))
    doc_type = doc_type_result.scalar_one_or_none()
    if not doc_type:
        yield plog("ERROR", f"Tipo de documento con id={doc_type_id} no encontrado.")
        return

    yield plog("INFO", f"Tipo de documento: {doc_type.code} — prefijo {doc_type.prefix or '(sin prefijo)'}")

    stmt = (
        select(Invoice.invoice_number)
        .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
        .join(MissingFile, MissingFile.invoice_id == Invoice.id)
        .where(Invoice.audit_period_id == period.id)
        .where(FolderStatus.status == "PENDIENTE")
        .where(MissingFile.doc_type_id == doc_type_id)
        .where(MissingFile.resolved_at.is_(None))
        .distinct()
    )
    result = await db.execute(stmt)
    invoice_numbers = [row.invoice_number for row in result.all()]

    if not invoice_numbers:
        yield plog("INFO", f"No hay facturas PENDIENTE con hallazgo {doc_type.code} sin resolver.")
        return

    yield plog("INFO", f"Descargando {len(invoice_numbers)} factura(s) desde SIHOS...")

    password = decrypt(institution.sihos_password)
    downloader = SihosDownloader(
        user=institution.sihos_user,
        password=password,
        base_url=institution.sihos_base_url or "",
        hospital_nit=institution.nit,
        invoice_prefix=doc_type.prefix or "",
        invoice_id_prefix=institution.invoice_id_prefix or "",
        invoice_doc_code=institution.sihos_doc_code or "",
        output_dir=stage_path,
    )

    loop = asyncio.get_event_loop()
    progress_q: asyncio.Queue[str] = asyncio.Queue()

    def _on_progress(i: int, total: int, inv: str) -> None:
        loop.call_soon_threadsafe(progress_q.put_nowait, plog("INFO", f"[{i}/{total}] Factura {inv}"))

    task = asyncio.ensure_future(
        loop.run_in_executor(None, lambda: downloader.run_from_list(invoice_numbers, _on_progress))
    )
    while not task.done():
        try:
            yield progress_q.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.3)
    while not progress_q.empty():
        yield progress_q.get_nowait()
    await task  # propaga excepciones
    yield plog("INFO", "Descarga SIHOS completada.")


@_stage("DOWNLOAD_MEDICATION_SHEETS")
async def _download_medication_sheets(ctx: dict) -> AsyncGenerator[str, None]:
    """Download PDFs for PENDIENTE invoices with an unresolved finding of the chosen doc type."""
    from sqlalchemy import select

    from core.downloader import SihosDownloader

    from app.crypto import decrypt
    from app.models.finding import MissingFile
    from app.models.invoice import Invoice
    from app.models.rules import DocType, FolderStatus

    institution = ctx["institution"]
    stage_path: Path = ctx["stage_path"]
    db = ctx["db"]
    period = ctx["period"]
    doc_type_id: int = ctx.get("doc_type_id", 0)

    if not doc_type_id:
        yield plog(
            "ERROR", "No se seleccionó un tipo de documento. Elige uno en el panel antes de ejecutar esta etapa."
        )
        return

    if not institution.sihos_user or not institution.sihos_password:
        yield plog("ERROR", "Credenciales SIHOS no configuradas en la institución.")
        return

    doc_type_result = await db.execute(select(DocType).where(DocType.id == doc_type_id))
    doc_type = doc_type_result.scalar_one_or_none()
    if not doc_type:
        yield plog("ERROR", f"Tipo de documento con id={doc_type_id} no encontrado.")
        return

    yield plog("INFO", f"Tipo de documento seleccionado: {doc_type.code} — {doc_type.description}")

    stmt = (
        select(Invoice.invoice_number, Invoice.admission, Invoice.id_type, Invoice.id_number)
        .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
        .join(MissingFile, MissingFile.invoice_id == Invoice.id)
        .where(Invoice.audit_period_id == period.id)
        .where(Invoice.admission.isnot(None))
        .where(FolderStatus.status == "PENDIENTE")
        .where(MissingFile.doc_type_id == doc_type_id)
        .where(MissingFile.resolved_at.is_(None))
        .distinct()
    )
    result = await db.execute(stmt)
    targets: list[tuple[str, str, str, str]] = [
        (row.invoice_number, row.admission, row.id_type, row.id_number) for row in result.all()
    ]

    if not targets:
        yield plog("INFO", f"No hay facturas PENDIENTE con hallazgo {doc_type.code} sin resolver y número de admisión.")
        return

    yield plog("INFO", f"Descargando {len(targets)} factura(s) desde SIHOS...")

    password = decrypt(institution.sihos_password)
    downloader = SihosDownloader(
        user=institution.sihos_user,
        password=password,
        base_url=institution.sihos_base_url or "",
        hospital_nit=institution.nit,
        invoice_prefix="",
        invoice_id_prefix=institution.invoice_id_prefix or "",
        invoice_doc_code=institution.sihos_doc_code or "",
        output_dir=stage_path,
    )

    file_prefix = doc_type.prefix or doc_type.code
    loop = asyncio.get_event_loop()
    progress_q: asyncio.Queue[str] = asyncio.Queue()

    def _on_progress(i: int, total: int, inv: str) -> None:
        loop.call_soon_threadsafe(progress_q.put_nowait, plog("INFO", f"[{i}/{total}] Factura {inv}"))

    task = asyncio.ensure_future(
        loop.run_in_executor(None, lambda: downloader.run_medication_sheets(targets, file_prefix, _on_progress))
    )
    while not task.done():
        try:
            yield progress_q.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.3)
    while not progress_q.empty():
        yield progress_q.get_nowait()
    await task  # propaga excepciones
    yield plog("INFO", "Descarga completada.")


@_stage("VERIFY_INVOICE_CODE")
async def _verify_invoice_code(ctx: dict) -> AsyncGenerator[str, None]:
    """Verify that each invoice PDF contains its own invoice number in the text."""
    from core.scanner import DocumentScanner
    from core.validator import InvoiceValidator

    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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
    missing_code, _ = await executor(validator.validate_invoice_files, invoices)
    for f in missing_code:
        yield plog("WARN", f"Sin número de factura en PDF: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin código: {len(missing_code)}")


@_stage("CHECK_INVOICE_NUMBER_ON_FILES")
async def _check_invoice_number_on_files(ctx: dict) -> AsyncGenerator[str, None]:
    """Verify that files inside each invoice folder match the folder name."""
    from core.inspector import FolderInspector

    executor = _get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inspector = FolderInspector(stage_path, institution.invoice_id_prefix or "")
    mismatched = await executor(inspector.find_mismatched_files)
    for f in mismatched:
        yield plog("WARN", f"Archivo desajustado: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Archivos desajustados: {len(mismatched)}")


@_stage("CHECK_FOLDERS_WITH_EXTRA_TEXT")
async def _check_folders_with_extra_text(ctx: dict) -> AsyncGenerator[str, None]:
    """Detect folders with extra text; send interactive payload for auditor review."""
    import json

    from sqlalchemy import select

    from app.models.invoice import Invoice
    from core.inspector import FolderInspector

    executor = _get_executor()
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


@_stage("NORMALIZE_DIR_NAMES")
async def _normalize_dir_names(ctx: dict) -> AsyncGenerator[str, None]:
    """Rename folders with extra text to their canonical identifier."""
    from core.inspector import FolderInspector
    from core.ops import DocumentOps

    executor = _get_executor()
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


@_stage("CHECK_DIRS")
async def _check_dirs(ctx: dict) -> AsyncGenerator[str, None]:
    """Compare invoice numbers in DB against folders on disk; mark missing ones FALTANTE."""
    from core.inspector import FolderInspector

    from app.repositories.invoice_repo import InvoiceRepo

    executor = _get_executor()
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
        ct_map = await _build_ct_map(db, period)
        updated = await inv_repo.batch_update_folder_status(period.id, missing, "FALTANTE")
        await db.commit()
        yield plog("INFO", f"Marcadas como FALTANTE: {updated}")
        for name in missing:
            yield plog("WARN", "Sin carpeta en disco", folder=name, contract_type=_ct_for_folder(name, ct_map))


@_stage("MARK_UNKNOWN_DIRS")
async def _mark_unknown_dirs(ctx: dict) -> AsyncGenerator[str, None]:
    """Rename STAGE folders that match the invoice pattern but have no DB record.

    Folders are prefixed with '(DESCONOCIDO)'.  Already-marked folders are
    skipped so the stage is safe to re-run.
    """
    from core.inspector import FolderInspector
    from app.repositories.invoice_repo import InvoiceRepo

    executor = _get_executor()
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


@_stage("CHECK_REQUIRED_DOCS")
async def _check_required_docs(ctx: dict) -> AsyncGenerator[str, None]:
    """Verify required documents per service type; record findings for missing ones."""
    from core.inspector import FolderInspector

    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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
    ) -> dict[int, tuple[str, list[int]]]:
        """Returns {invoice_id: (invoice_number, [missing_doc_type_ids])}"""
        results: dict[int, tuple[str, list[int]]] = {}
        for inv_id, inv_number, svc_type_id in inv_data:
            required_dt_ids = std_map.get(svc_type_id, []) if svc_type_id is not None else []
            if not required_dt_ids:
                continue
            required_prefixes = {str(dt_id): prefix_map.get(dt_id, []) for dt_id in required_dt_ids}
            folder = stage_path / (id_prefix + inv_number)
            missing_codes = inspector.check_required_docs(folder, required_prefixes)
            if missing_codes:
                results[inv_id] = (inv_number, [int(c) for c in missing_codes])
        return results

    findings_map = await executor(_check_all_folders, invoice_data)

    # Bulk upsert all findings in one DB call
    all_findings: list[tuple[int, int]] = [
        (inv_id, dt_id) for inv_id, (_, dt_ids) in findings_map.items() for dt_id in dt_ids
    ]
    total_findings = len(all_findings)
    await finding_repo.bulk_upsert_findings(all_findings)

    ct_map = await _build_ct_map(db, period)
    invoices_with_findings = [inv_number for _, (inv_number, _) in findings_map.items()]
    for inv_id, (inv_number, dt_ids) in findings_map.items():
        ct = _ct_for_folder(inv_number, ct_map)
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


@_stage("REVISAR_SOBRANTES")
async def _revisar_sobrantes(ctx: dict) -> AsyncGenerator[str, None]:
    """Identify surplus files per invoice folder and suggest doc-type renames."""
    import json

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

    executor = _get_executor()

    def _scan_folders(
        s_path: Path,
        inv_snap: dict,
        s_std_map: dict,
        s_prefix_map: dict,
    ) -> tuple[list[dict], list[str], int]:
        from difflib import SequenceMatcher as SM

        items_out: list[dict] = []
        log_lines: list[str] = []
        total = 0

        for folder in sorted(s_path.iterdir()):
            if not folder.is_dir():
                continue

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

    items, log_lines, total_sobrantes = await executor(_scan_folders, stage_path, invoice_snapshot, std_map, prefix_map)

    for line in log_lines:
        yield line

    yield plog("INFO", f"Total: {total_sobrantes} archivos sobrantes en {len(items)} carpetas")
    if items:
        yield f"[DATA] {json.dumps(items, ensure_ascii=False)}"
    else:
        yield plog("INFO", "No se encontraron archivos sobrantes")


@_stage("VERIFY_CUFE")
async def _verify_cufe(ctx: dict) -> AsyncGenerator[str, None]:
    """Verify CUFE presence and tag folders missing a CUFE in invoice PDFs."""
    from core.ops import DocumentOps
    from core.scanner import DocumentScanner
    from core.validator import InvoiceValidator

    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
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
    _, missing_cufe = await executor(validator.validate_invoice_files, invoices)
    for f in missing_cufe:
        yield plog("WARN", f"Sin CUFE: {f.name}", folder=f.parent.name)
    yield plog("INFO", f"Facturas sin CUFE: {len(missing_cufe)}")

    if missing_cufe:
        ops = DocumentOps(stage_path, institution.invoice_id_prefix or "")
        marked = await executor(ops.tag_dirs_missing_cufe, missing_cufe)
        yield plog("INFO", f"Carpetas marcadas sin CUFE: {marked}")


@_stage("ORGANIZE")
async def _organize(ctx: dict) -> AsyncGenerator[str, None]:
    """Move eligible invoices (PRESENTE + no findings) to AUDIT/ADMIN[/CONTRACT]/FOLDER."""
    from core.helpers import safe_move

    from app.repositories.invoice_repo import InvoiceRepo

    executor = _get_executor()
    stage_path: Path = ctx["stage_path"]
    audit_path: Path = ctx["audit_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not stage_path.is_dir():
        yield plog("WARN", f"Directorio STAGE no existe: {stage_path}")
        return

    inv_repo = InvoiceRepo(db)
    organizable = await inv_repo.get_organizable_invoices(period.id)
    yield plog("INFO", f"Facturas organizables: {len(organizable)}")

    if not organizable:
        yield plog("INFO", "Nada que organizar.")
        return

    ct_map = await _build_ct_map(db, period)

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
            ct = _ct_for_folder(inv.invoice_number, ct_map)
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

    moved_pairs, failed_names = await executor(_batch_move, planned)

    # Derive moved_numbers from planned vs failed
    failed_set = set(failed_names)
    moved_numbers = [inv_number for src, _, inv_number in planned if src.name not in failed_set]

    for src_name, rel_dest in moved_pairs:
        ct = _ct_for_folder(src_name, ct_map)
        yield plog("INFO", f"Movida: {src_name} → {rel_dest}", contract_type=ct)
    for src_name in failed_names:
        ct = _ct_for_folder(src_name, ct_map)
        yield plog("WARN", f"Error al mover: {src_name}", contract_type=ct)

    yield plog("INFO", f"Movidas: {len(moved_numbers)}, no encontradas: {not_found}, fallidas: {len(failed_names)}")

    if moved_numbers:
        updated = await inv_repo.batch_update_to_auditada(period.id, moved_numbers)
        await db.commit()
        yield plog("INFO", f"Facturas marcadas AUDITADA: {updated}")


@_stage("DOWNLOAD_DRIVE")
async def _download_drive(ctx: dict) -> AsyncGenerator[str, None]:
    """Download invoice folders from Google Drive."""
    from core.drive import DriveSync

    from app.crypto import decrypt
    from app.repositories.invoice_repo import InvoiceRepo

    executor = _get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not institution.drive_credentials_enc:
        yield plog("ERROR", "Credenciales de Drive no configuradas en la institución.")
        return

    manual_list: list[str] | None = ctx.get("invoice_numbers") or None
    inv_repo: InvoiceRepo | None = None

    if manual_list is not None:
        missing = manual_list
        yield plog("INFO", f"Modo manual: {len(missing)} carpeta(s) ingresadas.")
    else:
        inv_repo = InvoiceRepo(db)
        missing = await inv_repo.get_invoice_numbers_by_status(period.id, "FALTANTE")
        yield plog("INFO", f"Facturas FALTANTE a buscar en Drive: {len(missing)}")
        if not missing:
            yield plog("INFO", "No hay facturas FALTANTE.")
            return

    creds = json.loads(decrypt(institution.drive_credentials_enc))
    drive = DriveSync(credentials_dict=creds)

    id_prefix = institution.invoice_id_prefix or ""

    stage_path.mkdir(parents=True, exist_ok=True)
    yield plog("INFO", f"Buscando {len(missing)} carpeta(s) en Drive...")
    # Search by bare invoice number so folders with separators or extra text
    # (e.g. "FE-12345", "FE12345 YA") are matched. download_missing_dirs batches
    # the search internally (see core/drive.py::_batch_search_folders) — call it
    # once with the full list instead of once per invoice number.
    found = await executor(drive.download_missing_dirs, missing, stage_path)
    downloaded = list(found)
    found_set = set(downloaded)
    for i, num in enumerate(missing, 1):
        status = "✓" if num in found_set else "✗ no encontrada"
        yield plog("INFO", f"[{i}/{len(missing)}] {id_prefix}{num} — {status}")
    yield plog("INFO", f"Carpetas descargadas de Drive: {len(downloaded)}/{len(missing)}")

    if manual_list is None and downloaded:
        assert inv_repo is not None  # manual_list is None → the else branch above set it
        updated = await inv_repo.batch_update_folder_status(period.id, downloaded, "PRESENTE")
        await db.commit()
        yield plog("INFO", f"Facturas actualizadas a PRESENTE: {updated}")


@_stage("DOWNLOAD_MISSING_DOCS")
async def _download_missing_docs(ctx: dict) -> AsyncGenerator[str, None]:
    """Download specific missing document files from Google Drive."""
    from core.drive import DriveSync

    from app.crypto import decrypt
    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.rules_repo import RulesRepo

    executor = _get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not institution.drive_credentials_enc:
        yield plog("ERROR", "Credenciales de Drive no configuradas en la institución.")
        return

    finding_repo = MissingFileRepo(db)
    rules_repo = RulesRepo(db)

    grouped = await finding_repo.get_findings_grouped_by_invoice(period.id)
    yield plog("INFO", f"Facturas con documentos faltantes: {len(grouped)}")

    if not grouped:
        yield plog("INFO", "No hay documentos faltantes.")
        return

    doc_types = await rules_repo.get_doc_types()
    prefix_by_code = {dt.code: dt.prefix for dt in doc_types if dt.prefix}

    creds = json.loads(decrypt(institution.drive_credentials_enc))
    drive = DriveSync(credentials_dict=creds)

    id_prefix = institution.invoice_id_prefix or ""

    # Build all (file_name, dest_folder) pairs upfront across every invoice.
    # dest_folder is created on demand by DriveSync.download_file.
    download_requests: list[tuple[str, Path]] = [
        (
            f"{prefix_by_code[code]}_{institution.nit}_{id_prefix}{invoice_number}.pdf",
            stage_path / (id_prefix + invoice_number),
        )
        for invoice_number, doc_codes in grouped.items()
        for code in doc_codes
        if code in prefix_by_code
    ]

    yield plog("INFO", f"Buscando {len(download_requests)} documento(s) en Drive...")
    found = not_found = 0
    total_docs = len(download_requests)
    for i, request in enumerate(download_requests, 1):
        f, nf = await executor(drive.download_specific_files, [request])
        found += f
        not_found += nf
        status = "✓" if f else "✗ no encontrado"
        yield plog("INFO", f"[{i}/{total_docs}] {request[0]} — {status}")
    yield plog("INFO", f"Encontrados: {found}, no encontrados: {not_found}")


_COMPRESS_CHUNK = 50  # files per progress report


@_stage("COMPRESS_AUDIT")
async def _compress_audit(ctx: dict) -> AsyncGenerator[str, None]:
    """Compress all PDFs in AUDIT using Ghostscript (ebook quality)."""
    import os

    from core.processor import DocumentProcessor

    loop = asyncio.get_running_loop()
    audit_path: Path = ctx["audit_path"]

    if not audit_path.is_dir():
        yield plog("WARN", f"Directorio AUDIT no existe: {audit_path}")
        return

    all_pdfs = list(audit_path.rglob("*.pdf"))
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

    for i, chunk in enumerate(chunks, 1):
        _chunk = chunk
        result: dict[str, int] = await loop.run_in_executor(
            None, lambda c=_chunk: DocumentProcessor.batch_compress(c, "ebook", workers)  # type: ignore[misc]
        )
        for key in totals:
            totals[key] += result[key]
        processed = min(i * _COMPRESS_CHUNK, total)
        yield plog(
            "INFO",
            f"Progreso: {processed}/{total} ({processed / total * 100:.0f}%) — éxitos: {totals['success']}, fallos: {totals['failed']}",
        )

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


@_stage("EXPORTAR_AUDITADOS")
async def _exportar_auditados(ctx: dict) -> AsyncGenerator[str, None]:
    """Prepara la exportación de AUDIT: valida y cuenta archivos.
    El ZIP se genera al vuelo en el endpoint /api/pipeline/stream-audit-zip."""
    audit_path: Path = ctx["audit_path"]

    if not audit_path.is_dir():
        yield plog("WARN", "Directorio AUDIT no existe — no hay nada que exportar.")
        return

    all_files = [f for f in audit_path.rglob("*") if f.is_file()]
    if not all_files:
        yield plog("INFO", "No hay archivos en AUDIT para exportar.")
        return

    total_mb = sum(f.stat().st_size for f in all_files) / (1024 * 1024)
    yield plog("INFO", f"{len(all_files)} archivos listos para descargar ({total_mb:.1f} MB)")
    yield plog("DATA", f"export_ready:{len(all_files)}")
