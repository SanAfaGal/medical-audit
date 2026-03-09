"""Pipeline execution: state, log handler, helpers, and _execute_pipeline."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level pipeline run state (survives Streamlit reruns)
# ---------------------------------------------------------------------------

_cancel_event = threading.Event()

_PIPE_RUNNING = "running"
_PIPE_LOG     = "log"

_pipe: dict[str, object] = {
    _PIPE_RUNNING: False,
    _PIPE_LOG:     "",
}


# ---------------------------------------------------------------------------
# Live log handler
# ---------------------------------------------------------------------------

class _LiveLogHandler(logging.Handler):
    """Logging handler that streams records to a callback in real time."""

    def __init__(self, on_record: Callable[[str], None]) -> None:
        super().__init__()
        self._on_record = on_record
        self._lines: list[str] = []
        self.setFormatter(logging.Formatter("%(levelname)-8s %(name)s — %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        self._lines.append(self.format(record))
        self._on_record("\n".join(self._lines))

    def getvalue(self) -> str:
        return "\n".join(self._lines)


# ---------------------------------------------------------------------------
# Invoice categorisation helpers
# ---------------------------------------------------------------------------


def _categorize_invoices(
    validator,
    invoices: list,
    repo,
    hospital: str,
    period: str,
) -> None:
    """Detect invoice types from PDF content using DB-configured keywords.

    Reads active invoice types and their keywords from the database, scans each
    PDF once (table text only via pdfplumber), caches the normalised content in
    memory, then matches all keywords against the cache — reducing PDF reads
    from N×K to N (one per invoice, regardless of how many types/keywords exist).

    Args:
        validator: Kept for call-site compatibility; no longer used internally.
        invoices: List of invoice PDF paths.
        repo: AuditRepository instance.
        hospital: Active hospital key.
        period: Audit period string.
    """
    from core.reader import DocumentReader
    from core.helpers import remove_accents

    active_types = [t for t in repo.fetch_invoice_types() if t["is_active"] and t["keywords"]]
    if not active_types:
        return

    # Read all PDFs in parallel (I/O bound — threads give real speedup).
    # Result: {Path: str | None}
    #   str  → normalised uppercase content of the valid service table
    #   None → no valid service table detected; invoice will be DESCONOCIDO
    def _read(f):
        raw = DocumentReader.read_text_if_has_table(f)
        return f, remove_accents(raw).upper() if raw is not None else None

    content_cache: dict = {}
    with ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 4) + 4)) as pool:
        futures = {pool.submit(_read, f): f for f in invoices}
        for future in as_completed(futures):
            path, content = future.result()
            content_cache[path] = content

    # Invoices with no valid service table → mark DESCONOCIDO, skip keyword matching
    no_table_dirs = {f.parent for f, content in content_cache.items() if content is None}
    for d in no_table_dirs:
        repo.set_tipos(hospital, period, d.name.upper(), ["DESCONOCIDO"])

    # Match all types/keywords against the cache (no more disk I/O)
    for inv_type in active_types:
        code = inv_type["code"]
        keywords = [remove_accents(kw).upper() for kw in inv_type["keywords"]]
        matched_dirs: set = set()
        for f, content in content_cache.items():
            if content is None or not content:
                continue
            if any(kw in content for kw in keywords):
                matched_dirs.add(f.parent)
        for d in matched_dirs:
            repo.add_tipo(hospital, period, d.name.upper(), code)



# ---------------------------------------------------------------------------
# Main pipeline execution
# ---------------------------------------------------------------------------

def _execute_pipeline(
    flags: dict[str, bool],
    hospital: str,
    period: str,
    on_update: Callable[[str], None] | None = None,
    invoice_numbers: list[str] | None = None,
) -> str:
    """Run the selected pipeline stages and return captured log output.

    Sets up a logging handler so all module loggers are captured. When
    *on_update* is provided, the callback is called with the accumulated
    text after every log record — safe to use from a background thread.

    Args:
        flags: Mapping of stage key to enabled boolean.
        hospital: Active hospital key (e.g. ``"SANTA_LUCIA"``).
        period: Audit period string (e.g. ``"22-28_MARZO"``).
        on_update: Optional callback receiving the full accumulated log text.
        invoice_numbers: Invoice IDs to download from SIHOS portal.

    Returns:
        Multi-line string of all log output produced during the run.
    """
    from config.settings import Settings
    from core.billing import BillingIngester
    from core.drive import DriveSync
    from core.helpers import flatten_prefixes
    from core.inspector import FolderInspector
    from core.ops import DocumentOps
    from core.organizer import FolderCopier, InvoiceOrganizer, LeafFolderFinder
    from core.processor import DocumentProcessor
    from core.reader import DocumentReader
    from core.scanner import DocumentScanner
    from core.standardizer import FilenameStandardizer
    from core.validator import InvoiceValidator
    from db.repository import AuditRepository
    from db.constants import InvoiceType

    handler = _LiveLogHandler(on_update or (lambda _: None))
    root = logging.getLogger()
    root.addHandler(handler)

    try:
        repo = AuditRepository(Settings.db_path)
        hospital_cfg = repo.fetch_hospital_config(hospital)

        pipeline_logger = logging.getLogger("app.pipeline")

        if not Settings.audit_path:
            pipeline_logger.error(
                "Audit path not configured. Set it in Settings → Directorio de auditoría."
            )
            return handler.getvalue()

        id_prefix    = hospital_cfg.get("INVOICE_IDENTIFIER_PREFIX", "")
        base_path    = Settings.audit_path / hospital / period
        staging_dir  = base_path / "STAGE"
        archive_dir  = base_path / "AUDIT"
        base_dir     = base_path / "BASE"
        sihos_report = base_path / (f"{period}_SIHOS.xlsx")

        scanner    = DocumentScanner(staging_dir)
        inspector  = FolderInspector(staging_dir, id_prefix=id_prefix)
        operations = DocumentOps(staging_dir, id_prefix=id_prefix)
        validator  = InvoiceValidator(staging_dir, id_prefix=id_prefix)
        admin_map  = repo.fetch_admin_contract_map(hospital)

        def _cancelled() -> bool:
            if _cancel_event.is_set():
                pipeline_logger.warning("Pipeline cancelled by user.")
                return True
            return False

        # ── Ingestion ────────────────────────────────────────────────────────

        df_processed = None

        if flags.get("LOAD_AND_PROCESS"):
            ingester = BillingIngester(admin_map)
            ingester.load_excel(sihos_report, Settings.raw_schema_columns)

            unknown_pairs = ingester.find_unknown_pairs()
            if unknown_pairs:
                saved = repo.register_unknown_mappings(hospital, unknown_pairs)
                pipeline_logger.warning(
                    "%d unmapped admin/contract pair(s) auto-registered in DB "
                    "(%d new). Go to Settings → Mapeos to fill in canonical values.",
                    len(unknown_pairs),
                    saved,
                )

            df_processed = ingester.build_invoice_dataframe()
            inserted = repo.upsert_invoices(df_processed, hospital=hospital, period=period)
            pipeline_logger.info("Invoices loaded into audit repository: %d", inserted)

        if _cancelled():
            return handler.getvalue()

        # ── Organise ─────────────────────────────────────────────────────────
        # Runs independently of LOAD_AND_PROCESS: reads eligible invoices from
        # the DB (PRESENTE + no findings) and moves them to the AUDIT directory.
        # On success each folder is marked AUDITADA.

        if flags.get("ORGANIZE"):
            df_to_organize = repo.fetch_organizable_invoices(hospital, period)
            if df_to_organize.empty:
                pipeline_logger.info(
                    "ORGANIZE: no eligible invoices (must be PRESENTE with no findings)."
                )
            else:
                pipeline_logger.info(
                    "ORGANIZE: %d invoice(s) eligible for organization.", len(df_to_organize)
                )
                organizer = InvoiceOrganizer(
                    df=df_to_organize,
                    staging_dir=staging_dir,
                    archive_dir=archive_dir,
                )
                result = organizer.organize(dry_run=False)
                pipeline_logger.info(
                    "ORGANIZE complete — moved: %d, not found: %d, failed: %d",
                    result.moved, result.not_found, result.failed,
                )
                if result.errors:
                    for err in result.errors:
                        pipeline_logger.error("ORGANIZE error: %s", err)
                if result.moved_ids:
                    repo.batch_update_folder_status(
                        hospital, period, result.moved_ids, "AUDITADA"
                    )
                    pipeline_logger.info(
                        "ORGANIZE: %d folder(s) marked AUDITADA.", len(result.moved_ids)
                    )

        if _cancelled():
            return handler.getvalue()

        # ── Drive download ───────────────────────────────────────────────────

        if flags.get("DOWNLOAD_DRIVE"):
            from db.constants import FolderStatus
            missing_folders = repo.fetch_by_folder_status(hospital, period, FolderStatus.MISSING)
            drive = DriveSync(credentials_path=Settings.drive_credentials_path(hospital))
            downloaded = drive.download_missing_dirs(missing_folders, staging_dir)
            if downloaded:
                repo.batch_update_folder_status(
                    hospital, period, downloaded, FolderStatus.PRESENT
                )
            pipeline_logger.info(
                "Drive download: %d/%d folders found and updated to PRESENTE",
                len(downloaded), len(missing_folders),
            )

        # ── Staging from BASE ────────────────────────────────────────────────

        if flags.get("RUN_STAGING"):
            leaf_finder  = LeafFolderFinder()
            leaf_folders = leaf_finder.find_leaf_folders(base_dir)
            if leaf_folders:
                copier = FolderCopier(staging_dir)
                copier.copy_folders(leaf_folders, use_prefix=False)
                pipeline_logger.info(
                    "Leaf folders copied from BASE to STAGE: %d", len(leaf_folders)
                )
            else:
                pipeline_logger.warning(
                    "RUN_STAGING: no leaf folders found in BASE directory: %s", base_dir
                )

        # ── Skip list (SOAT) ─────────────────────────────────────────────────

        soat_facturas = repo.fetch_by_tipo(hospital, period, InvoiceType.SOAT)
        skip_dirs     = inspector.resolve_dir_paths(soat_facturas)

        if _cancelled():
            return handler.getvalue()

        # ── Normalisation ────────────────────────────────────────────────────

        skip_set = set(skip_dirs)

        def _is_skipped(f) -> bool:  # type: ignore[return]
            return any(d in f.parents for d in skip_set)

        if flags.get("REMOVE_NON_PDF"):
            non_pdf = [f for f in scanner.find_non_pdf() if not _is_skipped(f)]
            removed = operations.remove_files(non_pdf)
            pipeline_logger.info("Non-PDF files removed: %d", removed)

        if flags.get("NORMALIZE_FILES"):
            prefixes_accepted = flatten_prefixes(hospital_cfg["DOCUMENT_STANDARDS"])
            invalid_files = [
                f for f in scanner.find_invalid_names(
                    valid_prefixes=prefixes_accepted,
                    suffix=hospital_cfg["INVOICE_IDENTIFIER_PREFIX"],
                    nit=hospital_cfg["NIT"],
                )
                if not _is_skipped(f)
            ]
            pipeline_logger.info("Archivos con nombre inválido: %d", len(invalid_files))
            for f in invalid_files:
                pipeline_logger.info("  %s", f.name)
            standardizer = FilenameStandardizer(
                nit=hospital_cfg["NIT"],
                valid_prefixes=prefixes_accepted,
                suffix_const=hospital_cfg["INVOICE_IDENTIFIER_PREFIX"],
                prefix_map=Settings.filename_fixes,
            )
            standardizer.run(invalid_files)

        if flags.get("CHECK_INVOICE_NUMBER_ON_FILES"):
            mismatched = inspector.find_mismatched_files(skip_dirs=skip_dirs)
            pipeline_logger.info("Files mismatched to folder name: %d", len(mismatched))

        dirs_with_extra_text: list = []
        if flags.get("CHECK_FOLDERS_WITH_EXTRA_TEXT") or flags.get("NORMALIZE_DIR_NAMES"):
            dirs_with_extra_text = inspector.find_malformed_dirs(skip=skip_dirs)

        if flags.get("CHECK_FOLDERS_WITH_EXTRA_TEXT"):
            pipeline_logger.info(
                "Directories with extra text in name: %d", len(dirs_with_extra_text)
            )

        if flags.get("NORMALIZE_DIR_NAMES"):
            renamed = operations.standardize_dir_names(dirs_with_extra_text)
            pipeline_logger.info("Directories renamed: %d", renamed)

        if _cancelled():
            return handler.getvalue()

        # ── Invoice audit ────────────────────────────────────────────────────

        doc_standards  = hospital_cfg.get("DOCUMENT_STANDARDS", {})
        invoice_prefix = doc_standards.get("FACTURA", "")
        invoices       = scanner.find_by_prefix(invoice_prefix) if invoice_prefix else []

        if flags.get("LIST_UNREADABLE_PDFS"):
            no_text = DocumentReader.find_needing_ocr(invoices)
            pipeline_logger.info("Invoice PDFs without extractable text: %d", len(no_text))
            for f in no_text:
                pipeline_logger.info("  No text layer: %s", f.name)

        if flags.get("DELETE_UNREADABLE_PDFS"):
            to_delete = DocumentReader.find_needing_ocr(invoices)
            deleted = 0
            for f in to_delete:
                try:
                    f.unlink()
                    deleted += 1
                    pipeline_logger.info("Deleted unreadable PDF: %s", f.name)
                except OSError as exc:
                    pipeline_logger.error("Failed to delete %s: %s", f.name, exc)
            pipeline_logger.info("Deleted %d unreadable invoice PDFs", deleted)

        if flags.get("CATEGORIZE_INVOICES"):
            _categorize_invoices(validator, invoices, repo, hospital, period)
            pipeline_logger.info("Invoice categorization complete.")

        missing_code: list = []
        missing_cufe: list = []
        if flags.get("VERIFY_INVOICE_CODE") or flags.get("VERIFY_CUFE") or flags.get("TAG_MISSING_CUFE"):
            missing_code, missing_cufe = validator.validate_invoice_files(invoices)

        if flags.get("VERIFY_INVOICE_CODE"):
            pipeline_logger.info(
                "Facturas sin número de factura en contenido: %d", len(missing_code)
            )
            for f in missing_code:
                pipeline_logger.info("  %s", f.name)

        if flags.get("VERIFY_CUFE"):
            pipeline_logger.info("Facturas sin CUFE: %d", len(missing_cufe))
            for f in missing_cufe:
                pipeline_logger.info("  %s", f.name)

        if flags.get("TAG_MISSING_CUFE"):
            if missing_cufe:
                marked = operations.tag_dirs_missing_cufe(missing_cufe)
                pipeline_logger.info("Carpetas marcadas como sin CUFE: %d", marked)
            else:
                pipeline_logger.info("No hay carpetas sin CUFE para marcar.")

        if flags.get("CHECK_INVOICES"):
            needing_ocr = DocumentReader.find_needing_ocr(invoices)
            ocr_result  = DocumentProcessor.batch_ocr(files=needing_ocr, max_workers=8)
            pipeline_logger.info("OCR batch completed for invoices: %s", ocr_result)

        if flags.get("CHECK_DIRS"):
            from db.constants import FolderStatus
            all_folders  = repo.fetch_invoice_ids(hospital, period)
            missing_dirs = inspector.find_missing_dirs(expected_dirs=all_folders)
            pipeline_logger.info("Missing directories: %d", len(missing_dirs))
            for factura in missing_dirs:
                repo.update_folder_status(hospital, period, factura, FolderStatus.MISSING)

        if flags.get("CHECK_INVALID_FILES"):
            all_files    = scanner.find_by_extension()
            invalid_pdfs = DocumentReader.find_unreadable(all_files)
            pipeline_logger.info("Unreadable PDF files: %d", len(invalid_pdfs))

        # ── Dynamic document requirements check ──────────────────────────────

        if flags.get("CHECK_REQUIRED_DOCS"):
            doc_types_map = {
                dt["code"]: dt["prefixes"]
                for dt in repo.fetch_document_types()
                if dt["is_active"]
            }
            inv_types_info = {
                it["code"]: {"sort_order": it["sort_order"], "required_docs": it["required_docs"]}
                for it in repo.fetch_invoice_types()
                if it["is_active"]
            }
            all_invoice_ids = repo.fetch_by_folder_status(hospital, period, "PRESENTE")
            checked = 0
            findings_added = 0

            for factura in all_invoice_ids:
                tipos = repo.fetch_tipos(hospital, period, factura)
                if "SOAT" in tipos:
                    continue

                max_priority = max(
                    (inv_types_info.get(t, {}).get("sort_order", 0) for t in tipos),
                    default=0,
                )
                priority_types = [
                    t for t in tipos
                    if inv_types_info.get(t, {}).get("sort_order", 0) == max_priority
                ]

                required: set[str] = set()
                for t in priority_types:
                    required.update(inv_types_info.get(t, {}).get("required_docs", []))

                if not required:
                    checked += 1
                    continue

                folder = staging_dir / factura
                missing_docs = inspector.check_required_docs(
                    folder,
                    {code: doc_types_map.get(code, []) for code in required},
                )
                for doc_code in missing_docs:
                    repo.record_finding(hospital, period, factura, doc_code)
                    findings_added += 1

                checked += 1

            pipeline_logger.info(
                "Verificación de documentos requeridos: %d facturas revisadas, "
                "%d hallazgos registrados",
                checked,
                findings_added,
            )

        # ── SIHOS invoice download ────────────────────────────────────────────

        if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
            from core.downloader import SihosDownloader
            if invoice_numbers:
                downloader = SihosDownloader(
                    user=hospital_cfg["sihos_user"],
                    password=hospital_cfg["sihos_password"],
                    base_url=hospital_cfg["SIHOS_BASE_URL"],
                    hospital_nit=hospital_cfg["NIT"],
                    invoice_prefix=doc_standards.get("FACTURA", ""),
                    invoice_id_prefix=id_prefix,
                    invoice_doc_code=hospital_cfg["SIHOS_INVOICE_DOC_CODE"],
                    output_dir=staging_dir,
                )
                downloader.run_from_list(invoice_numbers)
            else:
                pipeline_logger.warning("No invoice numbers provided for SIHOS download.")

    except (OSError, RuntimeError, ValueError) as exc:
        logging.getLogger("app.pipeline").error(
            "Pipeline error: %s", exc, exc_info=True
        )
    finally:
        root.removeHandler(handler)

    return handler.getvalue()
