"""Pipeline execution: state, log handler, helpers, and _execute_pipeline."""

from __future__ import annotations

import contextlib
import logging
import threading
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

_SEARCH_ECG        = "ELECTROCARD"
_SEARCH_LABORATORY = "LABORATORIO CLINICO"
_SEARCH_XRAY       = "RADIOGRAF"
_SEARCH_POLYCLINIC = "P909000"
_SEARCH_EMERGENCY  = "URGENCIA"

_TEXT_TO_TIPO: dict[str, str] = {
    _SEARCH_LABORATORY: "LABORATORIO",
    _SEARCH_ECG:        "ECG",
    _SEARCH_XRAY:       "RADIOGRAFIA",
    _SEARCH_EMERGENCY:  "URGENCIAS",
    _SEARCH_POLYCLINIC: "POLICLINICA",
}


def _categorize_invoices(
    validator,
    invoices: list,
    repo,
    hospital: str,
    period: str,
) -> tuple[list, list, list, list, list]:
    """Detect invoice types from PDF content and persist them to the DB.

    Args:
        validator: InvoiceValidator instance.
        invoices: List of invoice PDF paths.
        repo: AuditRepository instance.
        hospital: Active hospital key.
        period: Audit period string.

    Returns:
        Tuple of (dirs_lab, dirs_ecg, xray_dirs, polyclinic_dirs, emergency_dirs).
    """
    from db.constants import InvoiceType

    dirs_ecg        = validator.find_files_with_text(invoices, _SEARCH_ECG,        return_parent=True)
    dirs_lab        = validator.find_files_with_text(invoices, _SEARCH_LABORATORY, return_parent=True)
    xray_dirs       = validator.find_files_with_text(invoices, _SEARCH_XRAY,       return_parent=True)
    polyclinic_dirs = validator.find_files_with_text(invoices, _SEARCH_POLYCLINIC, return_parent=True)
    emergency_dirs  = validator.find_files_with_text(invoices, _SEARCH_EMERGENCY,  return_parent=True)

    for dirs, tipo in [
        (dirs_lab,        InvoiceType.LABORATORIO),
        (dirs_ecg,        InvoiceType.ECG),
        (xray_dirs,       InvoiceType.RADIOGRAFIA),
        (emergency_dirs,  InvoiceType.URGENCIAS),
        (polyclinic_dirs, InvoiceType.POLICLINICA),
    ]:
        for d in dirs:
            with contextlib.suppress(ValueError):
                repo.update_tipo(hospital, period, d.name.upper(), tipo)

    return dirs_lab, dirs_ecg, xray_dirs, polyclinic_dirs, emergency_dirs


_DOC_CHECK_STAGES = frozenset({
    "CHECK_HISTORIA",
    "CHECK_RESULTADOS",
    "CHECK_FIRMA",
    "CHECK_VALIDACION",
    "CHECK_AUTORIZACION",
})


def _build_doc_check_context(
    scanner,
    inspector,
    validator,
    invoices: list,
    skip_dirs: list,
    repo,
    hospital: str,
    period: str,
) -> dict:
    """Compute shared directory groupings needed for individual document checks.

    Args:
        scanner: DocumentScanner instance.
        inspector: FolderInspector instance.
        validator: InvoiceValidator instance.
        invoices: List of invoice PDF paths.
        skip_dirs: Directories to skip (e.g. SOAT invoices).
        repo: AuditRepository instance.
        hospital: Active hospital key.
        period: Audit period string.

    Returns:
        Dict with keys: ``emergency_dirs``, ``results_dirs``, ``history_dirs``,
        ``dirs_lab_test``.
    """
    from db.constants import InvoiceType

    all_dirs = scanner.list_dirs()

    dirs_lab, dirs_ecg, xray_dirs, polyclinic_dirs, emergency_dirs = _categorize_invoices(
        validator, invoices, repo, hospital, period
    )

    results_dirs = list(set(dirs_lab + xray_dirs + dirs_ecg) - set(polyclinic_dirs))
    history_dirs = list(set(all_dirs) - set(polyclinic_dirs) - set(results_dirs))

    lab_facturas  = repo.fetch_by_tipo(hospital, period, [
        InvoiceType.LABORATORIO, InvoiceType.ECG, InvoiceType.RADIOGRAFIA,
    ])
    dirs_lab_test = inspector.resolve_dir_paths(lab_facturas)

    return {
        "emergency_dirs": emergency_dirs,
        "results_dirs":   results_dirs,
        "history_dirs":   history_dirs,
        "dirs_lab_test":  dirs_lab_test,
    }


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

            if flags.get("ORGANIZE"):
                organizer = InvoiceOrganizer(
                    df=df_processed,
                    staging_base=staging_dir,
                    final_base=archive_dir,
                )
                result = organizer.organize(dry_run=False)
                pipeline_logger.info("Organise result: %s", result)

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

        # ── Document presence checks ─────────────────────────────────────────

        if any(flags.get(k) for k in _DOC_CHECK_STAGES):
            ctx = _build_doc_check_context(
                scanner, inspector, validator, invoices, skip_dirs,
                repo, hospital, period,
            )
            emergency_dirs = ctx["emergency_dirs"]
            results_dirs   = ctx["results_dirs"]
            history_dirs   = ctx["history_dirs"]
            dirs_lab_test  = ctx["dirs_lab_test"]

            def _log_missing(dirs: list, label: str) -> None:
                pipeline_logger.info("%s — %d directorio(s):", label, len(dirs))
                for d in dirs:
                    pipeline_logger.info("  %s", d.name.upper())

            if flags.get("CHECK_HISTORIA"):
                missing = inspector.find_dirs_missing_file(
                    doc_standards.get("HISTORIA", ""),
                    skip=skip_dirs + dirs_lab_test,
                    target_dirs=history_dirs,
                )
                _log_missing(missing, "Historias clínicas faltantes")

            if flags.get("CHECK_RESULTADOS"):
                missing = inspector.find_dirs_missing_file(
                    doc_standards.get("RESULTADOS", ""),
                    skip=skip_dirs + emergency_dirs,
                    target_dirs=results_dirs,
                )
                _log_missing(missing, "Resultados faltantes")

            if flags.get("CHECK_FIRMA"):
                missing = inspector.find_dirs_missing_file(
                    doc_standards.get("FIRMA", ""),
                    skip=skip_dirs,
                )
                _log_missing(missing, "Firmas faltantes")

            if flags.get("CHECK_VALIDACION"):
                missing = inspector.find_dirs_missing_file(
                    doc_standards.get("VALIDACION", ""),
                    skip=skip_dirs + emergency_dirs,
                )
                _log_missing(missing, "Validaciones faltantes")

            if flags.get("CHECK_AUTORIZACION"):
                missing = inspector.find_dirs_missing_file(
                    doc_standards.get("AUTORIZACION", ""),
                    skip=skip_dirs,
                    target_dirs=emergency_dirs,
                )
                _log_missing(missing, "Autorizaciones faltantes")

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
