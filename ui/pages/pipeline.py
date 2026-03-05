"""Pipeline page: toggle and execute audit pipeline stages from the Streamlit UI."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import streamlit as st

from ui.widgets import log_viewer, section_header

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipeline run state (module-level so it survives Streamlit reruns)
# ---------------------------------------------------------------------------

_cancel_event = threading.Event()

# Shared dict updated by the pipeline thread; read by the UI polling loop.
_pipe: dict = {
    "running": False,
    "log":     "",
    "error":   False,
}


class _LiveLogHandler(logging.Handler):
    """Logging handler that streams records to a Streamlit placeholder in real time."""

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
# Stage metadata
# ---------------------------------------------------------------------------

_STAGE_LABELS: dict[str, str] = {
    "LOAD_AND_PROCESS":              "Load and process SIHOS report",
    "EXPORT_INVOICES":               "Export invoices to Excel",
    "ORGANIZE":                      "Organise invoice folders",
    "DOWNLOAD_DRIVE":                "Download missing folders from Drive",
    "NORMALIZE_FILES":               "Normalise files (delete non-PDFs, rename)",
    "CHECK_FOLDERS_WITH_EXTRA_TEXT": "Detect folders with extra text in name",
    "NORMALIZE_DIR_NAMES":           "Rename malformed directory names",
    "CHECK_INVOICE_NUMBER_ON_FILES": "Verify invoice number on files",
    "CHECK_INVOICES":                "Audit invoices (OCR + CUFE)",
    "MARK_DIRS_MISSING_CUFE":        "Mark directories missing CUFE",
    "CHECK_INVALID_FILES":           "Detect unreadable PDF files",
    "CHECK_FOUR_MAIN_FILES":         "Verify four mandatory document types",
    "CHECK_DIRS":                    "Detect missing directories",
    "DOWNLOAD_INVOICES_FROM_SIHOS":  "Download invoices from SIHOS portal",
}

_STAGE_GROUPS: list[tuple[str, list[str]]] = [
    ("Ingestion", [
        "LOAD_AND_PROCESS",
        "EXPORT_INVOICES",
    ]),
    ("Download", [
        "DOWNLOAD_DRIVE",
        "DOWNLOAD_INVOICES_FROM_SIHOS",
    ]),
    ("Organisation", [
        "ORGANIZE",
    ]),
    ("Normalisation", [
        "NORMALIZE_FILES",
        "CHECK_FOLDERS_WITH_EXTRA_TEXT",
        "NORMALIZE_DIR_NAMES",
    ]),
    ("Verification", [
        "CHECK_INVOICE_NUMBER_ON_FILES",
        "CHECK_INVOICES",
        "MARK_DIRS_MISSING_CUFE",
        "CHECK_INVALID_FILES",
        "CHECK_FOUR_MAIN_FILES",
        "CHECK_DIRS",
    ]),
]

# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------

_SEARCH_ECG         = "ELECTROCARD"
_SEARCH_LABORATORY  = "LABORATORIO CLINICO"
_SEARCH_XRAY        = "RADIOGRAF"
_SEARCH_POLYCLINIC  = "P909000"
_SEARCH_EMERGENCY   = "URGENCIA"
_SEARCH_SIGN_VERIFY = "COMPROB"

# Mapping from search text → InvoiceType (imported lazily inside functions)
_TEXT_TO_TIPO: dict[str, str] = {
    _SEARCH_LABORATORY: "LABORATORIO",
    _SEARCH_ECG:        "ECG",
    _SEARCH_XRAY:       "RADIOGRAFIA",
    _SEARCH_EMERGENCY:  "URGENCIAS",
    _SEARCH_POLYCLINIC: "POLICLINICA",
}

def _run_check_four_main_files(
    scanner,
    inspector,
    validator,
    invoices: list,
    skip_dirs: list,
    repo,
    hospital: str,
    period: str,
    hospital_cfg: dict,
) -> None:
    """Execute the four-mandatory-files audit check across invoice directories.

    Detects invoice types via PDF text search and updates the repository.

    Args:
        scanner: DocumentScanner instance.
        inspector: FolderInspector instance.
        validator: InvoiceValidator instance.
        invoices: List of invoice PDF paths.
        skip_dirs: Directories to skip during checks.
        repo: AuditRepository instance for updating invoice types.
        hospital: Active hospital key.
        period: Audit period string.
        hospital_cfg: Hospital configuration dict from the repository.
    """
    from db.schema import InvoiceType

    pipeline_logger = logging.getLogger("app.pipeline")
    all_dirs = scanner.list_dirs()

    dirs_ecg        = validator.find_files_with_text(invoices, _SEARCH_ECG,        return_parent=True)
    dirs_lab        = validator.find_files_with_text(invoices, _SEARCH_LABORATORY, return_parent=True)
    xray_dirs       = validator.find_files_with_text(invoices, _SEARCH_XRAY,       return_parent=True)
    polyclinic_dirs = validator.find_files_with_text(invoices, _SEARCH_POLYCLINIC, return_parent=True)
    emergency_dirs  = validator.find_files_with_text(invoices, _SEARCH_EMERGENCY,  return_parent=True)

    # Update invoice types in DB based on detected PDF content
    for dirs, tipo in [
        (dirs_lab,        InvoiceType.LABORATORIO),
        (dirs_ecg,        InvoiceType.ECG),
        (xray_dirs,       InvoiceType.RADIOGRAFIA),
        (emergency_dirs,  InvoiceType.URGENCIAS),
        (polyclinic_dirs, InvoiceType.POLICLINICA),
    ]:
        for d in dirs:
            factura = d.name.upper()
            try:
                repo.update_tipo(hospital, period, factura, tipo)
            except ValueError:
                pass  # factura not in DB (normalised name mismatch) — skip silently

    results_dirs = list(set(dirs_lab + xray_dirs + dirs_ecg) - set(polyclinic_dirs))
    history_dirs = list(set(all_dirs) - set(polyclinic_dirs) - set(results_dirs))

    # Replace lab.txt: derive lab directories from DB tipo
    lab_facturas  = repo.fetch_by_tipo(hospital, period, [
        InvoiceType.LABORATORIO, InvoiceType.ECG, InvoiceType.RADIOGRAFIA,
    ])
    dirs_lab_test = inspector.resolve_dir_paths(lab_facturas)

    missing_histories = inspector.find_dirs_missing_file(
        hospital_cfg["DOCUMENT_STANDARDS"]["HISTORIA"],
        skip=skip_dirs + dirs_lab_test,
        target_dirs=history_dirs,
    )
    pipeline_logger.info("Directories missing medical history files: %d", len(missing_histories))

    missing_results = inspector.find_dirs_missing_file(
        hospital_cfg["DOCUMENT_STANDARDS"]["RESULTADOS"],
        skip=skip_dirs + emergency_dirs,
        target_dirs=results_dirs,
    )
    pipeline_logger.info("Directories missing results files: %d", len(missing_results))

    missing_signatures = inspector.find_dirs_missing_file(
        hospital_cfg["DOCUMENT_STANDARDS"]["FIRMA"],
        skip=skip_dirs,
    )
    pipeline_logger.info("Directories missing signature files: %d", len(missing_signatures))

    missing_validations = inspector.find_dirs_missing_file(
        hospital_cfg["DOCUMENT_STANDARDS"]["VALIDACION"],
        skip=skip_dirs + emergency_dirs,
    )
    pipeline_logger.info("Directories missing validation files: %d", len(missing_validations))

    missing_auths = inspector.find_dirs_missing_file(
        hospital_cfg["DOCUMENT_STANDARDS"]["AUTORIZACION"],
        skip=skip_dirs,
        target_dirs=emergency_dirs,
    )
    pipeline_logger.info("Directories missing authorization files: %d", len(missing_auths))


def _execute_pipeline(
    flags: dict[str, bool],
    hospital: str,
    period: str,
    live_slot: "st.delta_generator.DeltaGenerator | None" = None,
    on_update: "Callable[[str], None] | None" = None,
) -> str:
    """Run the selected pipeline stages and return captured log output.

    Sets up a logging handler so all module loggers are captured.  When
    *live_slot* is provided, the accumulated log is pushed to it after
    every log record (direct Streamlit update, main thread only).  When
    *on_update* is provided instead, the callback is called with the
    accumulated text — safe to use from a background thread.

    Args:
        flags: Mapping of stage key to enabled boolean.
        live_slot: Optional Streamlit placeholder updated on each log line.
        on_update: Optional callback receiving the full accumulated log text.

    Returns:
        Multi-line string of all log output produced during the run.
    """
    from pathlib import Path

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
    from db.schema import InvoiceType

    def _update_slot(text: str) -> None:
        if live_slot is not None:
            live_slot.code(text, language=None)
        if on_update is not None:
            on_update(text)

    pipeline_logger_early = logging.getLogger("app.pipeline")

    def _cancelled() -> bool:
        if _cancel_event.is_set():
            pipeline_logger_early.warning("Pipeline cancelled by user.")
            return True
        return False

    handler = _LiveLogHandler(_update_slot)
    root = logging.getLogger()
    root.addHandler(handler)

    try:
        repo = AuditRepository(Settings.db_path)

        # Load hospital config from DB
        hospital_cfg = repo.fetch_hospital_config(hospital)

        if not Settings.audit_path:
            logging.getLogger("app.pipeline").error(
                "Audit path not configured. Set it in Settings → Directorio de auditoría."
            )
            return handler.getvalue()

        id_prefix    = hospital_cfg.get("INVOICE_IDENTIFIER_PREFIX", "")
        base_path    = Settings.audit_path / hospital / period
        staging_dir  = base_path / "STAGE"
        archive_dir  = base_path / "AUDIT"
        base_dir     = base_path / "BASE"
        sihos_report = base_path / ("%s_SIHOS.xlsx" % period)
        audit_report = base_path / ("%s_AUDITORIA.xlsx" % period)

        scanner    = DocumentScanner(staging_dir)
        inspector  = FolderInspector(staging_dir, id_prefix=id_prefix)
        operations = DocumentOps(staging_dir, id_prefix=id_prefix)
        validator  = InvoiceValidator(staging_dir, id_prefix=id_prefix)
        admin_map  = repo.fetch_admin_contract_map(hospital)

        pipeline_logger = logging.getLogger("app.pipeline")

        # ── Backup ───────────────────────────────────────────────────────────

        backup_path = repo.backup(Settings.backup_dir)
        if backup_path:
            pipeline_logger.info("Database backed up to: %s", backup_path)

        # ── Ingestion ────────────────────────────────────────────────────────

        df_processed = None

        if flags.get("LOAD_AND_PROCESS"):
            ingester = BillingIngester(admin_map)
            ingester.load_excel(sihos_report, Settings.raw_schema_columns)

            if not ingester.validate_admin_contract_pairs():
                pipeline_logger.warning("Halted: pre-audit validation failed.")
                return handler.getvalue()

            df_processed = ingester.build_invoice_dataframe()
            inserted = repo.upsert_invoices(
                df_processed,
                hospital=hospital,
                period=period,
            )
            pipeline_logger.info("Invoices loaded into audit repository: %d", inserted)

            if flags.get("EXPORT_INVOICES"):
                ingester.export_to_excel(
                    df_processed,
                    audit_report,
                    Settings.export_schema_columns,
                )

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
            from db.schema import FolderStatus
            missing_folders = repo.fetch_by_folder_status(
                hospital, period, FolderStatus.MISSING
            )
            drive = DriveSync(credentials_path=Settings.drive_credentials_path(hospital))
            drive.download_missing_dirs(missing_folders, base_dir)
            leaf_finder = LeafFolderFinder()
            leaf_folders = leaf_finder.find_leaf_folders(base_dir)
            if leaf_folders:
                copier = FolderCopier(staging_dir)
                copier.copy_folders(leaf_folders, use_prefix=False)
                pipeline_logger.info("Leaf folders copied to staging: %d", len(leaf_folders))

        # ── Skip list (SOAT) — derived from DB ───────────────────────────────

        soat_facturas = repo.fetch_by_tipo(
            hospital, period, InvoiceType.SOAT
        )
        skip_dirs = inspector.resolve_dir_paths(soat_facturas)

        if _cancelled():
            return handler.getvalue()

        # ── Normalisation ────────────────────────────────────────────────────

        if flags.get("NORMALIZE_FILES"):
            skip_set = set(skip_dirs)

            def _is_skipped(f) -> bool:  # type: ignore[return]
                return any(d in f.parents for d in skip_set)

            non_pdf = [f for f in scanner.find_non_pdf() if not _is_skipped(f)]
            pipeline_logger.info("Non-PDF files removed: %d", len(operations.remove_files(non_pdf)))

            prefixes_accepted = flatten_prefixes(hospital_cfg["DOCUMENT_STANDARDS"])
            invalid_files = [
                f for f in scanner.find_invalid_names(
                    valid_prefixes=prefixes_accepted,
                    suffix=hospital_cfg["INVOICE_IDENTIFIER_PREFIX"],
                    nit=hospital_cfg["NIT"],
                )
                if not _is_skipped(f)
            ]
            pipeline_logger.info("Files with invalid naming structure: %d", len(invalid_files))

            standardizer = FilenameStandardizer(
                nit=hospital_cfg["NIT"],
                valid_prefixes=prefixes_accepted,
                suffix_const=hospital_cfg["INVOICE_IDENTIFIER_PREFIX"],
                prefix_map=repo.fetch_filename_fixes(),
            )
            standardizer.run(invalid_files)

        if flags.get("CHECK_INVOICE_NUMBER_ON_FILES"):
            mismatched = inspector.find_mismatched_files(skip_dirs=skip_dirs)
            pipeline_logger.info("Files mismatched to folder name: %d", len(mismatched))

        dirs_with_extra_text: list = []
        if flags.get("CHECK_FOLDERS_WITH_EXTRA_TEXT") or flags.get("NORMALIZE_DIR_NAMES"):
            dirs_with_extra_text = inspector.find_malformed_dirs(skip=skip_dirs)

        if flags.get("CHECK_FOLDERS_WITH_EXTRA_TEXT"):
            pipeline_logger.info("Directories with extra text in name: %d", len(dirs_with_extra_text))

        if flags.get("NORMALIZE_DIR_NAMES"):
            renamed = operations.standardize_dir_names(dirs_with_extra_text)
            pipeline_logger.info("Directories renamed: %d", renamed)

        if _cancelled():
            return handler.getvalue()

        # ── Invoice audit ────────────────────────────────────────────────────

        invoices = scanner.find_by_prefix(hospital_cfg["DOCUMENT_STANDARDS"]["FACTURA"])

        if flags.get("CHECK_INVOICES"):
            needing_ocr = DocumentReader.find_needing_ocr(invoices)
            ocr_result  = DocumentProcessor.batch_ocr(files=needing_ocr, max_workers=8)
            pipeline_logger.info("OCR batch completed for invoices: %s", ocr_result)

            missing_code, missing_cufe = validator.validate_invoice_files(invoices)
            pipeline_logger.info("Invoices missing invoice code in content: %d", len(missing_code))
            pipeline_logger.info("Invoices missing CUFE: %d", len(missing_cufe))

            if flags.get("MARK_DIRS_MISSING_CUFE"):
                marked = operations.tag_dirs_missing_cufe(missing_cufe)
                pipeline_logger.info("Directories marked as missing CUFE: %d", marked)

            missing_invoice_files = inspector.find_dirs_missing_file(
                hospital_cfg["DOCUMENT_STANDARDS"]["FACTURA"], skip=skip_dirs
            )
            pipeline_logger.info("Directories missing invoice files: %d", len(missing_invoice_files))

        if flags.get("CHECK_DIRS"):
            from db.schema import FolderStatus
            all_folders = repo.fetch_invoice_ids(hospital, period)
            missing_dirs = inspector.find_missing_dirs(expected_dirs=all_folders)
            pipeline_logger.info("Missing directories: %d", len(missing_dirs))
            for factura in missing_dirs:
                repo.update_folder_status(
                    hospital, period, factura, FolderStatus.MISSING
                )

        if flags.get("CHECK_INVALID_FILES"):
            all_files    = scanner.find_by_extension()
            invalid_pdfs = DocumentReader.find_unreadable(all_files)
            pipeline_logger.info("Unreadable PDF files: %d", len(invalid_pdfs))

        if flags.get("CHECK_FOUR_MAIN_FILES"):
            _run_check_four_main_files(
                scanner, inspector, validator, invoices, skip_dirs,
                repo, hospital, period, hospital_cfg=hospital_cfg,
            )

        if flags.get("CHECK_IF_FILES_NEED_OCR"):
            signatures = scanner.find_by_prefix(hospital_cfg["DOCUMENT_STANDARDS"]["FIRMA"])
            sigs_needing_ocr = DocumentReader.find_needing_ocr(signatures)
            ocr_result = DocumentProcessor.batch_ocr(files=sigs_needing_ocr, max_workers=10)
            pipeline_logger.info("OCR batch completed for signatures: %s", ocr_result)

        if flags.get("CHECK_SIGNS"):
            signatures = scanner.find_by_prefix(hospital_cfg["DOCUMENT_STANDARDS"]["FIRMA"])
            files_with_text = validator.find_files_with_text(
                files=signatures, search_text=_SEARCH_SIGN_VERIFY, return_parent=False
            )
            pipeline_logger.info("Signature files containing verification text: %d", len(files_with_text))

        if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
            from core.downloader import SihosDownloader
            raw_text = st.session_state.get("invoices_to_download", "")
            invoice_numbers = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
            if invoice_numbers:
                doc_standards = hospital_cfg.get("DOCUMENT_STANDARDS", {})
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
        logging.getLogger("app.pipeline").error("Pipeline error: %s", exc, exc_info=True)
    finally:
        root.removeHandler(handler)

    return handler.getvalue()


# ---------------------------------------------------------------------------
# Page render
# ---------------------------------------------------------------------------


def render(config_error: str | None) -> None:
    """Render the Pipeline page.

    Args:
        config_error: Error string if Settings failed to load, else ``None``.
    """
    if config_error:
        from ui.widgets import config_error_banner
        config_error_banner(config_error)
        return

    section_header("Pipeline stages")

    flags: dict[str, bool] = {}
    cols = st.columns(3)

    for idx, (group_name, group_keys) in enumerate(_STAGE_GROUPS):
        with cols[idx % 3]:
            st.markdown(
                f'<div class="group-card">'
                f'<div class="group-title">{group_name}</div>',
                unsafe_allow_html=True,
            )
            for key in group_keys:
                flags[key] = st.checkbox(_STAGE_LABELS[key], key="stage_%s" % key)
            st.markdown("</div>", unsafe_allow_html=True)

    # Controls row
    c_clear, _, c_run = st.columns([1.5, 4.0, 1.5])
    if c_clear.button("Clear selection", width="stretch"):
        for key in _STAGE_LABELS:
            st.session_state["stage_%s" % key] = False
        st.rerun()

    selected = [k for k, v in flags.items() if v]

    if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
        st.divider()
        section_header("Invoice numbers to download")
        st.text_area(
            "Paste invoice numbers (one per line)",
            key="invoices_to_download",
            height=140,
            placeholder="FE12345\nFE12346\n...",
        )

    st.divider()

    if selected:
        joined = ", ".join(_STAGE_LABELS[k] for k in selected)
        st.markdown(
            '<div class="run-summary"><b>%d stage(s) selected:</b> %s</div>'
            % (len(selected), joined),
            unsafe_allow_html=True,
        )
    else:
        st.caption("No stages selected.")

    run_col, cancel_col, _ = st.columns([1.5, 1.5, 4])
    run_btn    = run_col.button("Run pipeline", type="primary", disabled=not selected or _pipe["running"])
    cancel_btn = cancel_col.button("Cancel", disabled=not _pipe["running"])

    if cancel_btn:
        _cancel_event.set()
        st.rerun()

    # ── While pipeline is running: poll and refresh ──────────────────────────

    if _pipe["running"]:
        status_slot = st.empty()
        status_slot.markdown(
            '<div class="status-bar info">Running — please wait…</div>',
            unsafe_allow_html=True,
        )
        section_header("Pipeline output")
        live_slot = st.empty()
        live_slot.code(_pipe["log"] or "Starting…", language=None)
        time.sleep(0.4)
        st.rerun()

    # ── Show last run result if available ───────────────────────────────────

    elif _pipe["log"]:
        has_error = "ERROR" in _pipe["log"] or "CRITICAL" in _pipe["log"]
        cancelled = "cancelled by user" in _pipe["log"]
        if cancelled:
            st.markdown(
                '<div class="status-bar info">Pipeline cancelled.</div>',
                unsafe_allow_html=True,
            )
        elif has_error:
            st.markdown(
                '<div class="status-bar error">Pipeline finished with errors. See log below.</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="status-bar success">Pipeline completed successfully.</div>',
                unsafe_allow_html=True,
            )
        section_header("Pipeline output")
        log_viewer(_pipe["log"])

    # ── Launch pipeline in background thread ─────────────────────────────────

    if run_btn:
        _cancel_event.clear()
        _pipe["running"] = True
        _pipe["log"]     = ""
        _pipe["error"]   = False

        # Capture flags and session values now — evaluated at click time
        _flags_snapshot    = dict(flags)
        _hospital_snapshot = st.session_state.get("sel_hospital", "")
        _period_snapshot   = st.session_state.get("sel_period", "")

        def _run_thread() -> None:
            try:
                result = _execute_pipeline(
                    _flags_snapshot,
                    hospital=_hospital_snapshot,
                    period=_period_snapshot,
                    on_update=lambda text: _pipe.__setitem__("log", text),
                )
                _pipe["log"] = result
            except Exception as exc:
                _pipe["log"] += "\nERROR (thread): %s" % exc
                _pipe["error"] = True
            finally:
                _pipe["running"] = False

        threading.Thread(target=_run_thread, daemon=True).start()
        st.rerun()
