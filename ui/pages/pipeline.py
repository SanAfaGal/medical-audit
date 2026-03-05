"""Pipeline page: toggle and execute audit pipeline stages from the Streamlit UI."""

from __future__ import annotations

import io
import logging

import streamlit as st

from ui.widgets import log_viewer, section_header

logger = logging.getLogger(__name__)

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
    """
    from config.settings import Settings
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
        Settings.hospital["DOCUMENT_STANDARDS"]["HISTORIA"],
        skip=skip_dirs + dirs_lab_test,
        target_dirs=history_dirs,
    )
    pipeline_logger.info("Directories missing medical history files: %d", len(missing_histories))

    missing_results = inspector.find_dirs_missing_file(
        Settings.hospital["DOCUMENT_STANDARDS"]["RESULTADOS"],
        skip=skip_dirs + emergency_dirs,
        target_dirs=results_dirs,
    )
    pipeline_logger.info("Directories missing results files: %d", len(missing_results))

    missing_signatures = inspector.find_dirs_missing_file(
        Settings.hospital["DOCUMENT_STANDARDS"]["FIRMA"],
        skip=skip_dirs,
    )
    pipeline_logger.info("Directories missing signature files: %d", len(missing_signatures))

    missing_validations = inspector.find_dirs_missing_file(
        Settings.hospital["DOCUMENT_STANDARDS"]["VALIDACION"],
        skip=skip_dirs + emergency_dirs,
    )
    pipeline_logger.info("Directories missing validation files: %d", len(missing_validations))

    missing_auths = inspector.find_dirs_missing_file(
        Settings.hospital["DOCUMENT_STANDARDS"]["AUTORIZACION"],
        skip=skip_dirs,
        target_dirs=emergency_dirs,
    )
    pipeline_logger.info("Directories missing authorization files: %d", len(missing_auths))


def _execute_pipeline(flags: dict[str, bool]) -> str:
    """Run the selected pipeline stages and return captured log output.

    Sets up an in-memory logging handler so all module loggers are captured,
    runs each enabled stage in order, then removes the handler.

    Args:
        flags: Mapping of stage key to enabled boolean.

    Returns:
        Multi-line string of all log output produced during the run.
    """
    from config.settings import Settings
    from core.billing import BillingIngester
    from core.drive import DriveSync
    from core.helpers import flatten_prefixes, read_lines_from_file, write_lines_to_file
    from core.inspector import FolderInspector
    from core.organizer import FolderCopier, InvoiceOrganizer, LeafFolderFinder
    from core.processor import DocumentProcessor
    from core.reader import DocumentReader
    from core.scanner import DocumentScanner
    from core.standardizer import FilenameStandardizer
    from core.validator import InvoiceValidator
    from db.repository import AuditRepository
    from db.schema import InvoiceType

    buf     = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)-8s %(name)s — %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)

    try:
        docs_dir      = Settings.docs_dir
        invoices_list = docs_dir / "invoices.txt"
        missing_folders_list = docs_dir / "missing_folders.txt"

        scanner    = DocumentScanner(Settings.staging_dir)
        inspector  = FolderInspector(Settings.staging_dir)
        ops_cls    = __import__("core.ops", fromlist=["DocumentOps"]).DocumentOps
        operations = ops_cls(Settings.staging_dir)
        validator  = InvoiceValidator(Settings.staging_dir)
        repo       = AuditRepository(Settings.db_path)

        pipeline_logger = logging.getLogger("app.pipeline")

        # ── Ingestion ────────────────────────────────────────────────────────

        df_processed = None

        if flags.get("LOAD_AND_PROCESS"):
            ingester = BillingIngester(Settings.admin_contract_map)
            ingester.load_excel(Settings.sihos_report_path, Settings.raw_schema_columns)

            if not ingester.validate_admin_contract_pairs():
                pipeline_logger.warning("Halted: pre-audit validation failed.")
                return buf.getvalue()

            df_processed = ingester.build_invoice_dataframe()
            inserted = repo.upsert_invoices(
                df_processed,
                hospital=Settings.active_hospital,
                period=Settings.audit_week,
            )
            pipeline_logger.info("Invoices loaded into audit repository: %d", inserted)

            if flags.get("EXPORT_INVOICES"):
                ingester.export_to_excel(
                    df_processed,
                    Settings.audit_report_path,
                    Settings.export_schema_columns,
                )
                ingester.export_invoice_list(df_processed, invoices_list)

            if flags.get("ORGANIZE"):
                organizer = InvoiceOrganizer(
                    df=df_processed,
                    staging_base=Settings.staging_dir,
                    final_base=Settings.archive_dir,
                )
                result = organizer.organize(dry_run=False)
                pipeline_logger.info("Organise result: %s", result)

        # ── Drive download ───────────────────────────────────────────────────

        if flags.get("DOWNLOAD_DRIVE"):
            missing_folders = read_lines_from_file(missing_folders_list) if missing_folders_list.exists() else []
            drive = DriveSync(credentials_path=Settings.drive_credentials)
            drive.download_missing_dirs(missing_folders, Settings.base_dir)
            leaf_finder = LeafFolderFinder()
            leaf_folders = leaf_finder.find_leaf_folders(Settings.base_dir)
            if leaf_folders:
                copier = FolderCopier(Settings.staging_dir)
                copier.copy_folders(leaf_folders, use_prefix=False)
                pipeline_logger.info("Leaf folders copied to staging: %d", len(leaf_folders))

        # ── Skip list (SOAT) — derived from DB ───────────────────────────────

        soat_facturas = repo.fetch_by_tipo(
            Settings.active_hospital, Settings.audit_week, InvoiceType.SOAT
        )
        skip_dirs = inspector.resolve_dir_paths(soat_facturas)

        # ── Normalisation ────────────────────────────────────────────────────

        if flags.get("NORMALIZE_FILES"):
            skip_set = set(skip_dirs)

            def _is_skipped(f) -> bool:  # type: ignore[return]
                return any(d in f.parents for d in skip_set)

            non_pdf = [f for f in scanner.find_non_pdf() if not _is_skipped(f)]
            pipeline_logger.info("Non-PDF files removed: %d", len(operations.remove_files(non_pdf)))

            prefixes_accepted = flatten_prefixes(Settings.hospital["DOCUMENT_STANDARDS"])
            invalid_files = [
                f for f in scanner.find_invalid_names(
                    valid_prefixes=prefixes_accepted,
                    suffix=Settings.hospital["INVOICE_IDENTIFIER_PREFIX"],
                    nit=Settings.hospital["NIT"],
                )
                if not _is_skipped(f)
            ]
            pipeline_logger.info("Files with invalid naming structure: %d", len(invalid_files))

            standardizer = FilenameStandardizer(
                nit=Settings.hospital["NIT"],
                valid_prefixes=prefixes_accepted,
                suffix_const=Settings.hospital["INVOICE_IDENTIFIER_PREFIX"],
                prefix_map=Settings.hospital["MISNAMED_FIXER_MAP"],
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

        # ── Invoice audit ────────────────────────────────────────────────────

        invoices = scanner.find_by_prefix(Settings.hospital["DOCUMENT_STANDARDS"]["FACTURA"])

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
                Settings.hospital["DOCUMENT_STANDARDS"]["FACTURA"], skip=skip_dirs
            )
            pipeline_logger.info("Directories missing invoice files: %d", len(missing_invoice_files))

        if flags.get("CHECK_DIRS"):
            all_folders = read_lines_from_file(invoices_list) if invoices_list.exists() else []
            missing_dirs = inspector.find_missing_dirs(expected_folders=all_folders)
            pipeline_logger.info("Missing directories: %d", len(missing_dirs))
            write_lines_to_file(missing_dirs, missing_folders_list)

        if flags.get("CHECK_INVALID_FILES"):
            all_files    = scanner.find_by_extension()
            invalid_pdfs = DocumentReader.find_unreadable(all_files)
            pipeline_logger.info("Unreadable PDF files: %d", len(invalid_pdfs))

        if flags.get("CHECK_FOUR_MAIN_FILES"):
            _run_check_four_main_files(
                scanner, inspector, validator, invoices, skip_dirs,
                repo, Settings.active_hospital, Settings.audit_week,
            )

        if flags.get("CHECK_IF_FILES_NEED_OCR"):
            signatures = scanner.find_by_prefix(Settings.hospital["DOCUMENT_STANDARDS"]["FIRMA"])
            sigs_needing_ocr = DocumentReader.find_needing_ocr(signatures)
            ocr_result = DocumentProcessor.batch_ocr(files=sigs_needing_ocr, max_workers=10)
            pipeline_logger.info("OCR batch completed for signatures: %s", ocr_result)

        if flags.get("CHECK_SIGNS"):
            signatures = scanner.find_by_prefix(Settings.hospital["DOCUMENT_STANDARDS"]["FIRMA"])
            files_with_text = validator.find_files_with_text(
                files=signatures, search_text=_SEARCH_SIGN_VERIFY, return_parent=False
            )
            pipeline_logger.info("Signature files containing verification text: %d", len(files_with_text))

        if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
            from core.downloader import SihosDownloader
            raw_text = st.session_state.get("invoices_to_download", "")
            invoice_numbers = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
            if invoice_numbers:
                downloader = SihosDownloader()
                downloader.run_from_list(invoice_numbers)
            else:
                pipeline_logger.warning("No invoice numbers provided for SIHOS download.")

    except (OSError, RuntimeError, ValueError) as exc:
        logging.getLogger("app.pipeline").error("Pipeline error: %s", exc, exc_info=True)
    finally:
        root.removeHandler(handler)

    return buf.getvalue()


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
    c_all, c_clear, _, c_run = st.columns([1.3, 1.5, 2.7, 1.5])
    if c_all.button("Select all", width="stretch"):
        for key in _STAGE_LABELS:
            st.session_state["stage_%s" % key] = True
        st.rerun()
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

    if st.button("Run pipeline", type="primary", disabled=not selected):
        status_slot = st.empty()
        status_slot.markdown(
            '<div class="status-bar info">Running %d stage(s)...</div>' % len(selected),
            unsafe_allow_html=True,
        )
        with st.spinner("Pipeline is running — please wait..."):
            log_output = _execute_pipeline(flags)

        has_error = "ERROR" in log_output or "CRITICAL" in log_output
        if has_error:
            status_slot.markdown(
                '<div class="status-bar error">Pipeline finished with errors. See log below.</div>',
                unsafe_allow_html=True,
            )
        else:
            status_slot.markdown(
                '<div class="status-bar success">Pipeline completed successfully.</div>',
                unsafe_allow_html=True,
            )

        if log_output.strip():
            section_header("Pipeline output")
            log_viewer(log_output)
