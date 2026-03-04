"""Documents page: view and edit the .txt input/output files used by the pipeline."""

from __future__ import annotations

import logging

import streamlit as st

logger = logging.getLogger(__name__)

_DOC_FILES: list[tuple[str, str]] = [
    ("missing_folders.txt",      "Missing folders"),
    ("missing_files.txt",        "Missing files"),
    ("skip_soat.txt",            "SOAT skip list"),
    ("invoices.txt",             "Invoice list"),
    ("lab.txt",                  "Lab folders"),
    ("invoices_to_download.txt", "Invoices to download from SIHOS"),
]


def render(config_error: str | None) -> None:
    """Render the Documents page.

    Args:
        config_error: Error string if Settings failed to load, else ``None``.
    """
    if config_error:
        from ui.widgets import config_error_banner
        config_error_banner(config_error)
        return

    from config.settings import Settings

    docs_dir = Settings.docs_dir
    if not docs_dir.exists():
        st.info(
            "The DOCS directory does not yet exist: `%s`. "
            "It will be created automatically when you save the first file." % docs_dir
        )

    left_files  = _DOC_FILES[:3]
    right_files = _DOC_FILES[3:]

    col_left, col_right = st.columns(2)

    for col, files in ((col_left, left_files), (col_right, right_files)):
        with col:
            for filename, label in files:
                path            = docs_dir / filename
                current_content = path.read_text(encoding="utf-8") if path.exists() else ""
                line_count      = len(current_content.splitlines()) if current_content else 0
                char_count      = len(current_content)

                st.markdown(
                    '<div class="doc-header">'
                    '<span class="doc-name">%s</span>'
                    '<span class="doc-meta">%s &nbsp;&nbsp; %d lines &nbsp;&nbsp; %d chars</span>'
                    "</div>" % (label, filename, line_count, char_count),
                    unsafe_allow_html=True,
                )

                edited = st.text_area(
                    label=filename,
                    value=current_content,
                    height=180,
                    key="doc_%s" % filename,
                    label_visibility="collapsed",
                )

                save_col, _ = st.columns([1, 3])
                if save_col.button("Save", key="save_%s" % filename, width="stretch"):
                    docs_dir.mkdir(parents=True, exist_ok=True)
                    path.write_text(edited, encoding="utf-8")
                    st.success("Saved: %s" % filename)
                    logger.info("Document saved: %s", path)

                st.markdown("<div style='height:.65rem'></div>", unsafe_allow_html=True)
