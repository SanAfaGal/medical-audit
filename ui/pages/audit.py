"""Audit page: browse invoices, manage findings, and export reports."""

from __future__ import annotations

import io
import logging

import pandas as pd
import streamlit as st

from ui.widgets import finding_row, metric_card, section_header, status_badge

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a DataFrame to Excel bytes for download.

    Args:
        df: DataFrame to serialise.

    Returns:
        Raw bytes of the ``.xlsx`` file.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Audit")
    return buf.getvalue()


def _highlight_row(row: pd.Series) -> list[str]:
    """Apply row-level highlighting to invoices that have findings.

    Args:
        row: A DataFrame row.

    Returns:
        List of CSS style strings, one per cell.
    """
    style = "background-color:#3D1F0A;color:#FED7AA;" if row.get("Comentario") else ""
    return [style] * len(row)


# ---------------------------------------------------------------------------
# Page render
# ---------------------------------------------------------------------------


def render(config_error: str | None) -> None:
    """Render the Audit page.

    Args:
        config_error: Error string if Settings failed to load, else ``None``.
    """
    if config_error:
        from ui.widgets import config_error_banner
        config_error_banner(config_error)
        return

    from config.settings import Settings
    from db.repository import AuditRepository
    from db.schema import FindingCode, FindingStatus, FolderStatus, InvoiceType

    db_path = Settings.db_path
    if not db_path.exists():
        st.warning("Audit database not found: `%s`" % db_path)
        st.info(
            "Run the **Load and process SIHOS report** pipeline stage "
            "to initialise the database."
        )
        return

    repo = AuditRepository(db_path)
    hp_map = repo.fetch_hospitals_and_periods()

    if not hp_map:
        st.info("The audit database is empty. Load a SIHOS report first.")
        return

    # --- Selectors ---
    sel1, sel2, *_ = st.columns([2, 2, 4])
    hospital = sel1.selectbox("Hospital", options=sorted(hp_map.keys()))
    period   = sel2.selectbox("Period",   options=hp_map.get(hospital, []))

    if not hospital or not period:
        return

    df = repo.to_dataframe(hospital, period)

    if df.empty:
        st.info("No invoices recorded for this period.")
        return

    # --- Summary metrics ---
    total         = len(df)
    with_findings = int((df["Comentario"] != "").sum())
    clean         = total - with_findings
    pct_clean     = int(clean / total * 100) if total else 0
    missing_count = int((df["Estado carpeta"] == FolderStatus.MISSING).sum())

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(metric_card("Total invoices", total, "in this period"), unsafe_allow_html=True)
    with m2:
        color_wf = "amber" if with_findings else ""
        st.markdown(
            metric_card("With findings", with_findings, "%d%% of total" % (100 - pct_clean), color=color_wf),
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            metric_card("Without findings", clean, "%d%% of total" % pct_clean, color="green"),
            unsafe_allow_html=True,
        )
    with m4:
        color_rate = "green" if pct_clean >= 80 else "amber"
        st.markdown(
            metric_card("Approval rate", "%d%%" % pct_clean, "invoices with no findings", color=color_rate),
            unsafe_allow_html=True,
        )
    with m5:
        color_missing = "red" if missing_count else "green"
        st.markdown(
            metric_card("Missing folders", missing_count, "not found on disk", color=color_missing),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Invoice table ---
    section_header("Period invoices")

    tipo_options = ["All"] + sorted(InvoiceType._ALL)
    filter_col, *_ = st.columns([2, 6])
    tipo_filter = filter_col.selectbox("Filter by type", tipo_options, key="tipo_filter")
    display_df = df if tipo_filter == "All" else df[df["Tipo"] == tipo_filter]

    st.dataframe(
        display_df.style.apply(_highlight_row, axis=1),
        width="stretch",
        height=320,
    )

    dl_col, *_ = st.columns([2, 6])
    excel_bytes = _df_to_excel_bytes(df.reset_index())
    dl_col.download_button(
        label="Export to Excel",
        data=excel_bytes,
        file_name="%s_%s_AUDIT.xlsx" % (hospital, period),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

    st.divider()

    # --- Invoice detail ---
    section_header("Invoice detail and finding management")

    detail_col, action_col = st.columns([5, 3])

    with detail_col:
        invoice_id = st.text_input(
            "Invoice number",
            placeholder="e.g. FE12345 — type and press Enter",
        )
        if not invoice_id:
            st.caption("Enter an invoice number to view and manage its findings.")
            return

        if invoice_id not in df.index:
            st.error("Invoice `%s` not found in this period." % invoice_id)
            return

        findings = repo.fetch_findings(hospital, period, invoice_id)

        if findings:
            st.markdown("**Recorded findings:**")
            cards_html = ""
            for f in findings:
                badge = status_badge(f["status"])
                cards_html += (
                    f'<div class="finding-card">'
                    f'<span class="finding-type">{f["finding_type"]}</span>'
                    f"{badge}"
                    f'{"<span class=finding-note>" + f["note"] + "</span>" if f.get("note") else ""}'
                    f"</div>"
                )
            st.markdown(cards_html, unsafe_allow_html=True)
        else:
            st.success("This invoice has no recorded findings.")

    all_finding_codes  = sorted(FindingCode._ALL)
    all_statuses       = sorted(FindingStatus._ALL)
    existing_types     = [f["finding_type"] for f in findings] if findings else []

    with action_col:
        if not invoice_id or invoice_id not in df.index:
            return

        action = st.radio(
            "Action",
            ["Add finding", "Edit finding", "Remove finding", "Change type"],
            horizontal=False,
        )

        if action == "Add finding":
            ft_add     = st.selectbox("Type",            all_finding_codes, key="add_ft")
            status_add = st.selectbox("Status",          all_statuses,      key="add_status")
            note_add   = st.text_input("Note (optional)",                   key="add_note")
            if st.button("Add", type="primary", key="btn_add", width="stretch"):
                repo.record_finding(
                    hospital, period, invoice_id, ft_add,
                    status=status_add, note=note_add,
                )
                st.success("Finding added.")
                st.rerun()

        elif action == "Edit finding" and existing_types:
            ft_edit  = st.selectbox("Finding", existing_types, key="edit_ft")
            current  = next(f for f in findings if f["finding_type"] == ft_edit)
            cur_idx  = all_statuses.index(current["status"]) if current["status"] in all_statuses else 0
            new_status = st.selectbox("Status", all_statuses, index=cur_idx, key="edit_status")
            new_note   = st.text_input("Note", value=current.get("note", ""), key="edit_note")
            if st.button("Save", type="primary", key="btn_edit", width="stretch"):
                repo.update_status(hospital, period, invoice_id, ft_edit, new_status)
                repo.update_note(hospital, period, invoice_id, ft_edit, new_note)
                st.success("Updated.")
                st.rerun()

        elif action == "Remove finding" and existing_types:
            ft_del = st.selectbox("Finding to remove", existing_types, key="del_ft")
            if st.button("Remove", type="primary", key="btn_del", width="stretch"):
                repo.delete_finding(hospital, period, invoice_id, ft_del)
                st.success("Removed.")
                st.rerun()

        elif action == "Change type":
            current_tipo = df.at[invoice_id, "Tipo"] if invoice_id in df.index else "GENERAL"
            all_tipos = sorted(InvoiceType._ALL)
            cur_tipo_idx = all_tipos.index(current_tipo) if current_tipo in all_tipos else 0
            new_tipo = st.selectbox("Invoice type", all_tipos, index=cur_tipo_idx, key="change_tipo")
            st.caption("Set type to **SOAT** to exclude this invoice from document checks.")
            if st.button("Apply", type="primary", key="btn_tipo", width="stretch"):
                repo.update_tipo(hospital, period, invoice_id, new_tipo)
                st.success("Type updated to %s." % new_tipo)
                st.rerun()

        elif not existing_types and action in ("Edit finding", "Remove finding"):
            st.caption("This invoice has no findings to edit or remove.")
