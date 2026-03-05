"""Audit page: browse invoices, manage findings, and export reports."""

from __future__ import annotations

import io
import logging

import pandas as pd
import streamlit as st

from ui.widgets import finding_row, metric_card, section_header

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
    from db.schema import FindingCode, FolderStatus, InvoiceType

    db_path = Settings.db_path
    if not db_path.exists():
        st.warning("Audit database not found: `%s`" % db_path)
        st.info(
            "Run the **Load and process SIHOS report** pipeline stage "
            "to initialise the database."
        )
        return

    repo = AuditRepository(db_path)

    hospital = st.session_state.get("sel_hospital")
    period   = st.session_state.get("sel_period")

    if not hospital or not period:
        st.info("Select a hospital and period in the header to view audit data.")
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

    tipo_options = ["All"] + sorted(set(InvoiceType))
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

    # -----------------------------------------------------------------------
    # Batch operations
    # -----------------------------------------------------------------------

    st.divider()
    section_header("Operaciones en lote")

    known_facturas = set(df.index)

    def _apply_batch(raw: str, fn, label: str) -> None:
        facturas = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if not facturas:
            st.warning("Ingresa al menos una factura.")
            return
        ok, missing = [], []
        for f in facturas:
            if f in known_facturas:
                fn(f)
                ok.append(f)
            else:
                missing.append(f)
        if ok:
            st.success("%s aplicado a %d factura(s)." % (label, len(ok)))
        if missing:
            st.warning("%d factura(s) no encontradas: %s" % (len(missing), ", ".join(missing)))
        if ok:
            st.rerun()

    with st.expander("Estado de carpeta en lote"):
        raw_fs   = st.text_area("Facturas (una por línea)", key="batch_fs_list", height=120)
        nuevo_fs = st.selectbox("Nuevo estado", sorted(set(FolderStatus)), key="batch_fs_val")
        if st.button("Aplicar estado", key="btn_batch_fs", type="primary"):
            _apply_batch(
                raw_fs,
                lambda f: repo.update_folder_status(hospital, period, f, nuevo_fs),
                "Estado '%s'" % nuevo_fs,
            )

    with st.expander("Hallazgos en lote"):
        raw_hf   = st.text_area("Facturas (una por línea)", key="batch_hf_list", height=120)
        nuevo_hf = st.selectbox("Tipo de hallazgo", sorted(set(FindingCode)), key="batch_hf_val")
        if st.button("Agregar hallazgo", key="btn_batch_hf", type="primary"):
            _apply_batch(
                raw_hf,
                lambda f: repo.record_finding(hospital, period, f, nuevo_hf),
                "Hallazgo '%s'" % nuevo_hf,
            )

    with st.expander("Tipo de factura en lote"):
        raw_tp   = st.text_area("Facturas (una por línea)", key="batch_tp_list", height=120)
        nuevo_tp = st.selectbox("Nuevo tipo", sorted(set(InvoiceType)), key="batch_tp_val")
        if st.button("Aplicar tipo", key="btn_batch_tp", type="primary"):
            _apply_batch(
                raw_tp,
                lambda f: repo.update_tipo(hospital, period, f, nuevo_tp),
                "Tipo '%s'" % nuevo_tp,
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
        elif invoice_id not in df.index:
            st.error("Invoice `%s` not found in this period." % invoice_id)
        else:
            findings = repo.fetch_findings(hospital, period, invoice_id)

            if findings:
                st.markdown("**Archivos faltantes:**")
                cards_html = "".join(finding_row(ft) for ft in findings)
                st.markdown(cards_html, unsafe_allow_html=True)
            else:
                st.success("Esta carpeta no tiene archivos faltantes.")

            # Folder-level note
            section_header("Nota de carpeta")
            current_nota = df.at[invoice_id, "Nota"]
            new_nota = st.text_area(
                "Nota",
                value=current_nota,
                height=100,
                key="nota_input",
                label_visibility="collapsed",
            )
            if st.button("Guardar nota", key="btn_nota"):
                repo.update_nota(hospital, period, invoice_id, new_nota)
                st.success("Nota guardada.")
                st.rerun()

    with action_col:
        if invoice_id and invoice_id in df.index:
            findings = repo.fetch_findings(hospital, period, invoice_id)
            all_finding_codes = sorted(set(FindingCode))
            existing_types    = findings

            action = st.radio(
                "Action",
                ["Add finding", "Remove finding", "Change type"],
                horizontal=False,
            )

            if action == "Add finding":
                ft_add = st.selectbox("Missing document type", all_finding_codes, key="add_ft")
                if st.button("Add", type="primary", key="btn_add", width="stretch"):
                    repo.record_finding(hospital, period, invoice_id, ft_add)
                    st.success("Finding added.")
                    st.rerun()

            elif action == "Remove finding" and existing_types:
                ft_del = st.selectbox("Finding to remove", existing_types, key="del_ft")
                if st.button("Remove", type="primary", key="btn_del", width="stretch"):
                    repo.delete_finding(hospital, period, invoice_id, ft_del)
                    st.success("Removed.")
                    st.rerun()

            elif action == "Change type":
                current_tipo = df.at[invoice_id, "Tipo"]
                all_tipos = sorted(set(InvoiceType))
                cur_tipo_idx = all_tipos.index(current_tipo) if current_tipo in all_tipos else 0
                new_tipo = st.selectbox("Invoice type", all_tipos, index=cur_tipo_idx, key="change_tipo")
                st.caption("Set type to **SOAT** to exclude this invoice from document checks.")
                if st.button("Apply", type="primary", key="btn_tipo", width="stretch"):
                    repo.update_tipo(hospital, period, invoice_id, new_tipo)
                    st.success("Type updated to %s." % new_tipo)
                    st.rerun()

            elif not existing_types and action == "Remove finding":
                st.caption("This invoice has no findings to remove.")
