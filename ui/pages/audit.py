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
        st.warning(f"Audit database not found: `{db_path}`")
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
        st.markdown(metric_card("Total facturas", total, "en este período"), unsafe_allow_html=True)
    with m2:
        color_wf = "amber" if with_findings else ""
        st.markdown(
            metric_card("Con hallazgos", with_findings, f"{100 - pct_clean}% del total", color=color_wf),
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            metric_card("Sin hallazgos", clean, f"{pct_clean}% del total", color="green"),
            unsafe_allow_html=True,
        )
    with m4:
        color_rate = "green" if pct_clean >= 80 else "amber"
        st.markdown(
            metric_card("Tasa de aprobación", f"{pct_clean}%", "facturas sin hallazgos", color=color_rate),
            unsafe_allow_html=True,
        )
    with m5:
        color_missing = "red" if missing_count else "green"
        st.markdown(
            metric_card("Carpetas faltantes", missing_count, "no encontradas en disco", color=color_missing),
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Invoice table ---
    section_header("Facturas del período")

    # --- Summary breakdown (by type, folder status, and findings) ---
    with st.expander("Resumen por tipo, estado de carpeta y hallazgos", expanded=False):
        br_col1, br_col2, br_col3 = st.columns(3)
        with br_col1:
            st.caption("**Registros por tipo de factura**")
            tipo_counts = (
                df.groupby("Tipo").size()
                .reset_index(name="Cantidad")
                .sort_values("Cantidad", ascending=False)
            )
            st.dataframe(tipo_counts, hide_index=True, width="stretch")
        with br_col2:
            st.caption("**Registros por estado de carpeta**")
            estado_counts = (
                df.groupby("Estado carpeta").size()
                .reset_index(name="Cantidad")
                .sort_values("Cantidad", ascending=False)
            )
            st.dataframe(estado_counts, hide_index=True, width="stretch")
        with br_col3:
            st.caption("**Hallazgos más frecuentes**")
            all_findings = (
                df["Comentario"]
                .loc[df["Comentario"] != ""]
                .str.split("; ")
                .explode()
                .str.strip()
            )
            if all_findings.empty:
                st.caption("Sin hallazgos registrados.")
            else:
                finding_counts = (
                    all_findings.value_counts()
                    .rename_axis("Hallazgo")
                    .reset_index(name="Cantidad")
                )
                st.dataframe(finding_counts, hide_index=True, width="stretch")

    # --- Filters ---
    tipo_options   = ["Todos"] + sorted(set(InvoiceType))
    estado_options = ["Todos"] + sorted(set(FolderStatus))

    f1, f2, f3, *_ = st.columns([2, 2, 2, 2])
    tipo_filter    = f1.selectbox("Tipo de factura", tipo_options, key="tipo_filter")
    estado_filter  = f2.selectbox("Estado de carpeta", estado_options, key="estado_filter")
    hallazgo_search = f3.text_input(
        "Buscar hallazgo",
        placeholder="ej: Firma, CUFE…",
        key="hallazgo_search",
    )

    display_df = df.copy()
    if tipo_filter != "Todos":
        display_df = display_df[display_df["Tipo"] == tipo_filter]
    if estado_filter != "Todos":
        display_df = display_df[display_df["Estado carpeta"] == estado_filter]
    if hallazgo_search:
        display_df = display_df[
            display_df["Comentario"].str.contains(hallazgo_search, case=False, na=False)
        ]

    st.dataframe(
        display_df.style.apply(_highlight_row, axis=1),
        width="stretch",
        height=320,
    )

    dl_col, *_ = st.columns([2, 6])
    excel_bytes = _df_to_excel_bytes(df.reset_index())
    dl_col.download_button(
        label="Exportar a Excel",
        data=excel_bytes,
        file_name=f"{hospital}_{period}_AUDIT.xlsx",
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
            st.success(f"{label} aplicado a {len(ok)} factura(s).")
        if missing:
            st.warning(f"{len(missing)} factura(s) no encontradas: {', '.join(missing)}")
        if ok:
            st.rerun()

    with st.expander("Estado de carpeta en lote"):
        raw_fs   = st.text_area("Facturas (una por línea)", key="batch_fs_list", height=120)
        nuevo_fs = st.selectbox("Nuevo estado", sorted(set(FolderStatus)), key="batch_fs_val")
        if st.button("Aplicar estado", key="btn_batch_fs", type="primary"):
            _apply_batch(
                raw_fs,
                lambda f: repo.update_folder_status(hospital, period, f, nuevo_fs),
                f"Estado '{nuevo_fs}'",
            )

    with st.expander("Agregar hallazgos en lote"):
        raw_hf   = st.text_area("Facturas (una por línea)", key="batch_hf_list", height=120)
        nuevo_hf = st.selectbox("Tipo de hallazgo", sorted(set(FindingCode)), key="batch_hf_val")
        if st.button("Agregar hallazgo", key="btn_batch_hf", type="primary"):
            _apply_batch(
                raw_hf,
                lambda f: repo.record_finding(hospital, period, f, nuevo_hf),
                f"Hallazgo '{nuevo_hf}'",
            )

    with st.expander("Eliminar hallazgos en lote"):
        raw_del_hf = st.text_area("Facturas (una por línea)", key="batch_del_hf_list", height=120)
        del_hf     = st.selectbox("Hallazgo a eliminar", sorted(set(FindingCode)), key="batch_del_hf_val")
        if st.button("Eliminar hallazgo", key="btn_batch_del_hf", type="primary"):
            _apply_batch(
                raw_del_hf,
                lambda f: repo.delete_finding(hospital, period, f, del_hf),
                f"Hallazgo '{del_hf}' eliminado",
            )

    with st.expander("Tipo de factura en lote"):
        raw_tp   = st.text_area("Facturas (una por línea)", key="batch_tp_list", height=120)
        nuevo_tp = st.selectbox("Nuevo tipo", sorted(set(InvoiceType)), key="batch_tp_val")
        if st.button("Aplicar tipo", key="btn_batch_tp", type="primary"):
            _apply_batch(
                raw_tp,
                lambda f: repo.update_tipo(hospital, period, f, nuevo_tp),
                f"Tipo '{nuevo_tp}'",
            )

    st.divider()

    # --- Invoice detail ---
    section_header("Detalle de factura y hallazgos")

    detail_col, action_col = st.columns([5, 3])

    with detail_col:
        invoice_id = st.text_input(
            "Número de factura",
            placeholder="ej. FE12345 — escribe y presiona Enter",
        )
        if not invoice_id:
            st.caption("Ingresa un número de factura para ver y gestionar sus hallazgos.")
        elif invoice_id not in df.index:
            st.error(f"Factura `{invoice_id}` no encontrada en este período.")
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
                "Acción",
                ["Agregar hallazgo", "Eliminar hallazgo", "Cambiar tipo"],
                horizontal=False,
            )

            if action == "Agregar hallazgo":
                ft_add = st.selectbox("Tipo de documento faltante", all_finding_codes, key="add_ft")
                if st.button("Agregar", type="primary", key="btn_add", width="stretch"):
                    repo.record_finding(hospital, period, invoice_id, ft_add)
                    st.success("Hallazgo agregado.")
                    st.rerun()

            elif action == "Eliminar hallazgo" and existing_types:
                ft_del = st.selectbox("Hallazgo a eliminar", existing_types, key="del_ft")
                if st.button("Eliminar", type="primary", key="btn_del", width="stretch"):
                    repo.delete_finding(hospital, period, invoice_id, ft_del)
                    st.success("Hallazgo eliminado.")
                    st.rerun()

            elif action == "Cambiar tipo":
                current_tipo = df.at[invoice_id, "Tipo"]
                all_tipos = sorted(set(InvoiceType))
                cur_tipo_idx = all_tipos.index(current_tipo) if current_tipo in all_tipos else 0
                new_tipo = st.selectbox("Tipo de factura", all_tipos, index=cur_tipo_idx, key="change_tipo")
                st.caption("Establece el tipo **SOAT** para excluir esta factura de la verificación de documentos.")
                if st.button("Aplicar", type="primary", key="btn_tipo", width="stretch"):
                    repo.update_tipo(hospital, period, invoice_id, new_tipo)
                    st.success(f"Tipo actualizado a {new_tipo}.")
                    st.rerun()

            elif not existing_types and action == "Eliminar hallazgo":
                st.caption("Esta factura no tiene hallazgos para eliminar.")
