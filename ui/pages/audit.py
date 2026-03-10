"""Audit page: browse invoices, manage findings, and export reports."""

from __future__ import annotations

import io
import logging

import pandas as pd
import streamlit as st

from ui.widgets import finding_chip, metric_card, section_header

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

    db_path = Settings.db_path
    if not db_path.exists():
        st.warning(f"Audit database not found: `{db_path}`")
        st.info(
            "Run the **Load and process SIHOS report** pipeline stage "
            "to initialise the database."
        )
        return

    repo = AuditRepository(db_path)

    # Load dynamic lookup tables from DB
    doc_labels   = repo.fetch_document_labels()      # {code: label}
    inv_types_db = repo.fetch_invoice_types()         # list[dict]
    folder_stats = repo.fetch_folder_statuses()       # list[dict]
    all_doc_codes = [dt["code"] for dt in repo.fetch_document_types() if dt["is_active"]]

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
    missing_count = int((df["Estado carpeta"] == "FALTANTE").sum())

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
            st.caption("**Registros por tipo de factura (solo presentes)**")
            tipo_counts = (
                df[df["Estado carpeta"] == "PRESENTE"].groupby("Tipo").size()
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
    tipo_options   = ["Todos"] + [it["code"] for it in inv_types_db]
    estado_options = ["Todos"] + [fs["code"] for fs in folder_stats]

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
        display_df = display_df[display_df["Tipo"].str.contains(tipo_filter, regex=False, na=False)]
    if estado_filter != "Todos":
        display_df = display_df[display_df["Estado carpeta"] == estado_filter]
    if hallazgo_search:
        display_df = display_df[
            display_df["Comentario"].str.contains(hallazgo_search, case=False, na=False)
        ]

    _HIDDEN_COLUMNS = {"Fecha": None, "Documento": None, "Numero": None, "Paciente": None, "Operario": None, "Nota": None}
    event = st.dataframe(
        display_df.style.apply(_highlight_row, axis=1),
        column_config=_HIDDEN_COLUMNS,
        on_select="rerun",
        selection_mode="multi-row",
        key="invoice_table",
    )
    selected_rows     = event.selection.rows
    selected_facturas = list(display_df.index[selected_rows])
    st.session_state["selected_facturas"] = selected_facturas

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
    # Context-sensitive panel: detail (1 row) or batch bar (N rows)
    # -----------------------------------------------------------------------

    known_facturas = set(df.index)

    def _apply_batch(facturas: list[str], fn, label: str) -> None:
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

    st.divider()

    if len(selected_facturas) == 1:
        # ── Single-invoice detail panel ──────────────────────────────────────
        invoice_id = selected_facturas[0]
        row        = df.loc[invoice_id]
        section_header(f"Factura: {invoice_id}")
        st.markdown(
            f'<span style="color:#94A3B8;font-size:.85rem;">'
            f'Administradora: <b style="color:#CBD5E1">{row["Administradora"]}</b>'
            f' &nbsp;|&nbsp; Estado: <b style="color:#CBD5E1">{row["Estado carpeta"]}</b>'
            f"</span>",
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        findings = repo.fetch_findings(hospital, period, invoice_id)

        # Chips row
        if findings:
            chips_html = " ".join(finding_chip(doc_labels.get(ft, ft)) for ft in findings)
            st.markdown(
                f'<div style="margin-bottom:.5rem;"><span style="color:#94A3B8;font-size:.82rem;'
                f'margin-right:.5rem;">Hallazgos:</span>{chips_html}</div>',
                unsafe_allow_html=True,
            )
        else:
            st.success("Esta carpeta no tiene hallazgos registrados.")

        # Actions row
        act_c1, act_c2 = st.columns(2)
        with act_c1:
            if findings:
                ft_del = st.selectbox(
                    "Eliminar hallazgo",
                    findings,
                    format_func=lambda c: doc_labels.get(c, c),
                    key="del_ft",
                )
                if st.button("Eliminar", key="btn_del", type="primary", width="stretch"):
                    repo.delete_finding(hospital, period, invoice_id, ft_del)
                    st.success("Hallazgo eliminado.")
                    st.rerun()
            else:
                st.caption("Sin hallazgos para eliminar.")

        with act_c2:
            ft_add = st.selectbox(
                "Agregar hallazgo",
                all_doc_codes,
                format_func=lambda c: doc_labels.get(c, c),
                key="add_ft",
            )
            if st.button("Agregar", key="btn_add", type="primary", width="stretch"):
                repo.record_finding(hospital, period, invoice_id, ft_add)
                st.success("Hallazgo agregado.")
                st.rerun()

        # Types + note row
        tipo_c, nota_c = st.columns([3, 4])
        with tipo_c:
            section_header("Tipos de factura")
            current_tipos_str = df.at[invoice_id, "Tipo"]
            current_tipos     = [t.strip() for t in current_tipos_str.split(",") if t.strip()]
            tipo_opts_list    = [it["code"] for it in inv_types_db]
            new_tipos = st.multiselect(
                "Tipos",
                tipo_opts_list,
                default=[t for t in current_tipos if t in tipo_opts_list],
                format_func=lambda c: next((it["display_name"] for it in inv_types_db if it["code"] == c), c),
                key="change_tipo",
                label_visibility="collapsed",
            )
            st.caption("Selecciona **SOAT** para excluir de la verificación de documentos.")
            if st.button("Guardar tipos", key="btn_tipo", width="stretch"):
                repo.set_tipos(hospital, period, invoice_id, new_tipos or ["GENERAL"])
                st.success(f"Tipos actualizados: {new_tipos or ['GENERAL']}")
                st.rerun()

        with nota_c:
            section_header("Nota de carpeta")
            current_nota = df.at[invoice_id, "Nota"]
            new_nota = st.text_area(
                "Nota",
                value=current_nota,
                height=90,
                key="nota_input",
                label_visibility="collapsed",
            )
            if st.button("Guardar nota", key="btn_nota"):
                repo.update_nota(hospital, period, invoice_id, new_nota)
                st.success("Nota guardada.")
                st.rerun()

    elif len(selected_facturas) > 1:
        # ── Multi-row batch bar ───────────────────────────────────────────────
        section_header(f"Operaciones en lote — {len(selected_facturas)} facturas seleccionadas")
        bc1, bc2, bc3, bc4 = st.columns(4)

        with bc1:
            status_opts = [fs["code"] for fs in folder_stats]
            nuevo_fs    = st.selectbox("Estado", status_opts, key="batch_fs_val")
            if st.button("Aplicar estado", key="btn_batch_fs", type="primary", width="stretch"):
                _apply_batch(
                    selected_facturas,
                    lambda f: repo.update_folder_status(hospital, period, f, nuevo_fs),
                    f"Estado '{nuevo_fs}'",
                )

        with bc2:
            nuevo_hf = st.selectbox(
                "Agregar hallazgo",
                all_doc_codes,
                format_func=lambda c: doc_labels.get(c, c),
                key="batch_hf_val",
            )
            if st.button("Agregar", key="btn_batch_hf", type="primary", width="stretch"):
                _apply_batch(
                    selected_facturas,
                    lambda f: repo.record_finding(hospital, period, f, nuevo_hf),
                    f"Hallazgo '{doc_labels.get(nuevo_hf, nuevo_hf)}'",
                )

        with bc3:
            del_hf = st.selectbox(
                "Eliminar hallazgo",
                all_doc_codes,
                format_func=lambda c: doc_labels.get(c, c),
                key="batch_del_hf_val",
            )
            if st.button("Eliminar", key="btn_batch_del_hf", type="primary", width="stretch"):
                _apply_batch(
                    selected_facturas,
                    lambda f: repo.delete_finding(hospital, period, f, del_hf),
                    f"Hallazgo '{doc_labels.get(del_hf, del_hf)}' eliminado",
                )

        with bc4:
            tipo_opts_b = [it["code"] for it in inv_types_db]
            nuevos_tp   = st.multiselect(
                "Tipos de factura",
                tipo_opts_b,
                format_func=lambda c: next((it["display_name"] for it in inv_types_db if it["code"] == c), c),
                key="batch_tp_val",
            )
            if st.button("Aplicar tipos", key="btn_batch_tp", type="primary", width="stretch"):
                if not nuevos_tp:
                    st.warning("Selecciona al menos un tipo.")
                else:
                    _apply_batch(
                        selected_facturas,
                        lambda f: repo.set_tipos(hospital, period, f, nuevos_tp),
                        f"Tipos {nuevos_tp}",
                    )

    else:
        # ── Nothing selected hint ─────────────────────────────────────────────
        st.caption("Haz clic en una fila para gestionar sus hallazgos, o selecciona varias para operaciones en lote.")

    # ── Batch ops via sidebar invoice list (legacy flow) ─────────────────────
    _shared_invoices = st.session_state.get("shared_invoices", "")
    if _shared_invoices.strip():
        st.divider()
        section_header("Operaciones en lote (lista de la barra lateral)")
        sb1, sb2, sb3, sb4 = st.columns(4)

        with sb1:
            status_opts_sb = [fs["code"] for fs in folder_stats]
            sb_fs          = st.selectbox("Estado", status_opts_sb, key="sb_fs_val")
            if st.button("Aplicar estado", key="btn_sb_fs", type="primary", width="stretch"):
                _apply_batch(
                    [ln.strip() for ln in _shared_invoices.splitlines() if ln.strip()],
                    lambda f: repo.update_folder_status(hospital, period, f, sb_fs),
                    f"Estado '{sb_fs}'",
                )

        with sb2:
            sb_hf_add = st.selectbox(
                "Agregar hallazgo",
                all_doc_codes,
                format_func=lambda c: doc_labels.get(c, c),
                key="sb_hf_add_val",
            )
            if st.button("Agregar", key="btn_sb_hf_add", type="primary", width="stretch"):
                _apply_batch(
                    [ln.strip() for ln in _shared_invoices.splitlines() if ln.strip()],
                    lambda f: repo.record_finding(hospital, period, f, sb_hf_add),
                    f"Hallazgo '{doc_labels.get(sb_hf_add, sb_hf_add)}'",
                )

        with sb3:
            sb_hf_del = st.selectbox(
                "Eliminar hallazgo",
                all_doc_codes,
                format_func=lambda c: doc_labels.get(c, c),
                key="sb_hf_del_val",
            )
            if st.button("Eliminar", key="btn_sb_hf_del", type="primary", width="stretch"):
                _apply_batch(
                    [ln.strip() for ln in _shared_invoices.splitlines() if ln.strip()],
                    lambda f: repo.delete_finding(hospital, period, f, sb_hf_del),
                    f"Hallazgo '{doc_labels.get(sb_hf_del, sb_hf_del)}' eliminado",
                )

        with sb4:
            tipo_opts_sb = [it["code"] for it in inv_types_db]
            sb_tp        = st.multiselect(
                "Tipos de factura",
                tipo_opts_sb,
                format_func=lambda c: next((it["display_name"] for it in inv_types_db if it["code"] == c), c),
                key="sb_tp_val",
            )
            if st.button("Aplicar tipos", key="btn_sb_tp", type="primary", width="stretch"):
                if not sb_tp:
                    st.warning("Selecciona al menos un tipo.")
                else:
                    _apply_batch(
                        [ln.strip() for ln in _shared_invoices.splitlines() if ln.strip()],
                        lambda f: repo.set_tipos(hospital, period, f, sb_tp),
                        f"Tipos {sb_tp}",
                    )

    # ── Zona de peligro ───────────────────────────────────────────────────────
    st.divider()
    with st.expander("⚠️ Acciones avanzadas", expanded=False):
        st.warning(
            f"Esta acción eliminará **todos** los hallazgos de todas las facturas "
            f"del período **{period}** del hospital **{hospital}**."
        )
        with st.popover("🗑️ Eliminar todos los hallazgos del período"):
            st.markdown(
                f"**¿Confirmas la eliminación de todos los hallazgos?**\n\n"
                f"Hospital: `{hospital}` | Período: `{period}`"
            )
            if st.button("Sí, eliminar todos", type="primary", key="confirm_delete_all"):
                count = repo.delete_all_findings(hospital, period)
                st.success(f"Se eliminaron {count} hallazgo(s).")
                st.rerun()
