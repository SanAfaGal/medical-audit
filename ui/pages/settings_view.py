"""Settings page: configuration display, hospital management, and DB info."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from ui.widgets import section_header

logger = logging.getLogger(__name__)

_SKIP_ATTRS = frozenset({
    "hospital",
    "admin_contract_map",
    "raw_schema_columns",
    "export_schema_columns",
})


def render(config_error: str | None) -> None:
    """Render the Settings page.

    Args:
        config_error: Error string if Settings failed to load, else ``None``.
    """
    if config_error:
        from ui.widgets import config_error_banner
        config_error_banner(config_error)
        return

    from config.settings import Settings
    from db.repository import AuditRepository

    repo = AuditRepository(Settings.db_path)

    section_header("Active configuration")

    rows = []
    for key, value in vars(Settings).items():
        if key.startswith("_") or callable(value) or key in _SKIP_ATTRS:
            continue
        rows.append({"Parameter": key, "Value": str(value)})

    if rows:
        st.dataframe(
            pd.DataFrame(rows).set_index("Parameter"),
            width="stretch",
            height=min(40 + len(rows) * 35, 500),
        )
    else:
        st.caption("No configuration parameters to display.")

    st.divider()
    section_header("Key paths")

    path_attrs = [
        ("Staging directory",    Settings.staging_dir),
        ("Archive directory",    Settings.archive_dir),
        ("Base download dir",    Settings.base_dir),
        ("Docs directory",       Settings.docs_dir),
        ("Audit database",       Settings.db_path),
    ]

    for label, path in path_attrs:
        exists = Path(path).exists() if path else False
        indicator = "exists" if exists else "not found"
        st.markdown(
            "**%s** &nbsp; `%s` &nbsp; <span style='font-size:.75rem;color:%s;'>%s</span>"
            % (label, path, "#16A34A" if exists else "#DC2626", indicator),
            unsafe_allow_html=True,
        )

    st.divider()
    section_header("Database")

    db = Path(Settings.db_path)
    if db.exists():
        size_kb = db.stat().st_size / 1024
        st.markdown("**Path** &nbsp; `%s`" % db, unsafe_allow_html=True)
        st.markdown("**Size** &nbsp; `%.1f KB`" % size_kb, unsafe_allow_html=True)
    else:
        st.caption("Database not yet created.")

    backup_dir = Path(Settings.backup_dir)
    backups = sorted(backup_dir.glob("audit_*.db")) if backup_dir.exists() else []
    if backups:
        latest = backups[-1]
        latest_kb = latest.stat().st_size / 1024
        st.markdown(
            "**Last backup** &nbsp; `%s` &nbsp; `%.1f KB`" % (latest.name, latest_kb),
            unsafe_allow_html=True,
        )
        st.caption("%d backup(s) stored in `%s`" % (len(backups), backup_dir))
    else:
        st.caption("No backups found. Run any pipeline stage to create one.")

    # -----------------------------------------------------------------------
    # Hospital management
    # -----------------------------------------------------------------------

    st.divider()
    section_header("Gestión de hospitales")

    hospitals = repo.fetch_all_hospitals()
    hosp_keys = [h["key"] for h in hospitals]

    if not hosp_keys:
        st.info("No hay hospitales registrados en la BD.")
        hosp_keys = []
        sel_hosp = None
    else:
        sel_hosp = st.selectbox("Hospital", hosp_keys, key="settings_hosp_sel")

    # ── Hospital form ────────────────────────────────────────────────────────
    with st.expander("Agregar / editar hospital", expanded=False):
        current = next((h for h in hospitals if h["key"] == sel_hosp), {})
        with st.form("hosp_form"):
            f_key   = st.text_input("Clave (key)", value=sel_hosp or "", key="hf_key")
            f_name  = st.text_input("Nombre para mostrar", value=current.get("display_name", ""), key="hf_name")
            f_nit   = st.text_input("NIT", value=current.get("nit", ""), key="hf_nit")
            f_inv   = st.text_input("Prefijo factura (INVOICE_IDENTIFIER_PREFIX)", value=current.get("invoice_identifier_prefix", ""), key="hf_inv")
            f_url   = st.text_input("SIHOS base URL", value=current.get("sihos_base_url", ""), key="hf_url")
            f_code  = st.text_input("SIHOS doc code", value=current.get("sihos_invoice_doc_code", ""), key="hf_code")
            f_ds    = st.text_area(
                "DOCUMENT_STANDARDS (JSON)",
                value=current.get("document_standards", "{}"),
                height=140,
                key="hf_ds",
            )
            if st.form_submit_button("Guardar hospital", type="primary"):
                try:
                    ds = json.loads(f_ds)
                except json.JSONDecodeError as exc:
                    st.error("JSON inválido: %s" % exc)
                else:
                    repo.upsert_hospital(f_key, {
                        "display_name":              f_name,
                        "NIT":                       f_nit,
                        "INVOICE_IDENTIFIER_PREFIX": f_inv,
                        "SIHOS_BASE_URL":            f_url,
                        "SIHOS_INVOICE_DOC_CODE":    f_code,
                        "DOCUMENT_STANDARDS":        ds,
                    })
                    st.success("Hospital '%s' guardado." % f_key)
                    st.rerun()

    # ── Admin/contract mappings ───────────────────────────────────────────────

    if sel_hosp:
        st.markdown("<br>", unsafe_allow_html=True)
        section_header("Mappings admin/contrato — %s" % sel_hosp)

        mappings = repo.fetch_admin_contract_mappings(sel_hosp)

        if mappings:
            for m in mappings:
                col_raw, col_arrow, col_can, col_del = st.columns([3, 0.5, 3, 1])
                col_raw.markdown(
                    "`%s` / `%s`" % (m["raw_admin"], m["raw_contract"] or "—")
                )
                col_arrow.markdown("→")
                col_can.markdown(
                    "`%s` / `%s`" % (m["canonical_admin"] or "—", m["canonical_contract"] or "—")
                )
                if col_del.button("✕", key="del_map_%d" % m["id"]):
                    repo.delete_admin_contract_mapping(m["id"])
                    st.rerun()
        else:
            st.caption("No hay mappings para este hospital.")

        with st.expander("Agregar mapping"):
            with st.form("map_form_%s" % sel_hosp):
                mc1, mc2 = st.columns(2)
                m_raw_a  = mc1.text_input("Administradora (raw)", key="mf_raw_a")
                m_raw_c  = mc2.text_input("Contrato (raw)", key="mf_raw_c")
                mc3, mc4 = st.columns(2)
                m_can_a  = mc3.text_input("Administradora (canónica)", key="mf_can_a")
                m_can_c  = mc4.text_input("Contrato (canónico)", key="mf_can_c")
                if st.form_submit_button("Agregar", type="primary"):
                    repo.upsert_admin_contract_mapping(
                        sel_hosp,
                        m_raw_a,
                        m_raw_c or None,
                        m_can_a or None,
                        m_can_c or None,
                    )
                    st.success("Mapping agregado.")
                    st.rerun()

    # -----------------------------------------------------------------------
    # Filename prefix fixes (global, not per-hospital)
    # -----------------------------------------------------------------------

    st.divider()
    section_header("Correcciones de prefijos de archivo")
    st.caption(
        "Estos reemplazos aplican a todos los hospitales durante la etapa de "
        "normalización de nombres. Ej: OPD → OPF corrige cualquier archivo cuyo "
        "nombre empiece con OPD_."
    )

    fixes = repo.fetch_filename_fixes()

    if fixes:
        for wrong, correct in fixes.items():
            col_w, col_arr, col_c, col_del = st.columns([2, 0.5, 2, 1])
            col_w.markdown("`%s`" % wrong)
            col_arr.markdown("→")
            col_c.markdown("`%s`" % correct)
            if col_del.button("✕", key="del_fix_%s" % wrong):
                repo.delete_filename_fix(wrong)
                st.rerun()
    else:
        st.caption("No hay correcciones registradas.")

    with st.expander("Agregar corrección"):
        with st.form("fix_form"):
            fc1, fc2 = st.columns(2)
            f_wrong   = fc1.text_input("Prefijo incorrecto", key="ff_wrong", placeholder="OPD")
            f_correct = fc2.text_input("Prefijo correcto",   key="ff_correct", placeholder="OPF")
            if st.form_submit_button("Agregar", type="primary"):
                if not f_wrong or not f_correct:
                    st.error("Ambos campos son obligatorios.")
                else:
                    repo.upsert_filename_fix(f_wrong, f_correct)
                    st.success("Corrección '%s → %s' guardada." % (f_wrong.upper(), f_correct.upper()))
                    st.rerun()
