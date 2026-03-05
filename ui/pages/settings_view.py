"""Settings page: audit path, hospital configuration, Drive credentials, and SIHOS upload."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import streamlit as st

from ui.widgets import section_header

logger = logging.getLogger(__name__)


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

    # -----------------------------------------------------------------------
    # A. Directorio de auditoría (global)
    # -----------------------------------------------------------------------

    section_header("Directorio de auditoría")
    st.caption(
        "Ruta raíz donde se almacenan las carpetas de cada hospital y período "
        "(BASE, STAGE, AUDIT y reportes SIHOS). La base de datos y las "
        "credenciales de Drive se guardan aparte en `~/.medical-audit/`."
    )

    with st.form("audit_path_form"):
        new_audit_path = st.text_input(
            "Directorio de auditoría",
            value=str(Settings.audit_path),
            label_visibility="collapsed",
            placeholder="C:/Auditorias",
        )
        if st.form_submit_button("Guardar", type="primary"):
            p = Path(new_audit_path.strip())
            Settings.save_audit_path(p)
            st.success("Directorio guardado: `%s`" % p)
            st.rerun()

    # -----------------------------------------------------------------------
    # B. Configuración del hospital (basado en sel_hospital del header)
    # -----------------------------------------------------------------------

    st.divider()
    hospital = st.session_state.get("sel_hospital")

    if not hospital:
        st.info("Selecciona un hospital desde el encabezado para ver su configuración.")
        _render_global_sections(repo)
        return

    section_header("Hospital — %s" % hospital)

    hospitals = repo.fetch_all_hospitals()
    current = next((h for h in hospitals if h["key"] == hospital), {})

    # ── Hospital config form ─────────────────────────────────────────────────
    with st.expander("Editar configuración técnica", expanded=False):
        h = hospital  # shorthand for key suffix
        with st.form("hosp_form_%s" % h):
            f_name  = st.text_input("Nombre para mostrar", value=current.get("display_name", ""), key="hf_name_%s" % h)
            f_nit   = st.text_input("NIT", value=current.get("nit", ""), key="hf_nit_%s" % h)
            f_inv   = st.text_input("Prefijo factura (INVOICE_IDENTIFIER_PREFIX)", value=current.get("invoice_identifier_prefix", ""), key="hf_inv_%s" % h)
            f_url   = st.text_input("SIHOS base URL", value=current.get("sihos_base_url", ""), key="hf_url_%s" % h)
            f_code  = st.text_input("SIHOS doc code", value=current.get("sihos_invoice_doc_code", ""), key="hf_code_%s" % h)
            f_sihos_user = st.text_input("Usuario SIHOS", value=current.get("sihos_user", ""), key="hf_sihos_user_%s" % h)
            f_sihos_pass = st.text_input("Contraseña SIHOS", value=current.get("sihos_password", ""), type="password", key="hf_sihos_pass_%s" % h)
            f_ds = st.text_area(
                "DOCUMENT_STANDARDS (JSON)",
                value=current.get("document_standards", "{}"),
                height=140,
                key="hf_ds_%s" % h,
            )
            if st.form_submit_button("Guardar configuración", type="primary"):
                try:
                    ds = json.loads(f_ds)
                except json.JSONDecodeError as exc:
                    st.error("JSON inválido: %s" % exc)
                else:
                    repo.upsert_hospital(hospital, {
                        "display_name":              f_name,
                        "NIT":                       f_nit,
                        "INVOICE_IDENTIFIER_PREFIX": f_inv,
                        "SIHOS_BASE_URL":            f_url,
                        "SIHOS_INVOICE_DOC_CODE":    f_code,
                        "sihos_user":                f_sihos_user,
                        "sihos_password":            f_sihos_pass,
                        "DOCUMENT_STANDARDS":        ds,
                    })
                    st.success("Configuración de '%s' guardada." % hospital)
                    st.rerun()

    # ── Credenciales Drive ───────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    section_header("Credenciales Drive")

    cred_path = Settings.drive_credentials_path(hospital)
    if cred_path.exists():
        st.markdown(
            "<span style='color:#16A34A;font-weight:600;'>✓ drive.json configurado</span> "
            "&nbsp; `%s`" % cred_path,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<span style='color:#DC2626;font-weight:600;'>✗ No encontrado</span> "
            "&nbsp; `%s`" % cred_path,
            unsafe_allow_html=True,
        )

    uploaded_cred = st.file_uploader(
        "Subir drive.json (service account de Google Drive)",
        type=["json"],
        key="drive_cred_upload_%s" % hospital,
    )
    if uploaded_cred is not None:
        cred_path.parent.mkdir(parents=True, exist_ok=True)
        cred_path.write_bytes(uploaded_cred.read())
        st.success("Credenciales guardadas en `%s`" % cred_path)
        st.rerun()

    # ── Nuevo período ────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    section_header("Nuevo período")
    st.caption(
        "Crea la estructura de carpetas para un período y sube el reporte SIHOS. "
        "El período aparecerá en el selector del encabezado. "
        "Formato recomendado: `22-28_MARZO`"
    )

    with st.form("new_period_form_%s" % hospital):
        period_name = st.text_input(
            "Nombre del período",
            placeholder="22-28_MARZO",
            key="new_period_name_%s" % hospital,
        )
        sihos_file = st.file_uploader(
            "Reporte SIHOS (.xlsx)",
            type=["xlsx"],
            key="sihos_upload_%s" % hospital,
        )
        if st.form_submit_button("Crear período", type="primary"):
            period_name = period_name.strip()
            if not period_name:
                st.error("Ingresa un nombre de período.")
            elif sihos_file is None:
                st.error("Sube el reporte SIHOS (.xlsx).")
            else:
                period_dir = Settings.audit_path / hospital / period_name
                for sub in ("BASE", "STAGE", "AUDIT"):
                    (period_dir / sub).mkdir(parents=True, exist_ok=True)
                sihos_dest = period_dir / ("%s_SIHOS.xlsx" % period_name)
                sihos_dest.write_bytes(sihos_file.read())
                st.success(
                    "Período `%s` creado en `%s`" % (period_name, period_dir)
                )
                st.rerun()

    _render_global_sections(repo, hospital=hospital)


def _render_global_sections(repo, hospital: str | None = None) -> None:
    """Render admin/contract mappings and filename fix sections.

    Args:
        repo: AuditRepository instance.
        hospital: Selected hospital key, or None if no hospital is selected.
    """
    # ── Admin/contract mappings ───────────────────────────────────────────────
    if hospital:
        st.divider()
        section_header("Mappings admin/contrato — %s" % hospital)

        mappings = repo.fetch_admin_contract_mappings(hospital)
        if mappings:
            for m in mappings:
                col_raw, col_arrow, col_can, col_del = st.columns([3, 0.5, 3, 1])
                col_raw.markdown("`%s` / `%s`" % (m["raw_admin"], m["raw_contract"] or "—"))
                col_arrow.markdown("→")
                col_can.markdown("`%s` / `%s`" % (m["canonical_admin"] or "—", m["canonical_contract"] or "—"))
                if col_del.button("✕", key="del_map_%d" % m["id"]):
                    repo.delete_admin_contract_mapping(m["id"])
                    st.rerun()
        else:
            st.caption("No hay mappings para este hospital.")

        with st.expander("Agregar mapping"):
            with st.form("map_form_%s" % hospital):
                mc1, mc2 = st.columns(2)
                m_raw_a = mc1.text_input("Administradora (raw)", key="mf_raw_a")
                m_raw_c = mc2.text_input("Contrato (raw)", key="mf_raw_c")
                mc3, mc4 = st.columns(2)
                m_can_a = mc3.text_input("Administradora (canónica)", key="mf_can_a")
                m_can_c = mc4.text_input("Contrato (canónico)", key="mf_can_c")
                if st.form_submit_button("Agregar", type="primary"):
                    repo.upsert_admin_contract_mapping(
                        hospital, m_raw_a, m_raw_c or None, m_can_a or None, m_can_c or None,
                    )
                    st.success("Mapping agregado.")
                    st.rerun()

    # ── Filename prefix fixes (global) ────────────────────────────────────────
    st.divider()
    section_header("Correcciones de prefijos de archivo")
    st.caption(
        "Estos reemplazos aplican a todos los hospitales durante la etapa de "
        "normalización de nombres. Ej: OPD → OPF corrige archivos cuyo nombre empiece con OPD_."
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

    # ── Gestión de hospitales ─────────────────────────────────────────────────
    st.divider()
    section_header("Gestión de hospitales")
    st.caption("Registra nuevos hospitales o edita la clave/NIT de uno existente.")

    hospitals = repo.fetch_all_hospitals()
    hosp_keys = [h["key"] for h in hospitals]

    with st.expander("Agregar hospital"):
        with st.form("new_hosp_form"):
            fn_key  = st.text_input("Clave (key)", placeholder="SANTA_LUCIA", key="nh_key")
            fn_name = st.text_input("Nombre para mostrar", key="nh_name")
            fn_nit  = st.text_input("NIT", key="nh_nit")
            if st.form_submit_button("Crear hospital", type="primary"):
                fn_key = fn_key.strip().upper()
                if not fn_key:
                    st.error("La clave es obligatoria.")
                elif fn_key in hosp_keys:
                    st.error("Ya existe un hospital con esa clave.")
                else:
                    repo.upsert_hospital(fn_key, {
                        "display_name": fn_name or fn_key,
                        "NIT":          fn_nit,
                    })
                    st.success("Hospital '%s' creado." % fn_key)
                    st.rerun()

    # ── Base de datos ──────────────────────────────────────────────────────────
    st.divider()
    section_header("Base de datos")

    from config.settings import Settings as _S
    db = Path(_S.db_path)
    if db.exists():
        size_kb = db.stat().st_size / 1024
        st.markdown("**Ruta** &nbsp; `%s`" % db, unsafe_allow_html=True)
        st.markdown("**Tamaño** &nbsp; `%.1f KB`" % size_kb, unsafe_allow_html=True)
    else:
        st.caption("Base de datos aún no creada.")

    backup_dir = Path(_S.backup_dir)
    backups = sorted(backup_dir.glob("audit_*.db")) if backup_dir.exists() else []
    if backups:
        latest = backups[-1]
        st.markdown(
            "**Último backup** &nbsp; `%s` &nbsp; `%.1f KB`" % (latest.name, latest.stat().st_size / 1024),
            unsafe_allow_html=True,
        )
        st.caption("%d backup(s) en `%s`" % (len(backups), backup_dir))
    else:
        st.caption("Sin backups. Ejecuta cualquier etapa del pipeline para crear uno.")
