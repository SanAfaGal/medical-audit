"""Settings page: audit path, hospital configuration, Drive credentials, and SIHOS upload."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import streamlit as st

from ui.theme import RED, RED_LIGHT
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

    hospital = st.session_state.get("sel_hospital")

    tab_hosp, tab_global = st.tabs(["Hospital seleccionado", "Configuración global"])

    # -----------------------------------------------------------------------
    # Tab A: Configuración específica del hospital
    # -----------------------------------------------------------------------
    with tab_hosp:
        if not hospital:
            st.info("Selecciona un hospital desde la barra lateral para ver su configuración.")
            return

        section_header(f"Hospital — {hospital}")

        hospitals = repo.fetch_all_hospitals()
        current = next((h for h in hospitals if h["key"] == hospital), {})

        # ── Hospital config form ─────────────────────────────────────────────
        with st.expander("Editar configuración técnica", expanded=False):
            hk = hospital  # shorthand for key suffix
            with st.form(f"hosp_form_{hk}"):
                f_name = st.text_input(
                    "Nombre para mostrar", value=current.get("display_name", ""), key=f"hf_name_{hk}"
                )
                f_nit = st.text_input("NIT", value=current.get("nit", ""), key=f"hf_nit_{hk}")
                f_inv = st.text_input(
                    "Prefijo factura (INVOICE_IDENTIFIER_PREFIX)",
                    value=current.get("invoice_identifier_prefix", ""),
                    key=f"hf_inv_{hk}",
                )
                f_url = st.text_input(
                    "SIHOS base URL", value=current.get("sihos_base_url", ""), key=f"hf_url_{hk}"
                )
                f_code = st.text_input(
                    "SIHOS doc code",
                    value=current.get("sihos_invoice_doc_code", ""),
                    key=f"hf_code_{hk}",
                )
                f_sihos_user = st.text_input(
                    "Usuario SIHOS", value=current.get("sihos_user", ""), key=f"hf_sihos_user_{hk}"
                )
                f_sihos_pass = st.text_input(
                    "Contraseña SIHOS",
                    value=current.get("sihos_password", ""),
                    type="password",
                    key=f"hf_sihos_pass_{hk}",
                )
                if st.form_submit_button("Guardar configuración", type="primary"):
                    repo.upsert_hospital(hospital, {
                        "display_name":              f_name,
                        "NIT":                       f_nit,
                        "INVOICE_IDENTIFIER_PREFIX": f_inv,
                        "SIHOS_BASE_URL":            f_url,
                        "SIHOS_INVOICE_DOC_CODE":    f_code,
                        "sihos_user":                f_sihos_user,
                        "sihos_password":            f_sihos_pass,
                    })
                    st.success(f"Configuración de '{hospital}' guardada.")
                    st.rerun()

        # ── Credenciales Drive ───────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        section_header("Credenciales Drive")

        cred_path = Settings.drive_credentials_path(hospital)
        if cred_path.exists():
            st.markdown(
                "<span style='color:#16A34A;font-weight:600;'>✓ drive.json configurado</span> "
                f"&nbsp; `{cred_path}`",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<span style='color:#DC2626;font-weight:600;'>✗ No encontrado</span> "
                f"&nbsp; `{cred_path}`",
                unsafe_allow_html=True,
            )

        uploaded_cred = st.file_uploader(
            "Subir drive.json (service account de Google Drive)",
            type=["json"],
            key=f"drive_cred_upload_{hospital}",
        )
        if uploaded_cred is not None:
            cred_path.parent.mkdir(parents=True, exist_ok=True)
            cred_path.write_bytes(uploaded_cred.read())
            st.success(f"Credenciales guardadas en `{cred_path}`")
            st.rerun()

        # ── Nuevo período ────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        section_header("Nuevo período")
        st.caption(
            "Crea la estructura de carpetas para un período y sube el reporte SIHOS. "
            "El período aparecerá en el selector de la barra lateral. "
            "Formato recomendado: `22-28_MARZO`"
        )

        with st.form(f"new_period_form_{hospital}"):
            period_name = st.text_input(
                "Nombre del período",
                placeholder="22-28_MARZO",
                key=f"new_period_name_{hospital}",
            )
            sihos_file = st.file_uploader(
                "Reporte SIHOS (.xlsx)",
                type=["xlsx"],
                key=f"sihos_upload_{hospital}",
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
                    sihos_dest = period_dir / (f"{period_name}_SIHOS.xlsx")
                    sihos_dest.write_bytes(sihos_file.read())
                    st.success(f"Período `{period_name}` creado en `{period_dir}`")
                    st.rerun()

        # ── Mappings admin/contrato ──────────────────────────────────────────
        _render_mappings_section(repo, hospital)

        # ── Zona de peligro: eliminar período ───────────────────────────────
        period = st.session_state.get("sel_period")
        if period:
            _render_delete_period_section(repo, hospital, period)

    # -----------------------------------------------------------------------
    # Tab B: Configuración global
    # -----------------------------------------------------------------------
    with tab_global:
        _render_global_sections(repo)


def _render_delete_period_section(repo, hospital: str, period: str) -> None:
    """Render the danger-zone block for deleting all DB rows of a period.

    Uses a two-step confirmation pattern via session state to prevent
    accidental deletion. The confirmation is automatically invalidated if the
    user switches hospital or period between the two steps.

    Args:
        repo: AuditRepository instance.
        hospital: Currently selected hospital key.
        period: Currently selected period string.
    """
    _confirm_key = "_confirm_delete_period"

    pending = st.session_state.get(_confirm_key)

    # Stale confirmation — user switched hospital/period
    if pending and pending != (hospital, period):
        del st.session_state[_confirm_key]
        pending = None

    st.divider()
    st.markdown(
        f'<span style="font-size:.7rem;font-weight:700;letter-spacing:.08em;'
        f'color:{RED};text-transform:uppercase;">⚠ Zona de peligro</span>',
        unsafe_allow_html=True,
    )

    if not pending:
        st.caption(
            f"Elimina todas las facturas y hallazgos de **{hospital} / {period}** de la base de datos. "
            "Las carpetas físicas no se modifican. Esta acción no se puede deshacer."
        )
        if st.button("Eliminar período…", key="danger_delete_period", type="secondary"):
            st.session_state[_confirm_key] = (hospital, period)
            st.rerun()
    else:
        invoice_count = len(repo.fetch_invoice_ids(hospital, period))
        st.warning(
            f"Se borrarán **{invoice_count} factura(s)** y todos sus hallazgos de "
            f"**{hospital} / {period}**. Esta acción no se puede deshacer.",
            icon=None,
        )
        col_cancel, col_confirm = st.columns(2)
        if col_cancel.button("Cancelar", key="danger_cancel", width="stretch"):
            del st.session_state[_confirm_key]
            st.rerun()
        if col_confirm.button(
            f"Sí, eliminar {invoice_count} factura(s)",
            key="danger_confirm",
            type="primary",
            width="stretch",
        ):
            deleted = repo.delete_period(hospital, period)
            del st.session_state[_confirm_key]
            logger.info(
                "Period deleted via UI: hospital=%s period=%s rows=%d", hospital, period, deleted
            )
            st.success(f"Período **{hospital} / {period}** eliminado — {deleted} factura(s) borradas.")
            st.rerun()


def _render_mappings_section(repo, hospital: str) -> None:
    """Render admin/contract mappings for a specific hospital.

    Args:
        repo: AuditRepository instance.
        hospital: Selected hospital key.
    """
    st.divider()
    section_header(f"Mappings admin/contrato — {hospital}")

    mappings = repo.fetch_admin_contract_mappings(hospital)
    if mappings:
        pending = [m for m in mappings if not m["canonical_admin"]]
        mapped  = [m for m in mappings if m["canonical_admin"]]

        if pending:
            st.caption("Pares sin mapear — completa los campos canónicos:")
            for m in pending:
                with st.form(f"edit_map_{m['id']}"):
                    ec1, ec2 = st.columns(2)
                    ec1.markdown("**Raw:** `{}` / `{}`".format(m["raw_admin"], m["raw_contract"] or "—"))
                    ec3, ec4 = st.columns(2)
                    new_can_a = ec3.text_input("Administradora (canónica)", key=f"can_a_{m['id']}")
                    new_can_c = ec4.text_input("Contrato (canónico)", key=f"can_c_{m['id']}")
                    sb1, sb2 = st.columns([3, 1])
                    if sb1.form_submit_button("Guardar", type="primary"):
                        repo.upsert_admin_contract_mapping(
                            hospital,
                            m["raw_admin"],
                            m["raw_contract"],
                            new_can_a or None,
                            new_can_c or None,
                        )
                        st.rerun()
                    if sb2.form_submit_button("Eliminar"):
                        repo.delete_admin_contract_mapping(m["id"])
                        st.rerun()

        if mapped:
            if pending:
                st.caption("Pares ya mapeados:")
            for m in mapped:
                col_raw, col_arrow, col_can, col_del = st.columns([3, 0.5, 3, 1])
                col_raw.markdown("`{}` / `{}`".format(m["raw_admin"], m["raw_contract"] or "—"))
                col_arrow.markdown("→")
                can_a = m["canonical_admin"]
                can_c = m["canonical_contract"] or "—"
                col_can.markdown(f"`{can_a}` / `{can_c}`")
                if col_del.button("✕", key=f"del_map_{m['id']}"):
                    repo.delete_admin_contract_mapping(m["id"])
                    st.rerun()
    else:
        st.caption("No hay mappings para este hospital.")

    with st.expander("Agregar mapping"), st.form(f"map_form_{hospital}"):
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


def _render_global_sections(repo) -> None:
    """Render global configuration sections (filename fixes, doc standards, hospitals, DB).

    Args:
        repo: AuditRepository instance.
    """
    # ── Directorio de auditoría ───────────────────────────────────────────────
    from config.settings import Settings

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
            st.success(f"Directorio guardado: `{p}`")
            st.rerun()

    # ── Filename prefix fixes (global) ────────────────────────────────────────
    st.divider()
    section_header("Correcciones de prefijos de archivo")
    st.caption(
        "Estos reemplazos aplican a todos los hospitales durante la etapa de "
        "normalización de nombres. Ej: OPD → OPF corrige archivos cuyo nombre empiece con OPD_."
    )

    fixes = Settings.filename_fixes
    if fixes:
        for wrong, correct in fixes.items():
            col_w, col_arr, col_c, col_del = st.columns([2, 0.5, 2, 1])
            col_w.markdown(f"`{wrong}`")
            col_arr.markdown("→")
            col_c.markdown(f"`{correct}`")
            if col_del.button("✕", key=f"del_fix_{wrong}"):
                Settings.delete_filename_fix(wrong)
                st.rerun()
    else:
        st.caption("No hay correcciones registradas.")

    with st.expander("Agregar corrección"), st.form("fix_form"):
        fc1, fc2 = st.columns(2)
        f_wrong   = fc1.text_input("Prefijo incorrecto", key="ff_wrong", placeholder="OPD")
        f_correct = fc2.text_input("Prefijo correcto",   key="ff_correct", placeholder="OPF")
        if st.form_submit_button("Agregar", type="primary"):
            if not f_wrong or not f_correct:
                st.error("Ambos campos son obligatorios.")
            else:
                Settings.upsert_filename_fix(f_wrong, f_correct)
                st.success(f"Corrección '{f_wrong.upper()} → {f_correct.upper()}' guardada.")
                st.rerun()

    # ── Document standards (global) ───────────────────────────────────────────
    st.divider()
    section_header("Estándares de documentos")
    st.caption(
        "Mapeo global de tipos de documento a prefijos de archivo. "
        "Aplica a todos los hospitales. Edita el JSON y guarda."
    )
    with st.form("ds_form"):
        f_ds = st.text_area(
            "DOCUMENT_STANDARDS (JSON)",
            value=json.dumps(Settings.document_standards, indent=2),
            height=200,
            key="global_ds",
            label_visibility="collapsed",
        )
        if st.form_submit_button("Guardar estándares", type="primary"):
            try:
                ds = json.loads(f_ds)
            except json.JSONDecodeError as exc:
                st.error(f"JSON inválido: {exc}")
            else:
                Settings.save_document_standards(ds)
                st.success("Estándares guardados.")
                st.rerun()

    # ── Gestión de hospitales ─────────────────────────────────────────────────
    st.divider()
    section_header("Gestión de hospitales")
    st.caption("Registra nuevos hospitales o edita la clave/NIT de uno existente.")

    hospitals = repo.fetch_all_hospitals()
    hosp_keys = [h["key"] for h in hospitals]

    with st.expander("Agregar hospital"), st.form("new_hosp_form"):
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
                st.success(f"Hospital '{fn_key}' creado.")
                st.rerun()

    # ── Base de datos ──────────────────────────────────────────────────────────
    st.divider()
    section_header("Base de datos")

    db = Path(Settings.db_path)
    if db.exists():
        size_kb = db.stat().st_size / 1024
        st.markdown(f"**Ruta** &nbsp; `{db}`", unsafe_allow_html=True)
        st.markdown(f"**Tamaño** &nbsp; `{size_kb:.1f} KB`", unsafe_allow_html=True)
    else:
        st.caption("Base de datos aún no creada.")

    backup_dir = Path(Settings.backup_dir)
    backups = sorted(backup_dir.glob("audit_*.db")) if backup_dir.exists() else []
    if backups:
        latest = backups[-1]
        st.markdown(
            f"**Último backup** &nbsp; `{latest.name}` &nbsp; `{latest.stat().st_size / 1024:.1f} KB`",
            unsafe_allow_html=True,
        )
        st.caption(f"{len(backups)} backup(s) en `{backup_dir}`")
    else:
        st.caption("Sin backups. Ejecuta cualquier etapa del pipeline para crear uno.")

    log_file = Path(Settings.logs_dir) / "app.log"
    st.markdown("<br>", unsafe_allow_html=True)
    section_header("Logs")
    if log_file.exists():
        size_kb = log_file.stat().st_size / 1024
        st.markdown(f"**Archivo** &nbsp; `{log_file}`", unsafe_allow_html=True)
        st.markdown(f"**Tamaño** &nbsp; `{size_kb:.1f} KB`", unsafe_allow_html=True)
        st.caption("Rotación automática a los 5 MB · se conservan 5 archivos (app.log.1 … app.log.5).")
    else:
        st.caption("Aún no hay archivo de log. Se crea al iniciar la aplicación.")
