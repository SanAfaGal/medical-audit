"""UI sections for managing dynamic business-rules tables in the Settings page.

Provides three render functions (document types, invoice types, folder statuses)
that are called from ``settings_view._render_global_sections``.
"""

from __future__ import annotations

import json

import streamlit as st

from ui.widgets import section_header


# ---------------------------------------------------------------------------
# Document types
# ---------------------------------------------------------------------------


def render_document_types(repo) -> None:
    """Render the document types management section.

    Args:
        repo: AuditRepository instance.
    """
    section_header("Tipos de documento")
    st.caption(
        "Define qué archivos se esperan en cada carpeta. El prefijo de archivo "
        "se usa para buscar documentos. Vacío = sin verificación de archivo (ej. CUFE)."
    )

    doc_types = repo.fetch_document_types()

    for dt in doc_types:
        pfx_str = json.dumps(dt["prefixes"])
        with st.expander(f"{dt['label']}  —  `{dt['code']}`", expanded=False):
            with st.form(f"dt_form_{dt['code']}"):
                c1, c2 = st.columns(2)
                new_label = c1.text_input("Etiqueta", value=dt["label"], key=f"dt_label_{dt['code']}")
                new_pfx   = c2.text_input("Prefijos (JSON)", value=pfx_str, key=f"dt_pfx_{dt['code']}")
                new_active = st.checkbox("Activo", value=bool(dt["is_active"]), key=f"dt_active_{dt['code']}")
                s1, s2 = st.columns([3, 1])
                if s1.form_submit_button("Guardar", type="primary"):
                    try:
                        pfxs = json.loads(new_pfx)
                        if isinstance(pfxs, str):
                            pfxs = [pfxs]
                    except (json.JSONDecodeError, ValueError):
                        pfxs = [p.strip() for p in new_pfx.split(",") if p.strip()]
                    repo.upsert_document_type(dt["code"], new_label, pfxs, 1 if new_active else 0)
                    st.success("Guardado.")
                    st.rerun()
                if s2.form_submit_button("Eliminar"):
                    repo.delete_document_type(dt["code"])
                    st.rerun()

    with st.expander("Agregar tipo de documento"), st.form("new_dt_form"):
        c1, c2, c3 = st.columns(3)
        ndt_code  = c1.text_input("Código", placeholder="BITACORA", key="ndt_code")
        ndt_label = c2.text_input("Etiqueta", placeholder="Bitácora", key="ndt_label")
        ndt_pfx   = c3.text_input("Prefijos (JSON)", placeholder='["TAP"]', key="ndt_pfx")
        if st.form_submit_button("Agregar", type="primary"):
            ndt_code = ndt_code.strip().upper()
            if not ndt_code or not ndt_label:
                st.error("Código y etiqueta son obligatorios.")
            else:
                try:
                    pfxs = json.loads(ndt_pfx) if ndt_pfx.strip() else []
                    if isinstance(pfxs, str):
                        pfxs = [pfxs]
                except (json.JSONDecodeError, ValueError):
                    pfxs = [p.strip() for p in ndt_pfx.split(",") if p.strip()]
                repo.upsert_document_type(ndt_code, ndt_label, pfxs, 1)
                st.success(f"Tipo '{ndt_code}' creado.")
                st.rerun()


# ---------------------------------------------------------------------------
# Invoice types
# ---------------------------------------------------------------------------


def render_invoice_types(repo) -> None:
    """Render the invoice types management section.

    Args:
        repo: AuditRepository instance.
    """
    section_header("Tipos de factura")
    st.caption(
        "Define tipos de factura con palabras clave para detección automática en el PDF "
        "y documentos requeridos por carpeta. Mayor sort_order = mayor prioridad al detectar."
    )

    inv_types  = repo.fetch_invoice_types()
    doc_types  = repo.fetch_document_types()
    all_codes  = [dt["code"] for dt in doc_types]
    doc_labels = {dt["code"]: dt["label"] for dt in doc_types}

    for it in inv_types:
        kws      = it["keywords"] if isinstance(it["keywords"], list) else []
        req_docs = it["required_docs"] if isinstance(it["required_docs"], list) else []
        with st.expander(f"{it['display_name']}  —  `{it['code']}`", expanded=False):
            with st.form(f"it_form_{it['code']}"):
                c1, c2 = st.columns(2)
                new_name  = c1.text_input("Nombre", value=it["display_name"], key=f"it_name_{it['code']}")
                new_order = c2.number_input("Prioridad (sort_order)", value=it["sort_order"], step=10, key=f"it_order_{it['code']}")
                new_kws   = st.text_input(
                    "Palabras clave (separadas por coma, en minúsculas)",
                    value=", ".join(kws),
                    key=f"it_kws_{it['code']}",
                )
                new_req = st.multiselect(
                    "Documentos requeridos",
                    options=all_codes,
                    default=[d for d in req_docs if d in all_codes],
                    format_func=lambda c: doc_labels.get(c, c),
                    key=f"it_req_{it['code']}",
                )
                new_active = st.checkbox("Activo", value=bool(it["is_active"]), key=f"it_active_{it['code']}")
                s1, s2 = st.columns([3, 1])
                if s1.form_submit_button("Guardar", type="primary"):
                    kws_list = [k.strip().lower() for k in new_kws.split(",") if k.strip()]
                    repo.upsert_invoice_type(
                        it["code"], new_name, kws_list, new_req,
                        int(new_order), 1 if new_active else 0,
                    )
                    st.success("Guardado.")
                    st.rerun()
                if s2.form_submit_button("Eliminar"):
                    repo.delete_invoice_type(it["code"])
                    st.rerun()

    with st.expander("Agregar tipo de factura"), st.form("new_it_form"):
        c1, c2 = st.columns(2)
        nit_code = c1.text_input("Código", placeholder="MEDICAMENTOS", key="nit_code")
        nit_name = c2.text_input("Nombre", placeholder="Medicamentos", key="nit_name")
        if st.form_submit_button("Agregar", type="primary"):
            nit_code = nit_code.strip().upper()
            if not nit_code or not nit_name:
                st.error("Código y nombre son obligatorios.")
            else:
                repo.upsert_invoice_type(nit_code, nit_name, [], [], 0, 1)
                st.success(f"Tipo '{nit_code}' creado. Edítalo para agregar palabras clave y documentos requeridos.")
                st.rerun()


# ---------------------------------------------------------------------------
# Folder statuses
# ---------------------------------------------------------------------------


def render_folder_statuses(repo) -> None:
    """Render the folder statuses management section.

    Args:
        repo: AuditRepository instance.
    """
    section_header("Estados de carpeta")
    st.caption("Define los posibles estados de presencia física de una carpeta de factura.")

    statuses = repo.fetch_folder_statuses()

    for fs in statuses:
        with st.expander(f"{fs['label']}  —  `{fs['code']}`", expanded=False):
            with st.form(f"fs_form_{fs['code']}"):
                c1, c2 = st.columns(2)
                new_label = c1.text_input("Etiqueta", value=fs["label"], key=f"fs_label_{fs['code']}")
                new_order = c2.number_input("Orden", value=fs["sort_order"], step=1, key=f"fs_order_{fs['code']}")
                s1, s2 = st.columns([3, 1])
                if s1.form_submit_button("Guardar", type="primary"):
                    repo.upsert_folder_status(fs["code"], new_label, int(new_order))
                    st.success("Guardado.")
                    st.rerun()
                if s2.form_submit_button("Eliminar"):
                    repo.delete_folder_status(fs["code"])
                    st.rerun()

    with st.expander("Agregar estado"), st.form("new_fs_form"):
        c1, c2 = st.columns(2)
        nfs_code  = c1.text_input("Código", placeholder="INCOMPLETA", key="nfs_code")
        nfs_label = c2.text_input("Etiqueta", placeholder="Incompleta", key="nfs_label")
        if st.form_submit_button("Agregar", type="primary"):
            nfs_code = nfs_code.strip().upper()
            if not nfs_code or not nfs_label:
                st.error("Código y etiqueta son obligatorios.")
            else:
                repo.upsert_folder_status(nfs_code, nfs_label, len(statuses))
                st.success(f"Estado '{nfs_code}' creado.")
                st.rerun()
