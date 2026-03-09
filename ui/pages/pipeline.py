"""Pipeline page: stage selection UI and background execution."""

from __future__ import annotations

import logging
import time
import traceback
import threading

import streamlit as st

from ui.pages.pipeline_stages import STAGES, STAGE_GROUPS
from ui.pages.pipeline_runner import (
    _cancel_event,
    _execute_pipeline,
    _pipe,
    _PIPE_LOG,
    _PIPE_RESULTS,
    _PIPE_RUNNING,
)
from ui.widgets import log_viewer, section_header

logger = logging.getLogger(__name__)


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

    # Handle clear before checkboxes are instantiated
    if st.session_state.pop("_clear_stages", False):
        for key in STAGES:
            st.session_state[f"stage_{key}"] = False

    # Read current selections from session_state so the top action bar
    # can show the correct disabled state before the checkboxes are rendered.
    selected_top = [k for k in STAGES if st.session_state.get(f"stage_{k}", False)]
    _is_running  = bool(_pipe[_PIPE_RUNNING])

    # ── Top action bar ────────────────────────────────────────────────────────
    run_col, cancel_col, info_col = st.columns([1.5, 1.5, 7])
    run_btn = run_col.button(
        "Ejecutar pipeline",
        type="primary",
        disabled=not selected_top or _is_running,
        width="stretch",
    )
    cancel_btn = cancel_col.button(
        "Cancelar",
        disabled=not _is_running,
        width="stretch",
    )
    if _is_running:
        info_col.markdown(
            '<div class="run-summary">⏳ <b>Ejecutando…</b></div>',
            unsafe_allow_html=True,
        )
    elif selected_top:
        joined = ", ".join(STAGES[k].label for k in selected_top)
        info_col.markdown(
            f'<div class="run-summary"><b>{len(selected_top)} etapa(s):</b> {joined}</div>',
            unsafe_allow_html=True,
        )
    else:
        info_col.caption("Ninguna etapa seleccionada.")

    st.divider()

    # ── Stage checkboxes ──────────────────────────────────────────────────────
    section_header("Etapas del pipeline")

    flags: dict[str, bool] = {}
    cols = st.columns(3)

    for idx, (group_name, group_keys) in enumerate(STAGE_GROUPS):
        with cols[idx % 3]:
            st.markdown(
                f'<div class="group-card">'
                f'<div class="group-title">{group_name}</div>',
                unsafe_allow_html=True,
            )
            for key in group_keys:
                info = STAGES[key]
                flags[key] = st.checkbox(info.label, key=f"stage_{key}")
                st.markdown(
                    f'<div class="stage-desc">{info.description}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown("</div>", unsafe_allow_html=True)

    c_clear, _ = st.columns([1.5, 8])
    if c_clear.button("Limpiar selección", width="stretch"):
        st.session_state["_clear_stages"] = True
        st.rerun()

    if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
        st.info("Los números de factura a descargar se ingresan en el campo de la barra lateral.")

    if cancel_btn:
        _cancel_event.set()
        st.rerun()

    # ── Two-column layout: log on the left, inspection panel on the right ────

    log_col, results_col = st.columns([2, 1])

    with log_col:
        # While pipeline is running: poll and refresh
        if _pipe[_PIPE_RUNNING]:
            st.markdown(
                '<div class="status-bar info">Ejecutando — por favor espera…</div>',
                unsafe_allow_html=True,
            )
            section_header("Salida del pipeline")
            st.code(_pipe[_PIPE_LOG] or "Iniciando…", language=None)
            time.sleep(0.4)
            st.rerun()

        # Show last run result if available
        elif _pipe[_PIPE_LOG]:
            log_text  = str(_pipe[_PIPE_LOG])
            has_error = "ERROR" in log_text or "CRITICAL" in log_text
            cancelled = "cancelled by user" in log_text
            if cancelled:
                st.markdown(
                    '<div class="status-bar info">Pipeline cancelado.</div>',
                    unsafe_allow_html=True,
                )
            elif has_error:
                st.markdown(
                    '<div class="status-bar error">El pipeline terminó con errores. Ver log a continuación.</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="status-bar success">Pipeline completado exitosamente.</div>',
                    unsafe_allow_html=True,
                )
            section_header("Salida del pipeline")
            log_viewer(log_text)

    with results_col:
        section_header("Items para revisión manual")
        results = list(_pipe.get(_PIPE_RESULTS, []))
        if not results:
            st.caption("Ejecuta el pipeline para ver los ítems que requieren inspección manual.")
        else:
            for entry in results:
                label = entry["label"]
                items = entry["items"]
                with st.expander(f"{label}  ({len(items)})", expanded=True):
                    for item in items:
                        st.code(item, language=None)

    # ── Launch pipeline in background thread ─────────────────────────────────

    if run_btn:
        _cancel_event.clear()
        _pipe[_PIPE_RUNNING] = True
        _pipe[_PIPE_LOG]     = ""

        _flags_snapshot    = dict(flags)
        _hospital_snapshot = st.session_state.get("sel_hospital", "")
        _period_snapshot   = st.session_state.get("sel_period", "")
        _raw_invoices      = st.session_state.get("shared_invoices", "")
        _invoices_snapshot = [ln.strip() for ln in _raw_invoices.splitlines() if ln.strip()]

        def _run_thread() -> None:
            try:
                result = _execute_pipeline(
                    _flags_snapshot,
                    hospital=_hospital_snapshot,
                    period=_period_snapshot,
                    on_update=lambda text: _pipe.__setitem__(_PIPE_LOG, text),
                    invoice_numbers=_invoices_snapshot,
                )
                _pipe[_PIPE_LOG] = result
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc()
                _pipe[_PIPE_LOG] = (
                    str(_pipe[_PIPE_LOG])
                    + f"\nERROR (thread): {type(exc).__name__}: {exc}\n{tb}"
                )
            finally:
                _pipe[_PIPE_RUNNING] = False

        threading.Thread(target=_run_thread, daemon=True).start()
        st.rerun()
