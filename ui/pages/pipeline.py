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

    section_header("Etapas del pipeline")

    # Handle clear before checkboxes are instantiated
    if st.session_state.pop("_clear_stages", False):
        for key in STAGES:
            st.session_state[f"stage_{key}"] = False

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

    # Controls row
    c_clear, _, c_run = st.columns([1.5, 4.0, 1.5])
    if c_clear.button("Limpiar selección", width="stretch"):
        st.session_state["_clear_stages"] = True
        st.rerun()

    selected = [k for k, v in flags.items() if v]

    if flags.get("DOWNLOAD_INVOICES_FROM_SIHOS"):
        st.divider()
        section_header("Números de factura a descargar")
        st.text_area(
            "Pega los números de factura (uno por línea)",
            key="invoices_to_download",
            height=140,
            placeholder="FE12345\nFE12346\n...",
        )

    st.divider()

    if selected:
        joined = ", ".join(STAGES[k].label for k in selected)
        st.markdown(
            f'<div class="run-summary"><b>{len(selected)} etapa(s) seleccionada(s):</b> {joined}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.caption("Ninguna etapa seleccionada.")

    run_col, cancel_col, _ = st.columns([1.5, 1.5, 4])
    run_btn = run_col.button(
        "Ejecutar pipeline",
        type="primary",
        disabled=not selected or bool(_pipe[_PIPE_RUNNING]),
    )
    cancel_btn = cancel_col.button("Cancelar", disabled=not _pipe[_PIPE_RUNNING])

    if cancel_btn:
        _cancel_event.set()
        st.rerun()

    # ── While pipeline is running: poll and refresh ──────────────────────────

    if _pipe[_PIPE_RUNNING]:
        st.markdown(
            '<div class="status-bar info">Ejecutando — por favor espera…</div>',
            unsafe_allow_html=True,
        )
        section_header("Salida del pipeline")
        st.code(_pipe[_PIPE_LOG] or "Iniciando…", language=None)
        time.sleep(0.4)
        st.rerun()

    # ── Show last run result if available ───────────────────────────────────

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

    # ── Launch pipeline in background thread ─────────────────────────────────

    if run_btn:
        _cancel_event.clear()
        _pipe[_PIPE_RUNNING] = True
        _pipe[_PIPE_LOG]     = ""

        _flags_snapshot    = dict(flags)
        _hospital_snapshot = st.session_state.get("sel_hospital", "")
        _period_snapshot   = st.session_state.get("sel_period", "")
        _raw_invoices      = st.session_state.get("invoices_to_download", "")
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
