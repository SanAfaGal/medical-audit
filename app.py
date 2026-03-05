"""Streamlit entry point for the Medical Audit application.

Run with:
    streamlit run app.py

Three tabs:
  - Pipeline : Toggle and execute audit pipeline stages.
  - Audit    : Browse invoices, manage findings, export report.
  - Settings : Read-only display of the active configuration.
"""

from __future__ import annotations

import streamlit as st

# Page config must be the very first Streamlit call.
st.set_page_config(
    page_title="Medical Audit",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

from ui.theme import inject_css
from ui.widgets import page_header
import ui.pages.pipeline      as page_pipeline
import ui.pages.audit         as page_audit
import ui.pages.settings_view as page_settings

# ── Global CSS ─────────────────────────────────────────────────────────────────
inject_css()

# ── Settings — loaded once; errors are surfaced per-page ──────────────────────
_config_error: str | None = None
_period_map: dict = {}

try:
    from config.settings import Settings
    from db.repository import AuditRepository
    _period_map = AuditRepository(Settings.db_path).fetch_hospitals_and_periods()
except (EnvironmentError, OSError, KeyError) as exc:
    _config_error = str(exc)

# ── App header ──────────────────────────────────────────────────────────────────
page_header(_period_map)

# ── Tab layout ──────────────────────────────────────────────────────────────────
t_pipeline, t_audit, t_settings = st.tabs(["Pipeline", "Audit", "Settings"])

with t_pipeline:
    page_pipeline.render(_config_error)

with t_audit:
    page_audit.render(_config_error)

with t_settings:
    page_settings.render(_config_error)
