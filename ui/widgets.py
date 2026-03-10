"""Shared Streamlit widget helpers for the medical-audit application."""

from __future__ import annotations

import html
import logging
import re

import streamlit as st

from ui.theme import (
    AMBER,
    AMBER_LIGHT,
    GREEN,
    GREEN_LIGHT,
    NAVY_LIGHT,
    RED,
    RED_LIGHT,
    SLATE,
    SLATE_LIGHT,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_STATUS_STYLES: dict[str, tuple[str, str]] = {
    "PENDIENTE": (NAVY_LIGHT,  "#94A3B8"),
    "REVISAR":   (AMBER_LIGHT, "#FCD34D"),
    "ANULAR":    (RED_LIGHT,   "#FCA5A5"),
    "EXITOSO":   (GREEN_LIGHT, "#86EFAC"),
}

_STATUS_DOTS: dict[str, str] = {
    "PENDIENTE": SLATE,
    "REVISAR":   AMBER,
    "ANULAR":    RED,
    "EXITOSO":   GREEN,
}

_LOG_LINE_RE = re.compile(
    r"\b(ERROR|WARNING|WARN|INFO|DEBUG)\b",
    re.IGNORECASE,
)

_LOG_CSS_CLASS: dict[str, str] = {
    "ERROR":   "log-error",
    "WARNING": "log-warning",
    "WARN":    "log-warning",
    "INFO":    "log-info",
    "DEBUG":   "log-debug",
}


def _colorize_log_line(line: str) -> str:
    """Wrap a log line in an HTML span matching its severity.

    Args:
        line: A single log output line.

    Returns:
        HTML-escaped line wrapped in a ``<span>`` with the appropriate CSS
        class, or the plain escaped line if no severity keyword is found.
    """
    escaped = html.escape(line)
    match = _LOG_LINE_RE.search(line)
    if match:
        css_class = _LOG_CSS_CLASS.get(match.group(1).upper(), "")
        if css_class:
            return f'<span class="{css_class}">{escaped}</span>'
    return escaped


# ---------------------------------------------------------------------------
# Public widget functions
# ---------------------------------------------------------------------------


def status_badge(status: str) -> str:
    """Return an HTML badge ``<span>`` for an audit finding status.

    Args:
        status: One of ``PENDIENTE``, ``REVISAR``, ``ANULAR``, ``EXITOSO``.
            Unrecognised values fall back to the ``PENDIENTE`` style.

    Returns:
        HTML string containing the styled badge element.
    """
    key = status.upper() if status else "PENDIENTE"
    bg, color = _STATUS_STYLES.get(key, _STATUS_STYLES["PENDIENTE"])
    dot = _STATUS_DOTS.get(key, SLATE)
    label = html.escape(key.capitalize())
    return (
        f'<span class="badge" style="background:{bg};color:{color};">'
        f'<span style="width:6px;height:6px;border-radius:50%;'
        f'background:{dot};display:inline-block;flex-shrink:0;"></span>'
        f"{label}</span>"
    )


def section_header(text: str) -> None:
    """Render an uppercase section label with a trailing horizontal rule.

    Args:
        text: Section heading text (will be rendered as-is, no escaping).
    """
    st.markdown(
        f'<div class="section-label">{html.escape(text)}</div>',
        unsafe_allow_html=True,
    )


def metric_card(
    label: str,
    value: int | str,
    subtitle: str = "",
    color: str = "",
) -> str:
    """Build an HTML metric card element.

    Args:
        label: Short uppercase label displayed above the value.
        value: Primary numeric or text value to display prominently.
        subtitle: Optional secondary line shown below the value.
        color: Accent colour variant — ``"green"``, ``"amber"``, or ``"red"``.
            Defaults to the blue accent strip.

    Returns:
        HTML string for the metric card ``<div>``.
    """
    color_class = f" {color}" if color in ("green", "amber", "red") else ""
    sub_html = (
        f'<div class="mc-sub">{html.escape(str(subtitle))}</div>'
        if subtitle
        else ""
    )
    return (
        f'<div class="metric-card{color_class}">'
        f'<div class="mc-label">{html.escape(label)}</div>'
        f'<div class="mc-value">{html.escape(str(value))}</div>'
        f"{sub_html}"
        f"</div>"
    )


def log_viewer(log_text: str) -> None:
    """Render a terminal-style colourised log box.

    Each line is HTML-escaped and then wrapped in a severity-coloured
    ``<span>`` based on the first ``ERROR``/``WARNING``/``INFO``/``DEBUG``
    keyword found on the line.

    Args:
        log_text: Multi-line log string to display.
    """
    if not log_text:
        st.markdown(
            '<div class="log-box" style="color:#475569;font-style:italic;">'
            "No log output yet.</div>",
            unsafe_allow_html=True,
        )
        return

    lines = log_text.splitlines()
    colourised = "\n".join(_colorize_log_line(ln) for ln in lines)
    st.markdown(
        f'<div class="log-box">{colourised}</div>',
        unsafe_allow_html=True,
    )


def config_error_banner(error: str) -> None:
    """Display a full-width configuration error alert and halt rendering.

    Renders an error ``st.alert`` with a formatted message describing the
    configuration failure, then calls ``st.stop()`` to prevent further page
    rendering.

    Args:
        error: Human-readable description of the configuration error.
    """
    st.error(
        f"**Configuration error** — the application cannot start.\n\n{error}\n\n"
        "Check your `.env` file and `config/keys/` directory, then reload the page.",
        icon=None,
    )
    logger.error("Configuration error prevented app startup: %s", error)
    st.stop()


def page_header(period_map: dict[str, list[str]]) -> None:
    """Render the sidebar with brand identity and hospital/period selectors.

    The selected values are stored in ``st.session_state["sel_hospital"]``
    and ``st.session_state["sel_period"]`` so all pages can read them.

    Args:
        period_map: Mapping of hospital key → list of available period strings,
            as returned by ``AuditRepository.fetch_hospitals_and_periods()``.
    """
    with st.sidebar:
        st.markdown(
            """
            <div style="display:flex;align-items:center;gap:.6rem;padding:.5rem 0 1rem;">
                <div style="width:9px;height:9px;border-radius:50%;background:#3B82F6;
                            box-shadow:0 0 0 3px rgba(59,130,246,.25);flex-shrink:0;"></div>
                <div>
                    <div style="font-size:.95rem;font-weight:700;color:#fff;letter-spacing:-.015em;
                                line-height:1.2;">Auditoría Médica</div>
                    <div style="font-size:.68rem;color:rgba(255,255,255,.45);margin-top:.1rem;
                                letter-spacing:.02em;">Gestión documental y validación de facturas</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.divider()
        hospital_options = sorted(period_map.keys()) if period_map else []
        hospital = st.selectbox(
            "Hospital",
            options=hospital_options,
            key="sel_hospital",
            placeholder="— seleccionar hospital —",
        )
        period_options = period_map.get(hospital, []) if hospital else []
        st.selectbox(
            "Período",
            options=period_options,
            key="sel_period",
            placeholder="— seleccionar período —",
        )
        st.divider()
        st.text_area("Facturas seleccionadas",
            key="shared_invoices",
            height=240,
            placeholder="FE12345\nFE12346\n...",
        )


def run_summary(label: str, items: list[str], collapsed: bool = True) -> None:
    """Render a collapsible list of result items under a labelled expander.

    Args:
        label: Expander header text.
        items: List of string items to display as a bulleted list.
        collapsed: Whether the expander starts collapsed. Defaults to ``True``.
    """
    with st.expander(label, expanded=not collapsed):
        if items:
            for item in items:
                st.markdown(f"- `{item}`")
        else:
            st.markdown(
                f'<span style="color:{SLATE_LIGHT};font-size:.82rem;">No items.</span>',
                unsafe_allow_html=True,
            )


def finding_chip(label: str) -> str:
    """Return an HTML chip for a single finding label.

    Args:
        label: Human-readable finding label to display inside the chip.

    Returns:
        HTML string for the chip ``<span>``.
    """
    return (
        '<span style="display:inline-flex;align-items:center;gap:6px;'
        "background:#3D1F0A;color:#FED7AA;border:1px solid #92400E;"
        'border-radius:6px;padding:3px 10px;font-size:0.82rem;margin:2px;">'
        f"{html.escape(label)}</span>"
    )


def finding_row(finding_type: str, note: str = "") -> str:
    """Build an HTML finding-card row element.

    Args:
        finding_type: The finding code string (e.g. ``MISSING_CUFE``).
        note: Optional human-readable note for the finding.

    Returns:
        HTML string for the finding card ``<div>``.
    """
    note_html = (
        f'<span class="finding-note">{html.escape(note)}</span>'
        if note
        else ""
    )
    return (
        f'<div class="finding-card">'
        f'<span class="finding-type">{html.escape(finding_type)}</span>'
        f"{note_html}"
        f"</div>"
    )
