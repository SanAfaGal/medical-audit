"""UI palette constants and global CSS injection for the medical-audit Streamlit app."""

from __future__ import annotations

import logging

import streamlit as st

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

NAVY        = "#0F172A"
NAVY_MID    = "#1E293B"
NAVY_LIGHT  = "#334155"
SLATE       = "#94A3B8"
SLATE_LIGHT = "#64748B"
BORDER      = "#334155"
BG          = "#0F172A"
BG_CARD     = "#1E293B"
BLUE        = "#3B82F6"
BLUE_LIGHT  = "#172554"
BLUE_BORDER = "#1D4ED8"
GREEN       = "#22C55E"
GREEN_LIGHT = "#14532D"
AMBER       = "#F59E0B"
AMBER_LIGHT = "#78350F"
RED         = "#EF4444"
RED_LIGHT   = "#7F1D1D"

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_CSS = f"""
@import url('https://fonts.googleapis.com/css2?family=Inter:ital,opsz,wght@0,14..32,300;0,14..32,400;0,14..32,500;0,14..32,600;0,14..32,700;1,14..32,400&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Base ──────────────────────────────────────────────────────────────────── */
html, body, [class*="css"] {{
    font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
    -webkit-font-smoothing: antialiased;
}}
.stApp {{
    background: {BG};
}}
.block-container {{
    padding-top: 4rem !important;
    padding-bottom: 2.5rem !important;
    max-width: 1320px;
}}

/* ── Tab bar ────────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0;
    background: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 12px;
    padding: 5px;
    margin-bottom: 1.5rem;
    box-shadow: 0 1px 4px rgba(15,23,42,.06);
}}
.stTabs [data-baseweb="tab"] {{
    border-radius: 8px;
    padding: .5rem 1.5rem;
    font-size: .83rem;
    font-weight: 500;
    color: {SLATE};
    background: transparent;
    border: none;
    transition: background .15s ease, color .15s ease, box-shadow .15s ease;
    letter-spacing: .01em;
}}
.stTabs [data-baseweb="tab"]:hover {{
    background: {NAVY_LIGHT};
    color: #E2E8F0;
}}
.stTabs [aria-selected="true"] {{
    background: {BLUE} !important;
    color: #fff !important;
    font-weight: 600;
    box-shadow: 0 2px 8px rgba(37,99,235,.3);
}}
.stTabs [data-baseweb="tab-highlight"] {{ display: none; }}
.stTabs [data-baseweb="tab-border"]    {{ display: none; }}

/* ── App header ─────────────────────────────────────────────────────────────── */
.app-header {{
    background: linear-gradient(135deg, {NAVY} 0%, {NAVY_MID} 60%, #1e3a5f 100%);
    border-radius: 14px;
    padding: 1.1rem 1.75rem;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 4px 20px rgba(15,23,42,.18), inset 0 1px 0 rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.04);
}}
.app-header .header-brand {{
    display: flex;
    align-items: center;
    gap: .75rem;
}}
.app-header .header-dot {{
    width: 8px;
    height: 8px;
    background: {BLUE};
    border-radius: 50%;
    box-shadow: 0 0 0 3px rgba(37,99,235,.25);
    flex-shrink: 0;
}}
.app-header .header-title {{
    font-size: 1rem;
    font-weight: 700;
    color: #fff;
    letter-spacing: -.015em;
    line-height: 1.2;
}}
.app-header .header-subtitle {{
    font-size: .72rem;
    color: rgba(255,255,255,.5);
    font-weight: 400;
    letter-spacing: .02em;
    margin-top: .1rem;
}}
.app-header .header-chips {{
    display: flex;
    gap: .6rem;
    align-items: center;
}}
.app-header .chip {{
    background: rgba(255,255,255,.08);
    border: 1px solid rgba(255,255,255,.12);
    border-radius: 8px;
    padding: .3rem .9rem;
    font-size: .74rem;
    color: rgba(255,255,255,.7);
    letter-spacing: .01em;
    backdrop-filter: blur(4px);
}}
.app-header .chip b {{
    color: #fff;
    font-weight: 600;
}}

/* ── Metric cards ────────────────────────────────────────────────────────────── */
.metric-card {{
    background: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 12px;
    padding: 1.1rem 1.35rem 1.2rem;
    position: relative;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
    transition: box-shadow .15s ease, transform .1s ease;
}}
.metric-card:hover {{
    box-shadow: 0 4px 16px rgba(0,0,0,.08);
    transform: translateY(-1px);
}}
.metric-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: {BLUE};
    border-radius: 12px 12px 0 0;
}}
.metric-card.green::before {{ background: {GREEN}; }}
.metric-card.amber::before {{ background: {AMBER}; }}
.metric-card.red::before   {{ background: {RED};   }}
.metric-card .mc-label {{
    font-size: .67rem;
    font-weight: 700;
    letter-spacing: .09em;
    text-transform: uppercase;
    color: {SLATE};
    margin-bottom: .45rem;
}}
.metric-card .mc-value {{
    font-size: 2.1rem;
    font-weight: 700;
    color: #F1F5F9;
    line-height: 1.1;
    font-variant-numeric: tabular-nums;
    letter-spacing: -.03em;
}}
.metric-card .mc-sub {{
    font-size: .72rem;
    color: {SLATE_LIGHT};
    margin-top: .35rem;
    font-weight: 400;
}}

/* ── Pipeline group card ────────────────────────────────────────────────────── */
.group-card {{
    background: {BG_CARD};
    border-radius: 12px;
    border: 1px solid {BORDER};
    padding: 1.1rem 1.3rem 1rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
    transition: box-shadow .15s ease, border-color .15s ease;
}}
.group-card:hover {{
    box-shadow: 0 4px 14px rgba(0,0,0,.08);
    border-color: {BLUE_BORDER};
}}
.group-title {{
    font-size: .66rem;
    font-weight: 700;
    letter-spacing: .1em;
    text-transform: uppercase;
    color: {SLATE};
    margin-bottom: .7rem;
    padding-bottom: .55rem;
    border-bottom: 1px solid {BORDER};
    display: flex;
    align-items: center;
    gap: .5rem;
}}
.group-title::before {{
    content: '';
    width: 4px;
    height: 4px;
    background: {BLUE};
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
}}

/* ── Status bar ──────────────────────────────────────────────────────────────── */
.status-bar {{
    display: flex;
    align-items: center;
    gap: .65rem;
    padding: .7rem 1.2rem;
    border-radius: 10px;
    font-size: .82rem;
    font-weight: 500;
    margin-bottom: .75rem;
    letter-spacing: .005em;
}}
.status-bar.info    {{ background: {BLUE_LIGHT};  color: #93C5FD; border: 1px solid {BLUE_BORDER}; }}
.status-bar.success {{ background: {GREEN_LIGHT}; color: #86EFAC; border: 1px solid #166534; }}
.status-bar.error   {{ background: {RED_LIGHT};   color: #FCA5A5; border: 1px solid #991B1B; }}

/* ── Run summary ─────────────────────────────────────────────────────────────── */
.run-summary {{
    font-size: .81rem;
    color: {SLATE};
    padding: .5rem 0;
    line-height: 1.6;
}}
.run-summary b {{ color: #E2E8F0; font-weight: 600; }}

/* ── Status badges ───────────────────────────────────────────────────────────── */
.badge {{
    display: inline-flex;
    align-items: center;
    gap: .35rem;
    padding: .22rem .7rem;
    border-radius: 999px;
    font-size: .69rem;
    font-weight: 600;
    letter-spacing: .03em;
    line-height: 1.6;
    white-space: nowrap;
}}
.badge-pendiente {{ background: {NAVY_LIGHT}; color: {SLATE}; }}
.badge-revisar   {{ background: {AMBER_LIGHT}; color: #FCD34D; }}
.badge-anular    {{ background: {RED_LIGHT};   color: #FCA5A5; }}
.badge-exitoso   {{ background: {GREEN_LIGHT}; color: #86EFAC; }}

/* ── Finding card ────────────────────────────────────────────────────────────── */
.finding-card {{
    background: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 10px;
    padding: .65rem 1rem;
    margin-bottom: .4rem;
    display: flex;
    align-items: center;
    gap: .8rem;
    transition: border-color .12s ease, box-shadow .12s ease;
}}
.finding-card:hover {{
    border-color: {BLUE_BORDER};
    box-shadow: 0 2px 8px rgba(37,99,235,.08);
}}
.finding-type {{
    font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', monospace;
    font-size: .73rem;
    color: #CBD5E1;
    font-weight: 500;
    flex: 1;
    letter-spacing: -.01em;
}}
.finding-note {{
    font-size: .73rem;
    color: {SLATE};
    font-style: italic;
    letter-spacing: .005em;
}}

/* ── Terminal log box ────────────────────────────────────────────────────────── */
.log-box {{
    background: {NAVY};
    color: #CBD5E1;
    font-family: 'JetBrains Mono', 'Cascadia Code', 'Fira Code', monospace;
    font-size: .74rem;
    line-height: 1.8;
    padding: 1.2rem 1.5rem;
    border-radius: 12px;
    max-height: 440px;
    overflow-y: auto;
    white-space: pre-wrap;
    border: 1px solid {NAVY_LIGHT};
    box-shadow: inset 0 2px 8px rgba(0,0,0,.2);
    scrollbar-width: thin;
    scrollbar-color: {NAVY_LIGHT} transparent;
}}
.log-box::-webkit-scrollbar       {{ width: 5px; }}
.log-box::-webkit-scrollbar-track {{ background: transparent; }}
.log-box::-webkit-scrollbar-thumb {{ background: {NAVY_LIGHT}; border-radius: 4px; }}
.log-box .log-error    {{ color: #F87171; font-weight: 600; }}
.log-box .log-warning  {{ color: #FBBF24; }}
.log-box .log-info     {{ color: #60A5FA; }}
.log-box .log-debug    {{ color: #6EE7B7; }}

/* ── Section label ───────────────────────────────────────────────────────────── */
.section-label {{
    font-size: .66rem;
    font-weight: 700;
    letter-spacing: .1em;
    text-transform: uppercase;
    color: {SLATE_LIGHT};
    margin: 1.25rem 0 .6rem;
    display: flex;
    align-items: center;
    gap: .5rem;
}}
.section-label::after {{
    content: '';
    flex: 1;
    height: 1px;
    background: {BORDER};
}}

/* ── Document editor header ─────────────────────────────────────────────────── */
.doc-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: .6rem 1rem;
    background: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 10px 10px 0 0;
    border-bottom: none;
}}
.doc-name {{
    font-weight: 600;
    font-size: .875rem;
    color: #E2E8F0;
}}
.doc-meta {{
    font-size: .69rem;
    color: {SLATE_LIGHT};
    font-family: 'JetBrains Mono', monospace;
    letter-spacing: .02em;
}}

/* ── Buttons ─────────────────────────────────────────────────────────────────── */
.stButton > button {{
    border-radius: 9px;
    font-weight: 600;
    font-size: .83rem;
    transition: box-shadow .15s ease, transform .1s ease;
    letter-spacing: .01em;
}}
.stButton > button[kind="primary"] {{
    background: {BLUE};
    border-color: {BLUE};
}}
.stButton > button[kind="primary"]:hover {{
    background: #1D4ED8;
    box-shadow: 0 4px 16px rgba(37,99,235,.35);
    transform: translateY(-1px);
}}
.stButton > button[kind="secondary"]:hover {{
    border-color: {BLUE_BORDER};
    box-shadow: 0 2px 8px rgba(0,0,0,.08);
    transform: translateY(-1px);
}}

/* ── Inputs ──────────────────────────────────────────────────────────────────── */
[data-testid="stSelectbox"] > div,
[data-testid="stTextInput"] > div > div {{
    border-radius: 9px;
}}
[data-testid="stTextInput"] input:focus,
[data-testid="stSelectbox"] [data-baseweb="select"] > div:focus-within {{
    border-color: {BLUE} !important;
    box-shadow: 0 0 0 3px rgba(37,99,235,.15) !important;
}}

/* ── Dataframe ───────────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {{
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid {BORDER};
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
}}

/* ── Divider ─────────────────────────────────────────────────────────────────── */
hr {{
    border: none;
    border-top: 1px solid {BORDER} !important;
    margin: 1.25rem 0 !important;
}}

/* ── Alerts ──────────────────────────────────────────────────────────────────── */
[data-testid="stAlert"] {{ border-radius: 10px; }}

/* ── Checkbox ────────────────────────────────────────────────────────────────── */
[data-testid="stCheckbox"] label {{
    font-size: .84rem;
    color: #CBD5E1;
}}

/* ── Spinner ─────────────────────────────────────────────────────────────────── */
[data-testid="stSpinner"] > div {{
    border-top-color: {BLUE} !important;
}}
"""


def inject_css() -> None:
    """Inject global CSS into the Streamlit page."""
    st.markdown(f"<style>{_CSS}</style>", unsafe_allow_html=True)
