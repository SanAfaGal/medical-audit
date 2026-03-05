"""Settings page: read-only display of the active Settings values."""

from __future__ import annotations

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

    c1, c2 = st.columns(2)
    with c1:
        with st.expander("DOCUMENT_STANDARDS"):
            st.json(Settings.hospital.get("DOCUMENT_STANDARDS", {}))
    with c2:
        with st.expander("MISNAMED_FIXER_MAP"):
            st.json(Settings.hospital.get("MISNAMED_FIXER_MAP", {}))

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
            % (
                label,
                path,
                "#16A34A" if exists else "#DC2626",
                indicator,
            ),
            unsafe_allow_html=True,
        )
