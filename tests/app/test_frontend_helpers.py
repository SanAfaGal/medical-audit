"""Runs the Node tests for the shared browser helpers (static/js/api.js, toast.js)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_TEST_FILE = Path(__file__).resolve().parents[1] / "js" / "shared.test.mjs"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_shared_frontend_helpers_pass_their_node_tests():
    result = subprocess.run(
        ["node", "--test", str(_TEST_FILE)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=_TEST_FILE.parents[2],
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_page_uses_the_single_toast_store():
    """Guard against re-introducing per-page toast implementations."""
    templates = Path(__file__).resolve().parents[2] / "app" / "templates"
    offenders = []
    for html in templates.rglob("*.html"):
        text = html.read_text(encoding="utf-8")
        if "partials/toasts.html" in html.name:
            continue
        if "Alpine.store('toast'," in text or "toasts: []" in text or "_toastId" in text:
            offenders.append(str(html.relative_to(templates)))
    assert offenders == []


def test_base_template_loads_the_shared_scripts_and_the_toast_partial():
    base = (Path(__file__).resolve().parents[2] / "app" / "templates" / "base.html").read_text(encoding="utf-8")
    assert "asset('js/api.js')" in base and "asset('js/toast.js')" in base
    assert 'include "partials/toasts.html"' in base
    # api.js must load before toast.js (the store formats messages with apiErrorMessage)
    assert base.index("asset('js/api.js')") < base.index("asset('js/toast.js')")
