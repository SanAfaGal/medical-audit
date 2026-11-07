"""The page JavaScript lives in app/static/js: it must stay valid, and templates must not grow inline scripts."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[2] / "app"
TEMPLATES = sorted((APP / "templates").rglob("*.html"))
SCRIPTS = sorted((APP / "static" / "js").glob("*.js"))
_INLINE_SCRIPT = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.S)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_static_scripts_are_valid_javascript(script: Path):
    result = subprocess.run(["node", "--check", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, f"{script.name}: {result.stderr.strip()[:500]}"


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda p: p.relative_to(p.parents[1]).as_posix())
def test_templates_have_no_inline_scripts(template: Path):
    """Inline JS can't be linted or unit-tested; put it in app/static/js and load it with asset()."""
    inline = [m.group(1).strip()[:60] for m in _INLINE_SCRIPT.finditer(template.read_text(encoding="utf-8"))]
    assert inline == [], f"{template.name} has inline <script> blocks: {inline}"


def test_page_scripts_do_not_call_fetch_directly():
    """Requests go through api.* so errors are handled in one place (api.js itself and the ZIP blob are the exceptions)."""
    offenders = []
    for path in SCRIPTS + TEMPLATES:
        if path.name == "api.js":
            continue
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"\bfetch\(", line) and "download-zip" not in line:
                offenders.append(f"{path.name}:{line_no}")
    assert offenders == []


def test_asset_url_carries_the_file_version():
    from app.routers.pages import asset

    url = asset("js/api.js")
    assert re.fullmatch(r"/static/js/api\.js\?v=\d+", url)
    assert not url.endswith("?v=0")  # the file exists, so it has a real mtime
    assert asset("js/does-not-exist.js").endswith("?v=0")
