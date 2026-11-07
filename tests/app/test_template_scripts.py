"""Syntax check of the JavaScript embedded in the Jinja templates (until it moves to static/js)."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

TEMPLATES = sorted((Path(__file__).resolve().parents[2] / "app" / "templates").rglob("*.html"))
_INLINE_SCRIPT = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.S)

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")


def _inline_scripts(path: Path) -> list[str]:
    scripts = []
    for match in _INLINE_SCRIPT.finditer(path.read_text(encoding="utf-8")):
        body = re.sub(r"\{%.*?%\}|\{#.*?#\}", "", match.group(1), flags=re.S)  # Jinja statements and comments
        scripts.append(re.sub(r"\{\{.*?\}\}", "0", body, flags=re.S))  # Jinja expressions become a literal
    return scripts


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda p: p.relative_to(p.parents[1]).as_posix())
def test_inline_scripts_are_valid_javascript(template: Path, tmp_path: Path):
    for index, script in enumerate(_inline_scripts(template)):
        js = tmp_path / f"script{index}.js"
        js.write_text(script, encoding="utf-8")
        result = subprocess.run(["node", "--check", str(js)], capture_output=True, text=True)
        assert result.returncode == 0, f"{template.name} script #{index}: {result.stderr.strip()[:500]}"


def test_no_template_calls_fetch_directly_except_the_zip_download():
    """Requests go through api.* so errors are handled in one place (see static/js/api.js)."""
    offenders = []
    for template in TEMPLATES:
        for line_no, line in enumerate(template.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"\bfetch\(", line) and "download-zip" not in line:
                offenders.append(f"{template.name}:{line_no}")
    assert offenders == []
