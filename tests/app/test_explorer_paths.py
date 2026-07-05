"""Tests for explorer.py's sandbox path helper (now delegating to app.paths.safe_join)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.routers.api.explorer import _safe_resolve


class TestSafeResolve:
    def test_allows_nested_path(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        result = _safe_resolve(sandbox, "sub/dir/file.pdf")
        assert result == (sandbox / "sub" / "dir" / "file.pdf").resolve()

    def test_rejects_escaping_path(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        with pytest.raises(HTTPException) as exc_info:
            _safe_resolve(sandbox, "../../etc/passwd")
        assert exc_info.value.status_code == 400

    def test_strips_leading_separators(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        result = _safe_resolve(sandbox, "/absolute/looking/path.pdf")
        assert result == (sandbox / "absolute" / "looking" / "path.pdf").resolve()
