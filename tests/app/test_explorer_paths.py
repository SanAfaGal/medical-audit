"""Tests for the explorer's sandbox path helper (delegates to app.paths.safe_resolve)."""

from __future__ import annotations

import pytest

from app.services.explorer import ExplorerError
from app.services.explorer.sandbox import resolve_in


class TestSafeResolve:
    def test_allows_nested_path(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        result = resolve_in(sandbox, "sub/dir/file.pdf")
        assert result == (sandbox / "sub" / "dir" / "file.pdf").resolve()

    def test_rejects_escaping_path(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        with pytest.raises(ExplorerError) as exc_info:
            resolve_in(sandbox, "../../etc/passwd")
        assert exc_info.value.status_code == 400

    def test_strips_leading_separators(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        result = resolve_in(sandbox, "/absolute/looking/path.pdf")
        assert result == (sandbox / "absolute" / "looking" / "path.pdf").resolve()
