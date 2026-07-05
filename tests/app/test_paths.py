"""Tests for the sandbox-safe path helpers in app/paths.py."""

from __future__ import annotations

import pytest

from app.paths import safe_join, safe_resolve


class TestSafeResolve:
    def test_allows_base_itself(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        assert safe_resolve(base, base) == base.resolve()

    def test_allows_nested_path(self, tmp_path):
        base = tmp_path / "base"
        nested = base / "sub" / "file.txt"
        base.mkdir()
        assert safe_resolve(nested, base) == nested.resolve()

    def test_rejects_path_outside_base(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        outside = tmp_path / "outside" / "file.txt"
        with pytest.raises(ValueError):
            safe_resolve(outside, base)


class TestSafeJoin:
    def test_joins_nested_relative_path(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        result = safe_join(base, "sub/dir/file.txt")
        assert result == (base / "sub" / "dir" / "file.txt").resolve()

    def test_rejects_escaping_relative_path(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_join(base, "../../etc/passwd")

    def test_rejects_escaping_via_nested_dotdot(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_join(base, "a/../../../evil.txt")
