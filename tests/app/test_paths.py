"""Tests for the sandbox-safe path helper in app/paths.py (single contract: safe_resolve(base, target))."""

from __future__ import annotations

import pytest

from app.paths import safe_resolve


class TestSafeResolve:
    def test_allows_base_itself(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        assert safe_resolve(base, base) == base.resolve()

    def test_allows_absolute_path_inside_base(self, tmp_path):
        base = tmp_path / "base"
        nested = base / "sub" / "file.txt"
        base.mkdir()
        assert safe_resolve(base, nested) == nested.resolve()

    def test_rejects_absolute_path_outside_base(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_resolve(base, tmp_path / "outside" / "file.txt")

    def test_joins_nested_relative_path(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        assert safe_resolve(base, "sub/dir/file.txt") == (base / "sub" / "dir" / "file.txt").resolve()

    def test_rejects_escaping_relative_path(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_resolve(base, "../../etc/passwd")

    def test_rejects_escaping_via_nested_dotdot(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_resolve(base, "a/../../../evil.txt")

    def test_rejects_sibling_with_shared_prefix(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            safe_resolve(base, tmp_path / "base-evil" / "x")


class TestPeriodPaths:
    def test_layout_of_a_period(self, tmp_path):
        from unittest.mock import patch

        from app.paths import period_paths

        with patch("app.paths.audit_data_root", tmp_path):
            paths = period_paths("HOSP", "2024-01")

        base = tmp_path / "HOSP" / "2024-01"
        assert (paths.base, paths.drive, paths.stage, paths.audit) == (
            base,
            base / "DRIVE",
            base / "STAGE",
            base / "AUDIT",
        )


class TestIsPlainFilename:
    @pytest.mark.parametrize("name", ["a.pdf", "FEV_900_HSL1.pdf", "con espacios.pdf"])
    def test_accepts_bare_names(self, name):
        from app.paths import is_plain_filename

        assert is_plain_filename(name)

    @pytest.mark.parametrize("name", ["", "   ", "a/b.pdf", "a\\b.pdf", "..", "../x.pdf", "x..y/z"])
    def test_rejects_everything_else(self, name):
        from app.paths import is_plain_filename

        assert not is_plain_filename(name)
