"""Tests for core/ops.py — file and folder manipulation."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.core.ops import DocumentOps


@pytest.fixture
def ops(tmp_path: Path, id_prefix: str) -> DocumentOps:
    return DocumentOps(tmp_path, id_prefix)


class TestStandardizeDirNames:
    def test_renames_dir_with_extra_text(self, tmp_path: Path, id_prefix: str):
        d = tmp_path / "HSL123 EXTRA TEXT"
        d.mkdir()
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.standardize_dir_names([d])
        assert count == 1
        assert (tmp_path / "HSL123").is_dir()
        assert not d.exists()

    def test_already_canonical_is_skipped(self, tmp_path: Path, id_prefix: str):
        d = tmp_path / "HSL123"
        d.mkdir()
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.standardize_dir_names([d])
        assert count == 0
        assert d.exists()

    def test_target_exists_skips_rename(self, tmp_path: Path, id_prefix: str):
        original = tmp_path / "HSL123 EXTRA"
        canonical = tmp_path / "HSL123"
        original.mkdir()
        canonical.mkdir()  # target already exists
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.standardize_dir_names([original])
        assert count == 0
        assert original.exists()  # untouched

    def test_no_id_in_name_skips(self, tmp_path: Path, id_prefix: str):
        d = tmp_path / "NOID_FOLDER"
        d.mkdir()
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.standardize_dir_names([d])
        assert count == 0


class TestTagDirsMissingCufe:
    def test_appends_cufe_suffix(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        pdf = folder / "FEV_900_HSL123.pdf"
        pdf.write_bytes(b"")
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.tag_dirs_missing_cufe([pdf])
        assert count == 1
        assert (tmp_path / "HSL123 CUFE").is_dir()

    def test_already_tagged_is_skipped(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123 CUFE"
        folder.mkdir()
        pdf = folder / "FEV_900_HSL123.pdf"
        pdf.write_bytes(b"")
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.tag_dirs_missing_cufe([pdf])
        assert count == 0
        assert folder.exists()

    def test_target_already_exists_skips(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        (tmp_path / "HSL123 CUFE").mkdir()  # target already there
        pdf = folder / "FEV_900_HSL123.pdf"
        pdf.write_bytes(b"")
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.tag_dirs_missing_cufe([pdf])
        assert count == 0

    def test_deduplicates_parent_folder(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        pdf1 = folder / "FEV_900_HSL123.pdf"
        pdf2 = folder / "CRC_900_HSL123.pdf"
        pdf1.write_bytes(b"")
        pdf2.write_bytes(b"")
        ops = DocumentOps(tmp_path, id_prefix)
        count = ops.tag_dirs_missing_cufe([pdf1, pdf2])
        assert count == 1  # only one rename even for two files in same folder
