"""Tests for core/ops.py — file and folder manipulation."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.ops import DocumentOps


@pytest.fixture
def ops(tmp_path: Path, id_prefix: str) -> DocumentOps:
    return DocumentOps(tmp_path, id_prefix)


class TestParseNitFromFilename:
    def test_extracts_nit(self):
        assert DocumentOps.parse_nit_from_filename("FEV_900123456_HSL1.pdf") == "900123456"

    def test_returns_none_if_no_underscores(self):
        assert DocumentOps.parse_nit_from_filename("FEV900123456HSL1.pdf") is None

    def test_returns_none_if_no_digits(self):
        assert DocumentOps.parse_nit_from_filename("FEV_abc_HSL1.pdf") is None


class TestRemoveFiles:
    def test_deletes_listed_files(self, tmp_path: Path, ops: DocumentOps):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_bytes(b"")
        f2.write_bytes(b"")
        count = ops.remove_files([f1, f2])
        assert count == 2
        assert not f1.exists()
        assert not f2.exists()

    def test_returns_count_of_deleted(self, tmp_path: Path, ops: DocumentOps):
        f = tmp_path / "keep.pdf"
        f.write_bytes(b"")
        count = ops.remove_files([f])
        assert count == 1

    def test_missing_file_logs_error_and_continues(self, tmp_path: Path, ops: DocumentOps):
        ghost = tmp_path / "ghost.pdf"
        real = tmp_path / "real.pdf"
        real.write_bytes(b"")
        count = ops.remove_files([ghost, real])
        assert count == 1  # ghost fails, real succeeds


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


class TestApplyPrefixRenames:
    def test_renames_prefix(self, tmp_path: Path, ops: DocumentOps):
        f = tmp_path / "OLD_900_HSL1.pdf"
        f.write_bytes(b"")
        count = ops.apply_prefix_renames({"OLD": "NEW"}, [f])
        assert count == 1
        assert (tmp_path / "NEW_900_HSL1.pdf").exists()

    def test_unmapped_prefix_skipped(self, tmp_path: Path, ops: DocumentOps):
        f = tmp_path / "FEV_900_HSL1.pdf"
        f.write_bytes(b"")
        count = ops.apply_prefix_renames({"OTHER": "NEW"}, [f])
        assert count == 0
        assert f.exists()

    def test_empty_files_returns_zero(self, ops: DocumentOps):
        assert ops.apply_prefix_renames({"A": "B"}, []) == 0
