"""Tests for core/scanner.py — filesystem operations using tmp_path."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.scanner import DocumentScanner


@pytest.fixture
def scanner(tmp_stage: Path) -> DocumentScanner:
    return DocumentScanner(tmp_stage)


class TestFindByExtension:
    def test_finds_pdfs(self, scanner: DocumentScanner):
        result = scanner.find_by_extension("pdf")
        names = [f.name for f in result]
        assert "FEV_900123456_HSL123.pdf" in names
        assert "CRC_900123456_HSL123.pdf" in names

    def test_finds_all_pdfs_recursively(self, scanner: DocumentScanner):
        result = scanner.find_by_extension("pdf")
        # tmp_stage has 5 PDF files across 4 folders
        assert len(result) == 5

    def test_empty_extension_returns_nothing(self, scanner: DocumentScanner, tmp_path: Path):
        empty = DocumentScanner(tmp_path / "empty")
        empty.base_dir.mkdir()
        assert scanner.find_by_extension("xlsx") == []


class TestFindNonPdf:
    def test_returns_non_pdf_files(self, tmp_stage: Path):
        # add a non-PDF
        (tmp_stage / "HSL123" / "notes.txt").write_text("hello")
        scanner = DocumentScanner(tmp_stage)
        result = scanner.find_non_pdf()
        assert any(f.name == "notes.txt" for f in result)

    def test_excludes_pdfs(self, scanner: DocumentScanner):
        result = scanner.find_non_pdf()
        assert all(f.suffix.lower() != ".pdf" for f in result)

    def test_empty_dir_returns_empty_list(self, tmp_path: Path):
        d = tmp_path / "empty"
        d.mkdir()
        assert DocumentScanner(d).find_non_pdf() == []


class TestFindByPrefix:
    def test_single_prefix_string(self, scanner: DocumentScanner):
        result = scanner.find_by_prefix("FEV")
        assert all(f.name.upper().startswith("FEV") for f in result)
        assert len(result) == 4  # one FEV per folder

    def test_list_of_prefixes(self, scanner: DocumentScanner):
        result = scanner.find_by_prefix(["FEV", "CRC"])
        names = [f.name for f in result]
        assert any(n.startswith("FEV") for n in names)
        assert any(n.startswith("CRC") for n in names)

    def test_case_insensitive(self, scanner: DocumentScanner):
        upper = scanner.find_by_prefix("FEV")
        lower = scanner.find_by_prefix("fev")
        assert len(upper) == len(lower)

    def test_unknown_prefix_returns_empty(self, scanner: DocumentScanner):
        assert scanner.find_by_prefix("UNKNOWN") == []


class TestFindInFolders:
    def test_finds_files_in_named_folder(self, scanner: DocumentScanner):
        result = scanner.find_in_folders(["HSL123"])
        assert len(result) == 2  # FEV + CRC

    def test_missing_folder_skips_gracefully(self, scanner: DocumentScanner):
        result = scanner.find_in_folders(["NONEXISTENT"])
        assert result == []


class TestFindInvalidNames:
    def test_valid_files_not_returned(self, scanner: DocumentScanner, nit: str, id_prefix: str):
        result = scanner.find_invalid_names(["FEV", "CRC"], id_prefix, nit)
        valid_names = {
            "FEV_900123456_HSL123.pdf",
            "CRC_900123456_HSL123.pdf",
            "FEV_900123456_HSL456.pdf",
            "FEV_900123456_HSL789.pdf",
            "FEV_900123456_HSL000.pdf",
        }
        assert not any(f.name in valid_names for f in result)

    def test_invalid_prefix_flagged(self, tmp_path: Path, nit: str, id_prefix: str):
        d = tmp_path / "HSL1"
        d.mkdir()
        (d / "XX_900123456_HSL1.pdf").write_bytes(b"")  # XX is not a valid prefix
        scanner = DocumentScanner(tmp_path)
        result = scanner.find_invalid_names(["FEV", "CRC"], id_prefix, nit)
        assert any(f.name == "XX_900123456_HSL1.pdf" for f in result)

    def test_build_name_pattern_accepts_valid(self, nit: str, id_prefix: str):
        pattern = DocumentScanner._build_name_pattern(["FEV", "CRC"], id_prefix, nit)
        assert pattern.match(f"FEV_{nit}_{id_prefix}123.pdf")
        assert pattern.match(f"CRC_{nit}_{id_prefix}999.pdf")

    def test_build_name_pattern_rejects_no_digits(self, nit: str, id_prefix: str):
        pattern = DocumentScanner._build_name_pattern(["FEV"], id_prefix, nit)
        assert not pattern.match(f"FEV_{nit}_{id_prefix}.pdf")  # no digits

    def test_build_name_pattern_rejects_wrong_prefix(self, nit: str, id_prefix: str):
        pattern = DocumentScanner._build_name_pattern(["FEV"], id_prefix, nit)
        assert not pattern.match(f"XX_{nit}_{id_prefix}1.pdf")
