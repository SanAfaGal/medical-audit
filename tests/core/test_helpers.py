"""Tests for core/helpers.py — pure functions, no fixtures required."""

from __future__ import annotations

from pathlib import Path


from core.helpers import (
    flatten_prefixes,
    read_lines_from_file,
    remove_accents,
    safe_move,
)


# ---------------------------------------------------------------------------
# remove_accents
# ---------------------------------------------------------------------------


class TestRemoveAccents:
    def test_accented_spanish(self):
        assert remove_accents("Ñoño") == "Nono"

    def test_all_accents(self):
        assert remove_accents("áéíóúüÁÉÍÓÚÜ") == "aeiouuAEIOUU"

    def test_already_clean(self):
        assert remove_accents("Hello World") == "Hello World"

    def test_none_returns_empty(self):
        assert remove_accents(None) == ""

    def test_int_returns_empty(self):
        assert remove_accents(42) == ""

    def test_float_nan_returns_empty(self):
        import math

        assert remove_accents(math.nan) == ""

    def test_empty_string(self):
        assert remove_accents("") == ""

    def test_numbers_in_string(self):
        assert remove_accents("Factura 123") == "Factura 123"


# ---------------------------------------------------------------------------
# flatten_prefixes
# ---------------------------------------------------------------------------


class TestFlattenPrefixes:
    def test_string_values(self):
        result = flatten_prefixes({"A": "FEV", "B": "CRC"})
        assert set(result) == {"FEV", "CRC"}

    def test_list_values(self):
        result = flatten_prefixes({"A": ["FEV", "CRC"]})
        assert set(result) == {"FEV", "CRC"}

    def test_mixed_values(self):
        result = flatten_prefixes({"A": "FEV", "B": ["CRC", "EPI"]})
        assert set(result) == {"FEV", "CRC", "EPI"}

    def test_deduplication(self):
        result = flatten_prefixes({"A": "FEV", "B": "FEV"})
        assert result == ["FEV"]

    def test_empty_dict(self):
        assert flatten_prefixes({}) == []


# ---------------------------------------------------------------------------
# read_lines_from_file
# ---------------------------------------------------------------------------


class TestReadLinesFromFile:
    def test_reads_lines(self, tmp_path: Path):
        f = tmp_path / "list.txt"
        f.write_text("HSL001\nHSL002\nHSL003\n", encoding="utf-8")
        assert read_lines_from_file(f) == ["HSL001", "HSL002", "HSL003"]

    def test_strips_blank_lines(self, tmp_path: Path):
        f = tmp_path / "list.txt"
        f.write_text("\nHSL001\n\nHSL002\n", encoding="utf-8")
        assert read_lines_from_file(f) == ["HSL001", "HSL002"]

    def test_strips_whitespace(self, tmp_path: Path):
        f = tmp_path / "list.txt"
        f.write_text("  HSL001  \n  HSL002  \n", encoding="utf-8")
        assert read_lines_from_file(f) == ["HSL001", "HSL002"]

    def test_nonexistent_file(self, tmp_path: Path):
        assert read_lines_from_file(tmp_path / "nope.txt") == []

    def test_empty_file(self, tmp_path: Path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert read_lines_from_file(f) == []


# ---------------------------------------------------------------------------
# safe_move
# ---------------------------------------------------------------------------


class TestSafeMove:
    def test_moves_file(self, tmp_path: Path):
        src = tmp_path / "a.txt"
        src.write_text("hello")
        dest = tmp_path / "sub" / "b.txt"
        assert safe_move(src, dest) is True
        assert dest.read_text() == "hello"
        assert not src.exists()

    def test_destination_already_exists_returns_false(self, tmp_path: Path):
        src = tmp_path / "a.txt"
        src.write_text("src")
        dest = tmp_path / "b.txt"
        dest.write_text("existing")
        assert safe_move(src, dest) is False
        assert dest.read_text() == "existing"  # unchanged

    def test_moves_directory(self, tmp_path: Path):
        src = tmp_path / "folder"
        src.mkdir()
        (src / "file.pdf").write_bytes(b"")
        dest = tmp_path / "moved_folder"
        assert safe_move(src, dest) is True
        assert (dest / "file.pdf").exists()
