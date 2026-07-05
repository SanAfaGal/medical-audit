"""Tests for core/inspector.py — folder auditing logic."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.inspector import FolderInspector


@pytest.fixture
def inspector(tmp_stage: Path, id_prefix: str) -> FolderInspector:
    return FolderInspector(tmp_stage, id_prefix)


class TestFindMalformedDirs:
    def test_canonical_dirs_not_returned(self, inspector: FolderInspector):
        malformed = inspector.find_malformed_dirs()
        names = [d.name for d in malformed]
        assert "HSL123" not in names
        assert "HSL456" not in names

    def test_extra_text_dirs_returned(self, inspector: FolderInspector):
        malformed = inspector.find_malformed_dirs()
        names = [d.name for d in malformed]
        assert "HSL789 EXTRA" in names
        assert "HSL000 CUFE" in names

    def test_skip_list_excludes_dirs(self, inspector: FolderInspector, tmp_stage: Path):
        skip = [tmp_stage / "HSL789 EXTRA"]
        malformed = inspector.find_malformed_dirs(skip=skip)
        names = [d.name for d in malformed]
        assert "HSL789 EXTRA" not in names


class TestFindMissingDirs:
    # DB stores only the numeric portion (e.g. "123"), not the full folder name
    # ("HSL123"). find_missing_dirs must handle the prefix transparently.

    def test_existing_dir_not_missing(self, inspector: FolderInspector):
        missing = inspector.find_missing_dirs(["123"])
        assert "123" not in missing

    def test_absent_dir_is_missing(self, inspector: FolderInspector):
        missing = inspector.find_missing_dirs(["123", "999"])
        assert "999" in missing
        assert "123" not in missing

    def test_empty_expected_returns_empty(self, inspector: FolderInspector):
        assert inspector.find_missing_dirs([]) == []


class TestFindVoidDirs:
    def test_no_void_dirs_in_default_tree(self, inspector: FolderInspector):
        # Default tmp_stage has no "ANULAR" folder
        void = inspector.find_void_dirs()
        assert void == []

    def test_detects_anular_folder(self, tmp_stage: Path, id_prefix: str):
        (tmp_stage / "HSL999 ANULAR").mkdir()
        inspector = FolderInspector(tmp_stage, id_prefix)
        void = inspector.find_void_dirs()
        assert any("ANULAR" in d.name.upper() for d in void)


class TestFindMismatchedFiles:
    def test_matching_file_not_flagged(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        (folder / "FEV_900_HSL123.pdf").write_bytes(b"")
        inspector = FolderInspector(tmp_path, id_prefix)
        assert inspector.find_mismatched_files() == []

    def test_mismatched_file_flagged(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        (folder / "FEV_900_HSL456.pdf").write_bytes(b"")  # HSL456 ≠ HSL123
        inspector = FolderInspector(tmp_path, id_prefix)
        result = inspector.find_mismatched_files()
        assert any(f.name == "FEV_900_HSL456.pdf" for f in result)


class TestCheckRequiredDocs:
    def test_present_doc_is_not_missing(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL1"
        folder.mkdir()
        (folder / "CRC_900_HSL1.pdf").write_bytes(b"")
        inspector = FolderInspector(tmp_path, id_prefix)
        missing = inspector.check_required_docs(folder, {"FIRMA": ["CRC"]})
        assert "FIRMA" not in missing

    def test_absent_doc_is_missing(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL1"
        folder.mkdir()
        # no CRC file
        inspector = FolderInspector(tmp_path, id_prefix)
        missing = inspector.check_required_docs(folder, {"FIRMA": ["CRC"]})
        assert "FIRMA" in missing

    def test_empty_prefix_list_is_skipped(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL1"
        folder.mkdir()
        inspector = FolderInspector(tmp_path, id_prefix)
        # prefix list is empty → content-check, not file-presence check
        missing = inspector.check_required_docs(folder, {"CONTENT_CHECK": []})
        assert missing == []

    def test_nonexistent_folder_all_missing(self, tmp_path: Path, id_prefix: str):
        inspector = FolderInspector(tmp_path, id_prefix)
        missing = inspector.check_required_docs(tmp_path / "NOPE", {"FIRMA": ["CRC"], "HCU": ["EPI"]})
        assert set(missing) == {"FIRMA", "HCU"}

    def test_case_insensitive_prefix_match(self, tmp_path: Path, id_prefix: str):
        folder = tmp_path / "HSL1"
        folder.mkdir()
        (folder / "crc_900_HSL1.PDF").write_bytes(b"")  # lowercase name
        inspector = FolderInspector(tmp_path, id_prefix)
        missing = inspector.check_required_docs(folder, {"FIRMA": ["CRC"]})
        assert "FIRMA" not in missing


class TestResolveDirPaths:
    def test_returns_existing_dirs(self, tmp_stage: Path, id_prefix: str):
        inspector = FolderInspector(tmp_stage, id_prefix)
        result = inspector.resolve_dir_paths(["HSL123", "HSL456"])
        names = {d.name for d in result}
        assert "HSL123" in names
        assert "HSL456" in names

    def test_skips_nonexistent_names(self, tmp_stage: Path, id_prefix: str):
        inspector = FolderInspector(tmp_stage, id_prefix)
        result = inspector.resolve_dir_paths(["HSL123", "GHOST"])
        names = {d.name for d in result}
        assert "GHOST" not in names
