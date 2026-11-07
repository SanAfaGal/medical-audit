"""Tests for core/organizer.py — LeafFolderFinder, FolderCopier, InvoiceOrganizer."""

from __future__ import annotations

from pathlib import Path


from app.core.organizer import FolderCopier, LeafFolderFinder


# ---------------------------------------------------------------------------
# LeafFolderFinder
# ---------------------------------------------------------------------------


class TestLeafFolderFinder:
    def test_has_files_true(self, tmp_path: Path):
        d = tmp_path / "folder"
        d.mkdir()
        (d / "file.pdf").write_bytes(b"")
        assert LeafFolderFinder.has_files(d) is True

    def test_has_files_false_when_only_subdirs(self, tmp_path: Path):
        d = tmp_path / "parent"
        d.mkdir()
        (d / "child").mkdir()
        assert LeafFolderFinder.has_files(d) is False

    def test_has_files_false_on_nonexistent(self, tmp_path: Path):
        assert LeafFolderFinder.has_files(tmp_path / "ghost") is False

    def test_find_leaf_folders_deep_tree(self, tmp_drive: Path):
        finder = LeafFolderFinder()
        leaves = finder.find_leaf_folders(tmp_drive)
        names = {f.name for f in leaves}
        # HSL001 and HSL002 are leaf folders (contain files)
        assert "HSL001" in names
        assert "HSL002" in names
        # batch1 / batch2 have no direct files → not leaves
        assert "batch1" not in names
        assert "batch2" not in names

    def test_empty_root_returns_empty(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        assert LeafFolderFinder().find_leaf_folders(empty) == []


# ---------------------------------------------------------------------------
# FolderCopier
# ---------------------------------------------------------------------------


class TestFolderCopier:
    def test_moves_folder_without_prefix(self, tmp_path: Path):
        src = tmp_path / "src" / "HSL123"
        src.mkdir(parents=True)
        (src / "file.pdf").write_bytes(b"")
        target = tmp_path / "target"
        copier = FolderCopier(target)
        copier.move_folders([src], use_prefix=False)
        assert (target / "HSL123" / "file.pdf").exists()
        assert not src.exists()

    def test_moves_folder_with_prefix(self, tmp_path: Path):
        src = tmp_path / "batch1" / "HSL123"
        src.mkdir(parents=True)
        (src / "file.pdf").write_bytes(b"")
        target = tmp_path / "target"
        copier = FolderCopier(target)
        copier.move_folders([src], use_prefix=True)
        assert (target / "batch1_HSL123").is_dir()


# ---------------------------------------------------------------------------
# InvoiceOrganizer
# ---------------------------------------------------------------------------
