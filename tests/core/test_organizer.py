"""Tests for core/organizer.py — LeafFolderFinder, FolderCopier, InvoiceOrganizer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core.organizer import FolderCopier, InvoiceOrganizer, LeafFolderFinder


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


@pytest.fixture
def organizer_setup(tmp_path: Path):
    staging = tmp_path / "STAGE"
    audit = tmp_path / "AUDIT"
    staging.mkdir()
    audit.mkdir()
    # Create invoice folders in staging
    for name in ["HSL001", "HSL002", "HSL003"]:
        f = staging / name
        f.mkdir()
        (f / f"FEV_900_{name}.pdf").write_bytes(b"")
    # DataFrame: HSL001 and HSL002 are organizable, HSL004 is missing from staging
    df = pd.DataFrame(
        {"Ruta": ["HSL001", "HSL002", "HSL004"]},
        index=["HSL001", "HSL002", "HSL004"],
    )
    return staging, audit, df


class TestInvoiceOrganizer:
    def test_dry_run_does_not_move_files(self, organizer_setup):
        staging, audit, df = organizer_setup
        organizer = InvoiceOrganizer(df=df, staging_dir=staging, archive_dir=audit)
        result = organizer.organize(dry_run=True)
        assert result.moved == 2  # HSL001 + HSL002
        assert (staging / "HSL001").exists()  # not actually moved

    def test_real_run_moves_files(self, organizer_setup):
        staging, audit, df = organizer_setup
        organizer = InvoiceOrganizer(df=df, staging_dir=staging, archive_dir=audit)
        result = organizer.organize(dry_run=False)
        assert result.moved == 2
        assert (audit / "HSL001").is_dir()
        assert (audit / "HSL002").is_dir()
        assert not (staging / "HSL001").exists()

    def test_not_in_staging_increments_not_found(self, organizer_setup):
        staging, audit, df = organizer_setup
        organizer = InvoiceOrganizer(df=df, staging_dir=staging, archive_dir=audit)
        result = organizer.organize(dry_run=False)
        assert result.not_found == 1  # HSL004

    def test_moved_ids_populated(self, organizer_setup):
        staging, audit, df = organizer_setup
        organizer = InvoiceOrganizer(df=df, staging_dir=staging, archive_dir=audit)
        result = organizer.organize(dry_run=False)
        assert "HSL001" in result.moved_ids
        assert "HSL002" in result.moved_ids
        assert "HSL004" not in result.moved_ids

    def test_find_source_suffix_match(self, tmp_path: Path):
        staging = tmp_path / "stage"
        staging.mkdir()
        (staging / "PREFIX_HSL123").mkdir()  # folder has extra prefix
        audit = tmp_path / "audit"
        audit.mkdir()
        df = pd.DataFrame({"Ruta": ["HSL123"]}, index=["HSL123"])
        organizer = InvoiceOrganizer(df=df, staging_dir=staging, archive_dir=audit)
        result = organizer.organize(dry_run=True)
        assert result.moved == 1  # found via suffix match
