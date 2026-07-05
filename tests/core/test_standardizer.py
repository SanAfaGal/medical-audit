"""Tests for core/standardizer.py — pure naming logic + filesystem renames."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.standardizer import FilenameStandardizer


@pytest.fixture
def standardizer(nit: str, id_prefix: str) -> FilenameStandardizer:
    return FilenameStandardizer(
        nit=nit,
        valid_prefixes=["FEV", "CRC"],
        suffix_const=id_prefix,
    )


# ---------------------------------------------------------------------------
# build_canonical_name — pure logic (no filesystem)
# ---------------------------------------------------------------------------


class TestBuildCanonicalName:
    def test_id_from_parent_folder(self, tmp_path: Path, standardizer: FilenameStandardizer, nit: str, id_prefix: str):
        folder = tmp_path / f"{id_prefix}456"
        folder.mkdir()
        f = folder / "fev_anything.pdf"
        f.write_bytes(b"")
        name, reason = standardizer.build_canonical_name(f)
        assert name == f"FEV_{nit}_{id_prefix}456.pdf"
        assert reason == "Ok"

    def test_id_from_filename_fallback(
        self, tmp_path: Path, standardizer: FilenameStandardizer, nit: str, id_prefix: str
    ):
        folder = tmp_path / "unknown_folder"
        folder.mkdir()
        f = folder / f"fev_900_{id_prefix}789.pdf"
        f.write_bytes(b"")
        name, reason = standardizer.build_canonical_name(f)
        assert name == f"FEV_{nit}_{id_prefix}789.pdf"

    def test_unknown_prefix_returns_none(self, tmp_path: Path, standardizer: FilenameStandardizer, id_prefix: str):
        folder = tmp_path / f"{id_prefix}1"
        folder.mkdir()
        f = folder / "XX_whatever.pdf"
        f.write_bytes(b"")
        name, reason = standardizer.build_canonical_name(f)
        assert name is None
        assert "not recognised" in reason

    def test_no_id_found_returns_none(self, tmp_path: Path, standardizer: FilenameStandardizer):
        folder = tmp_path / "no_id"
        folder.mkdir()
        f = folder / "FEV_only_prefix.pdf"
        f.write_bytes(b"")
        name, reason = standardizer.build_canonical_name(f)
        assert name is None
        assert "invoice ID" in reason

    def test_prefix_map_applied(self, tmp_path: Path, nit: str, id_prefix: str):
        standardizer = FilenameStandardizer(
            nit=nit,
            valid_prefixes=["FEV"],
            suffix_const=id_prefix,
            prefix_map={"FEVV": "FEV"},  # remap typo
        )
        folder = tmp_path / f"{id_prefix}1"
        folder.mkdir()
        f = folder / "FEVV_anything.pdf"
        f.write_bytes(b"")
        name, reason = standardizer.build_canonical_name(f)
        assert name == f"FEV_{nit}_{id_prefix}1.pdf"


# ---------------------------------------------------------------------------
# run — filesystem renames
# ---------------------------------------------------------------------------


class TestRun:
    def test_renames_invalid_file(self, tmp_path: Path, standardizer: FilenameStandardizer, nit: str, id_prefix: str):
        folder = tmp_path / f"{id_prefix}1"
        folder.mkdir()
        f = folder / "fev_wrongname.pdf"
        f.write_bytes(b"")
        results = standardizer.run([f])
        assert any(r.status == "SUCCESS" for r in results)
        assert (folder / f"FEV_{nit}_{id_prefix}1.pdf").exists()

    def test_already_named_correctly_is_skipped(
        self, tmp_path: Path, standardizer: FilenameStandardizer, nit: str, id_prefix: str
    ):
        folder = tmp_path / f"{id_prefix}1"
        folder.mkdir()
        correct = folder / f"FEV_{nit}_{id_prefix}1.pdf"
        correct.write_bytes(b"")
        results = standardizer.run([correct])
        assert results == []  # nothing to rename

    def test_rejected_when_target_exists(
        self, tmp_path: Path, standardizer: FilenameStandardizer, nit: str, id_prefix: str
    ):
        folder = tmp_path / f"{id_prefix}1"
        folder.mkdir()
        source = folder / "fev_wrongname.pdf"
        source.write_bytes(b"")
        target = folder / f"FEV_{nit}_{id_prefix}1.pdf"
        target.write_bytes(b"")  # target already exists
        results = standardizer.run([source])
        assert any(r.status == "REJECTED" for r in results)
        assert source.exists()  # untouched

    def test_non_file_path_skipped(self, tmp_path: Path, standardizer: FilenameStandardizer):
        results = standardizer.run([tmp_path / "ghost.pdf"])
        assert results == []
