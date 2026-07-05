"""Tests for period creation — folder scaffolding."""

from __future__ import annotations

from pathlib import Path


from app.routers.api.periods import _PERIOD_SUBDIRS, _create_period_dirs


class TestCreatePeriodDirs:
    def test_creates_all_subdirs(self, tmp_path: Path):
        _create_period_dirs(str(tmp_path), "2024-01")
        for name in _PERIOD_SUBDIRS:
            assert (tmp_path / "2024-01" / name).is_dir()

    def test_returns_created_paths(self, tmp_path: Path):
        paths = _create_period_dirs(str(tmp_path), "2024-01")
        assert len(paths) == len(_PERIOD_SUBDIRS)
        assert all(Path(p).is_dir() for p in paths)

    def test_idempotent_when_dirs_already_exist(self, tmp_path: Path):
        _create_period_dirs(str(tmp_path), "2024-01")
        # second call must not raise
        paths = _create_period_dirs(str(tmp_path), "2024-01")
        assert len(paths) == len(_PERIOD_SUBDIRS)

    def test_creates_nested_base_path(self, tmp_path: Path):
        deep = str(tmp_path / "deep" / "nested")
        _create_period_dirs(deep, "2024-01")
        assert (Path(deep) / "2024-01" / "DRIVE").is_dir()
