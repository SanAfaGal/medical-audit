"""Tests for startup data checks (no database needed: the session is faked)."""

from __future__ import annotations

from types import SimpleNamespace

from app.services.bootstrap import REQUIRED_FOLDER_STATUSES, ensure_folder_statuses


class _FakeSession:
    def __init__(self, existing: list[str]) -> None:
        self._existing = existing
        self.added: list[str] = []
        self.commits = 0

    async def execute(self, _stmt):
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: list(self._existing)))

    def add(self, obj) -> None:
        self.added.append(obj.status)

    async def commit(self) -> None:
        self.commits += 1


class TestEnsureFolderStatuses:
    async def test_creates_all_statuses_on_an_unseeded_database(self):
        db = _FakeSession([])
        added = await ensure_folder_statuses(db)
        assert added == list(REQUIRED_FOLDER_STATUSES) == db.added
        assert db.commits == 1

    async def test_only_adds_the_missing_ones(self):
        db = _FakeSession(["PRESENTE", "PENDIENTE", "CUSTOM"])
        added = await ensure_folder_statuses(db)
        assert "PRESENTE" not in added and "PENDIENTE" not in added
        assert set(added) == set(REQUIRED_FOLDER_STATUSES) - {"PRESENTE", "PENDIENTE"}

    async def test_is_a_noop_when_everything_exists(self):
        db = _FakeSession(list(REQUIRED_FOLDER_STATUSES))
        assert await ensure_folder_statuses(db) == []
        assert db.added == [] and db.commits == 0

    def test_statuses_the_triggers_depend_on_are_required(self):
        assert {"PRESENTE", "PENDIENTE"} <= set(REQUIRED_FOLDER_STATUSES)
