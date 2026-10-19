"""List and search must show non-PDF files flagged with is_pdf=False."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.routers.api.explorer import list_directory, search_entries


@pytest.fixture
def sandbox(tmp_path):
    drive = tmp_path / "DRIVE"
    drive.mkdir()
    (drive / "a.pdf").write_bytes(b"%PDF-1.4")
    (drive / "sin_extension").write_bytes(b"%PDF-1.4")
    (drive / "nota.txt").write_text("x")
    (drive / "carpeta").mkdir()
    return tmp_path


def _patched(sandbox):
    return patch("app.routers.api.explorer._resolve_sandbox", AsyncMock(return_value=sandbox))


@pytest.mark.asyncio
async def test_list_includes_non_pdf_flagged(sandbox):
    with _patched(sandbox):
        res = await list_directory(institution_id=1, period_id=1, root="DRIVE", path="", db=None)
    by_name = {e.name: e for e in res.entries}
    assert set(by_name) == {"a.pdf", "sin_extension", "nota.txt", "carpeta"}
    assert by_name["a.pdf"].is_pdf is True
    assert by_name["sin_extension"].is_pdf is False
    assert by_name["nota.txt"].is_pdf is False
    assert by_name["carpeta"].is_dir is True


@pytest.mark.asyncio
async def test_search_includes_non_pdf_flagged(sandbox):
    with _patched(sandbox):
        res = await search_entries(institution_id=1, period_id=1, root="DRIVE", path="", q="n", db=None)
    by_name = {e.name: e for e in res.entries}
    assert by_name["sin_extension"].is_pdf is False
    assert by_name["nota.txt"].is_pdf is False
    assert "a.pdf" not in by_name
