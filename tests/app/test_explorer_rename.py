"""Rename endpoint must keep file extensions intact and reject bad names."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.routers.api.explorer import rename_entry
from app.schemas.explorer import RenameRequest


async def _rename(sandbox, path: str, new_name: str):
    body = RenameRequest(institution_id=1, period_id=1, path=path, new_name=new_name)
    with patch("app.routers.api.explorer._resolve_sandbox", AsyncMock(return_value=sandbox)):
        return await rename_entry(body, db=None)


@pytest.mark.asyncio
async def test_rename_keeps_extension(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"%PDF-1.4")
    result = await _rename(tmp_path, "a.pdf", "b.pdf")
    assert result.ok
    assert (tmp_path / "b.pdf").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("new_name", ["b", "b.txt"])
async def test_rename_rejects_extension_change(tmp_path, new_name):
    (tmp_path / "a.pdf").write_bytes(b"%PDF-1.4")
    with pytest.raises(HTTPException) as exc:
        await _rename(tmp_path, "a.pdf", new_name)
    assert exc.value.status_code == 400
    assert (tmp_path / "a.pdf").exists()


@pytest.mark.asyncio
async def test_rename_directory_without_extension_rule(tmp_path):
    (tmp_path / "carpeta").mkdir()
    result = await _rename(tmp_path, "carpeta", "otra")
    assert result.ok
    assert (tmp_path / "otra").is_dir()


@pytest.mark.asyncio
async def test_rename_rejects_path_separator(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"%PDF-1.4")
    with pytest.raises(HTTPException) as exc:
        await _rename(tmp_path, "a.pdf", "x/y.pdf")
    assert exc.value.status_code == 400
