"""Upload size rules: logos/credentials keep small caps (they live in the DB); explorer files and
archives are unlimited unless an operator opts in, and are streamed rather than held in memory."""

from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.config import settings
from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.routers.api.explorer import upload_files
from app.routers.api.institutions import upload_logo


class TestUploadLogoSizeLimit:
    async def test_rejects_logo_over_5mb(self):
        institution = SimpleNamespace(id=1, logo_bytes=None, logo_content_type=None)
        db = AsyncMock()
        db.get = AsyncMock(return_value=institution)

        oversized = b"x" * (5 * 1024 * 1024 + 1)
        upload = UploadFile(file=io.BytesIO(oversized), filename="logo.png", headers={"content-type": "image/png"})

        with pytest.raises(HTTPException) as exc_info:
            await upload_logo(institution_id=1, file=upload, db=db)
        assert exc_info.value.status_code == 413


def _explorer_fixtures(tmp_path):
    institution = SimpleNamespace(id=1, name="ACME")
    period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")

    async def _fake_get(model, pk):
        if model is Institution:
            return institution
        if model is AuditPeriod:
            return period
        return None

    db = AsyncMock()
    db.get = AsyncMock(side_effect=_fake_get)
    sandbox = tmp_path / "ACME" / "2024-01"
    sandbox.mkdir(parents=True)
    return db, sandbox


async def _upload(tmp_path, content: bytes, name: str = "big.pdf"):
    db, sandbox = _explorer_fixtures(tmp_path)
    upload = UploadFile(file=io.BytesIO(content), filename=name)
    with patch("app.routers.api.explorer.audit_data_root", tmp_path):
        result = await upload_files(
            institution_id=1, period_id=1, path="", relative_paths_json="[]", files=[upload], db=db
        )
    return result, sandbox


class TestUploadFilesSizeLimit:
    """The per-file cap is optional (MAX_UPLOAD_BYTES, 0 = unlimited): audits can be many GB."""

    async def test_no_cap_by_default_streams_large_files_to_disk(self, tmp_path):
        content = b"%PDF-1.4" + b"x" * (3 * 1024 * 1024)  # several chunks, > the old 200 MB check's chunk size
        result, sandbox = await _upload(tmp_path, content)

        assert result.skipped == []
        assert (sandbox / "big.pdf").read_bytes() == content

    async def test_optional_cap_skips_oversized_file_and_removes_the_partial(self, tmp_path, monkeypatch):
        monkeypatch.setattr(settings, "max_upload_bytes", 1000)
        result, sandbox = await _upload(tmp_path, b"%PDF-1.4" + b"x" * 5000, "huge.pdf")

        assert result.uploaded == []
        assert any("huge.pdf" in s for s in result.skipped)
        assert not (sandbox / "huge.pdf").exists()

    async def test_file_at_the_cap_is_accepted(self, tmp_path, monkeypatch):
        content = b"%PDF-1.4" + b"x" * 992
        monkeypatch.setattr(settings, "max_upload_bytes", len(content))
        result, sandbox = await _upload(tmp_path, content, "ok.pdf")

        assert result.skipped == []
        assert (sandbox / "ok.pdf").exists()
