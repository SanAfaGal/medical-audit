"""Regression tests: uploads must reject files above a size cap instead of
reading unbounded content into memory/DB."""

from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

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


class TestUploadFilesSizeLimit:
    async def test_oversized_file_is_skipped_not_written(self, tmp_path):
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

        oversized = b"%PDF-1.4" + b"x" * (200 * 1024 * 1024 + 1)
        upload = UploadFile(file=io.BytesIO(oversized), filename="huge.pdf")

        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            result = await upload_files(
                institution_id=1,
                period_id=1,
                path="",
                relative_paths_json="[]",
                files=[upload],
                db=db,
            )

        assert result.uploaded == []
        assert any("huge.pdf" in s for s in result.skipped)
        assert not (sandbox / "huge.pdf").exists()
