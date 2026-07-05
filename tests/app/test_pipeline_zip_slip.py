"""Regression test: load_drive_zip must reject ZIP members that escape drive_path (Zip Slip)."""

from __future__ import annotations

import io
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.routers.api.pipeline import load_drive_zip


def _malicious_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a/../../../evil.txt", b"pwned")
    return buf.getvalue()


class TestLoadDriveZipSlip:
    async def test_rejects_zip_with_path_traversal_member(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, period_label="2024-01")

        async def _fake_get(model, pk):
            if model is Institution:
                return institution
            if model is AuditPeriod:
                return period
            return None

        db = AsyncMock()
        db.get = AsyncMock(side_effect=_fake_get)

        upload = UploadFile(file=io.BytesIO(_malicious_zip_bytes()), filename="evil.zip")

        with patch("app.routers.api.pipeline.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await load_drive_zip(institution_id=1, period_id=1, file=upload, db=db)

        assert exc_info.value.status_code == 400
        assert not (tmp_path / "evil.txt").exists()
