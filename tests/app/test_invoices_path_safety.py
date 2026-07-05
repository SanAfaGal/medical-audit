"""Regression tests: invoice file operations must reject folder_path values that
escape audit_data_root, and filenames containing path separators."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.models.invoice import Invoice
from app.routers.api.invoices import (
    BulkAnnulRequest,
    DeleteSurplusRequest,
    FolderAnnulDecision,
    annul_folders,
    delete_surplus_file,
)


def _fake_db_returning_invoice(invoice):
    async def _fake_get(model, pk):
        if model is Invoice:
            return invoice
        return None

    db = AsyncMock()
    db.get = AsyncMock(side_effect=_fake_get)
    return db


class TestDeleteSurplusFilePathSafety:
    async def test_rejects_folder_path_outside_audit_root(self, tmp_path):
        invoice = SimpleNamespace(id=1)
        db = _fake_db_returning_invoice(invoice)
        data = DeleteSurplusRequest(folder_path=str(tmp_path / "outside" / "HSL123"), filename="x.pdf")

        with patch("app.paths.audit_data_root", tmp_path / "audit_root"):
            with pytest.raises(HTTPException) as exc_info:
                await delete_surplus_file(invoice_id=1, data=data, db=db)
        assert exc_info.value.status_code == 400

    async def test_rejects_folder_path_escaping_via_dotdot(self, tmp_path):
        audit_root = tmp_path / "audit_root"
        audit_root.mkdir()
        invoice = SimpleNamespace(id=1)
        db = _fake_db_returning_invoice(invoice)
        escaping_path = str(audit_root / "a" / ".." / ".." / "evil")
        data = DeleteSurplusRequest(folder_path=escaping_path, filename="x.pdf")

        with patch("app.paths.audit_data_root", audit_root):
            with pytest.raises(HTTPException) as exc_info:
                await delete_surplus_file(invoice_id=1, data=data, db=db)
        assert exc_info.value.status_code == 400

    async def test_rejects_filename_with_path_separator(self, tmp_path):
        audit_root = tmp_path / "audit_root"
        folder = audit_root / "HSL123"
        folder.mkdir(parents=True)
        invoice = SimpleNamespace(id=1)
        db = _fake_db_returning_invoice(invoice)
        data = DeleteSurplusRequest(folder_path=str(folder), filename="../../evil.pdf")

        with patch("app.paths.audit_data_root", audit_root):
            with pytest.raises(HTTPException) as exc_info:
                await delete_surplus_file(invoice_id=1, data=data, db=db)
        assert exc_info.value.status_code == 400


class TestAnnulFoldersPathSafety:
    async def test_rejects_decision_with_escaping_folder_path(self, tmp_path):
        audit_root = tmp_path / "audit_root"
        audit_root.mkdir()
        db = AsyncMock()
        result_mock = AsyncMock()
        result_mock.scalar_one_or_none = lambda: SimpleNamespace(id=99)
        db.execute = AsyncMock(return_value=result_mock)

        escaping_path = str(audit_root / ".." / "evil")
        req = BulkAnnulRequest(
            institution_id=1,
            period_id=1,
            decisions=[FolderAnnulDecision(folder_path=escaping_path, invoice_id=None, action="annul")],
        )

        with patch("app.paths.audit_data_root", audit_root):
            result = await annul_folders(req, db)

        assert result["annulled"] == 0
        assert len(result["errors"]) == 1
