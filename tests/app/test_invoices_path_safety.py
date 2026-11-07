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
    @staticmethod
    def _setup(tmp_path):
        """Sandbox <root>/HOSP/2024-01 with a STAGE/HSL1 folder, and a db that knows institution + period."""
        from app.models.institution import Institution
        from app.models.period import AuditPeriod

        root = tmp_path / "audit_root"
        folder = root / "HOSP" / "2024-01" / "STAGE" / "HSL1"
        folder.mkdir(parents=True)
        (folder / "a.pdf").write_bytes(b"x")
        institution = SimpleNamespace(id=1, name="HOSP")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")

        async def _fake_get(model, pk):
            return {Institution: institution, AuditPeriod: period}.get(model)

        db = AsyncMock()
        db.get = AsyncMock(side_effect=_fake_get)
        status = AsyncMock()
        status.scalar_one_or_none = lambda: SimpleNamespace(id=99)
        db.execute = AsyncMock(return_value=status)
        return root, folder, db

    @staticmethod
    def _req(*paths):
        return BulkAnnulRequest(
            institution_id=1,
            period_id=1,
            decisions=[FolderAnnulDecision(folder_path=p, invoice_id=None, action="annul") for p in paths],
        )

    async def test_rejects_decision_with_escaping_folder_path(self, tmp_path):
        root, _, db = self._setup(tmp_path)
        with patch("app.paths.audit_data_root", root):
            result = await annul_folders(self._req(str(root / ".." / "evil")), db)
        assert result["annulled"] == 0
        assert len(result["errors"]) == 1

    @pytest.mark.parametrize("bad", ["", ".", "STAGE", "../2023-01/STAGE/HSL1"])
    async def test_never_deletes_the_sandbox_or_a_phase_folder(self, tmp_path, bad):
        root, folder, db = self._setup(tmp_path)
        with patch("app.paths.audit_data_root", root):
            result = await annul_folders(self._req(bad), db)
        assert result["annulled"] == 0
        assert len(result["errors"]) == 1
        assert folder.is_dir()  # nothing was removed
        assert (root / "HOSP" / "2024-01").is_dir()

    async def test_annuls_a_folder_inside_the_period(self, tmp_path):
        root, folder, db = self._setup(tmp_path)
        with patch("app.paths.audit_data_root", root):
            result = await annul_folders(self._req(str(folder)), db)
        assert result["annulled"] == 1
        assert not folder.exists()
        assert (root / "HOSP" / "2024-01" / "STAGE").is_dir()  # only the invoice folder is gone

    async def test_unknown_period_is_404(self, tmp_path):
        root, _, db = self._setup(tmp_path)
        db.get = AsyncMock(return_value=None)
        with patch("app.paths.audit_data_root", root):
            with pytest.raises(HTTPException) as exc_info:
                await annul_folders(self._req("x"), db)
        assert exc_info.value.status_code == 404
