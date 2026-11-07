"""Regression tests: invoice file operations must reject folder_path values that
escape the invoice's period sandbox, and filenames containing path separators."""

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
    RenameSurplusRequest,
    annul_folders,
    delete_surplus_file,
    rename_surplus_file,
)


@pytest.fixture
def period_env(tmp_path):
    """Sandbox <root>/HOSP/2024-01 with STAGE/HSL1/{extra.pdf}, and a db that knows invoice, period and institution."""
    from app.models.institution import Institution
    from app.models.period import AuditPeriod

    root = tmp_path / "audit_root"
    folder = root / "HOSP" / "2024-01" / "STAGE" / "HSL1"
    folder.mkdir(parents=True)
    (folder / "extra.pdf").write_bytes(b"x")
    (tmp_path / "outside.pdf").write_bytes(b"secret")
    invoice = SimpleNamespace(id=1, audit_period_id=1)
    period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
    institution = SimpleNamespace(id=1, name="HOSP")

    async def _fake_get(model, pk):
        return {Invoice: invoice, AuditPeriod: period, Institution: institution}.get(model)

    db = AsyncMock()
    db.get = AsyncMock(side_effect=_fake_get)
    with patch("app.paths.audit_data_root", root):
        yield SimpleNamespace(root=root, folder=folder, db=db, outside=tmp_path / "outside.pdf")


class TestDeleteSurplusFilePathSafety:
    async def test_deletes_a_file_inside_the_invoice_folder(self, period_env):
        data = DeleteSurplusRequest(folder_path=str(period_env.folder), filename="extra.pdf")
        assert await delete_surplus_file(invoice_id=1, data=data, db=period_env.db) == {"ok": True}
        assert not (period_env.folder / "extra.pdf").exists()

    async def test_rejects_folder_path_outside_audit_root(self, period_env, tmp_path):
        data = DeleteSurplusRequest(folder_path=str(tmp_path / "outside" / "HSL123"), filename="x.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400

    async def test_rejects_folder_path_escaping_via_dotdot(self, period_env):
        data = DeleteSurplusRequest(folder_path=str(period_env.root / "a" / ".." / ".." / "evil"), filename="x.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400

    async def test_rejects_filename_with_path_separator(self, period_env):
        data = DeleteSurplusRequest(folder_path=str(period_env.folder), filename="../../evil.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400

    @pytest.mark.parametrize("bad", ["", ".", "STAGE", "../../2023-01/STAGE/HSL1"])
    async def test_rejects_the_sandbox_or_a_phase_folder(self, period_env, bad):
        # A file placed right in the period root / phase folder must not be reachable.
        (period_env.root / "HOSP" / "2024-01" / "keep.pdf").write_bytes(b"k")
        data = DeleteSurplusRequest(folder_path=bad, filename="keep.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400
        assert (period_env.root / "HOSP" / "2024-01" / "keep.pdf").exists()

    async def test_rejects_another_institutions_folder(self, period_env):
        other = period_env.root / "OTRO" / "2024-01" / "STAGE" / "HSL9"
        other.mkdir(parents=True)
        (other / "extra.pdf").write_bytes(b"x")
        data = DeleteSurplusRequest(folder_path=str(other), filename="extra.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400
        assert (other / "extra.pdf").exists()

    async def test_unknown_period_is_404(self, period_env):
        period_env.db.get = AsyncMock(
            side_effect=lambda model, pk: SimpleNamespace(id=1, audit_period_id=1) if model is Invoice else None
        )
        data = DeleteSurplusRequest(folder_path=str(period_env.folder), filename="extra.pdf")
        with pytest.raises(HTTPException) as exc_info:
            await delete_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 404


class TestRenameSurplusPathSafety:
    async def test_rejects_folder_outside_the_period(self, period_env, tmp_path):
        data = RenameSurplusRequest(folder_path=str(tmp_path), old_filename="outside.pdf", doc_type_id=1)
        with pytest.raises(HTTPException) as exc_info:
            await rename_surplus_file(invoice_id=1, data=data, db=period_env.db)
        assert exc_info.value.status_code == 400
        assert period_env.outside.exists()

    async def test_rejects_filename_with_path_separator(self, period_env):
        data = RenameSurplusRequest(folder_path=str(period_env.folder), old_filename="../x.pdf", doc_type_id=1)
        with pytest.raises(HTTPException) as exc_info:
            await rename_surplus_file(invoice_id=1, data=data, db=period_env.db)
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
