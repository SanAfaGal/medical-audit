"""reorder_pages must support deleting pages: page_order is now a keep-list, not a full permutation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import fitz
import pytest
from fastapi import HTTPException

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.routers.api.explorer import reorder_pages
from app.schemas.explorer import ReorderRequest


def _make_pdf(path, page_count: int) -> None:
    doc = fitz.open()
    for _ in range(page_count):
        doc.new_page()
    doc.save(str(path))
    doc.close()


def _fake_db(institution, period):
    async def _fake_get(model, pk):
        if model is Institution:
            return institution
        if model is AuditPeriod:
            return period
        return None

    db = AsyncMock()
    db.get = AsyncMock(side_effect=_fake_get)
    return db


class TestReorderPagesDeletion:
    async def test_subset_reorders_and_deletes(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        pdf_path = sandbox / "test.pdf"
        _make_pdf(pdf_path, 3)

        body = ReorderRequest(institution_id=1, period_id=1, path="test.pdf", page_order=[2, 0])
        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            result = await reorder_pages(body, db)

        assert result.ok is True
        doc = fitz.open(str(pdf_path))
        assert doc.page_count == 2
        doc.close()

    async def test_empty_page_order_returns_400(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 2)

        body = ReorderRequest(institution_id=1, period_id=1, path="test.pdf", page_order=[])
        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await reorder_pages(body, db)
        assert exc_info.value.status_code == 400

    async def test_duplicate_indices_returns_400(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 2)

        body = ReorderRequest(institution_id=1, period_id=1, path="test.pdf", page_order=[0, 0])
        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await reorder_pages(body, db)
        assert exc_info.value.status_code == 400

    async def test_out_of_range_index_returns_400(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 2)

        body = ReorderRequest(institution_id=1, period_id=1, path="test.pdf", page_order=[0, 5])
        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await reorder_pages(body, db)
        assert exc_info.value.status_code == 400
