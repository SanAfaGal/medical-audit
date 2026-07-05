"""split_pdf must reject malformed 'ranges' with 400, matching reorder_pages's pattern."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import fitz
import pytest
from fastapi import HTTPException

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.routers.api.explorer import preview_split, split_pdf
from app.schemas.explorer import SplitRequest


def _make_minimal_pdf(path) -> None:
    doc = fitz.open()
    doc.new_page()
    doc.save(str(path))
    doc.close()


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


class TestSplitPdfInvalidRanges:
    async def test_non_numeric_ranges_returns_400(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_minimal_pdf(sandbox / "test.pdf")

        body = SplitRequest(institution_id=1, period_id=1, path="test.pdf", ranges="abc")
        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await split_pdf(body, db)
        assert exc_info.value.status_code == 400


class TestPreviewSplit:
    async def test_no_ranges_returns_one_group_per_page(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 5)

        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            result = await preview_split(institution_id=1, period_id=1, path="test.pdf", ranges=None, db=db)
        assert result.groups == [[0], [1], [2], [3], [4]]

    async def test_valid_ranges_returns_expected_groups(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 5)

        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            result = await preview_split(institution_id=1, period_id=1, path="test.pdf", ranges="1-3, 5", db=db)
        assert result.groups == [[0, 1, 2], [4]]

    async def test_malformed_ranges_returns_400(self, tmp_path):
        institution = SimpleNamespace(id=1, name="ACME")
        period = SimpleNamespace(id=1, institution_id=1, period_label="2024-01")
        db = _fake_db(institution, period)

        sandbox = tmp_path / "ACME" / "2024-01"
        sandbox.mkdir(parents=True)
        _make_pdf(sandbox / "test.pdf", 5)

        with patch("app.routers.api.explorer.audit_data_root", tmp_path):
            with pytest.raises(HTTPException) as exc_info:
                await preview_split(institution_id=1, period_id=1, path="test.pdf", ranges="abc", db=db)
        assert exc_info.value.status_code == 400
