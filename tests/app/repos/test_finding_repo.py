"""Tests for app/repositories/finding_repo.py — requires PostgreSQL."""

from __future__ import annotations

import datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.institution import Institution
from app.models.invoice import Invoice
from app.models.period import AuditPeriod
from app.models.rules import DocType, FolderStatus
from app.repositories.finding_repo import MissingFileRepo

pytestmark = pytest.mark.db


async def _seed_period(db: AsyncSession) -> AuditPeriod:
    inst = Institution(name="FindingTestHosp", display_name="Finding Test", nit="444444444", invoice_id_prefix="FT")
    db.add(inst)
    await db.flush()
    period = AuditPeriod(
        institution_id=inst.id,
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2024, 1, 31),
        period_label="2024-01",
    )
    db.add(period)
    await db.flush()
    return period


async def _seed_invoice(db: AsyncSession, period_id: int, invoice_number: str) -> Invoice:
    fs = (await db.execute(select(FolderStatus).where(FolderStatus.status == "PRESENTE"))).scalar_one()
    inv = Invoice(
        audit_period_id=period_id,
        invoice_number=invoice_number,
        date=datetime.date(2024, 1, 10),
        id_type="CC",
        id_number="1",
        patient_name="Test",
        folder_status_id=fs.id,
    )
    db.add(inv)
    await db.flush()
    return inv


class TestBulkUpsertFindings:
    async def test_inserts_pairs_and_is_idempotent(self, seeded: AsyncSession):
        period = await _seed_period(seeded)
        inv = await _seed_invoice(seeded, period.id, "FT001")
        dt = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()

        repo = MissingFileRepo(seeded)
        await repo.bulk_upsert_findings([(inv.id, dt.id)])
        await repo.bulk_upsert_findings([(inv.id, dt.id)])  # duplicate call, must not error or duplicate rows

        findings = await repo.get_for_invoice(inv.id)
        assert len(findings) == 1
        assert findings[0].doc_type_id == dt.id


class TestResolveMissingFile:
    async def test_sets_resolved_at(self, seeded: AsyncSession):
        period = await _seed_period(seeded)
        inv = await _seed_invoice(seeded, period.id, "FT002")
        dt = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()

        repo = MissingFileRepo(seeded)
        await repo.record_missing_file(inv.id, dt.id, expected_path="")
        await repo.resolve_missing_file(inv.id, dt.id)

        findings = await repo.get_for_invoice(inv.id)
        assert findings[0].resolved_at is not None


class TestGetFindingsSummary:
    async def test_counts_unresolved_by_doc_type(self, seeded: AsyncSession):
        period = await _seed_period(seeded)
        inv1 = await _seed_invoice(seeded, period.id, "FT003")
        inv2 = await _seed_invoice(seeded, period.id, "FT004")
        dt = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()

        repo = MissingFileRepo(seeded)
        await repo.bulk_upsert_findings([(inv1.id, dt.id), (inv2.id, dt.id)])

        summary = await repo.get_findings_summary(period.id)

        assert any(row["code"] == "FACTURA" and row["count"] == 2 for row in summary)
