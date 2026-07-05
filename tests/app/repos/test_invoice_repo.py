"""Tests for app/repositories/invoice_repo.py — requires PostgreSQL."""

from __future__ import annotations

import datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.institution import Institution
from app.models.invoice import Invoice
from app.models.period import AuditPeriod
from app.models.rules import FolderStatus, ServiceType
from app.repositories.invoice_repo import InvoiceRepo

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_institution(db: AsyncSession) -> Institution:
    inst = Institution(
        name="RepoTestHosp",
        display_name="Repo Test",
        nit="222222222",
        invoice_id_prefix="RT",
    )
    db.add(inst)
    await db.flush()
    return inst


async def _seed_period(db: AsyncSession, inst_id: int) -> AuditPeriod:
    period = AuditPeriod(
        institution_id=inst_id,
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2024, 1, 31),
        period_label="2024-01",
    )
    db.add(period)
    await db.flush()
    return period


async def _seed_invoice(db, period_id, invoice_number, status_code="PRESENTE", st_code="GENERAL"):
    fs = (await db.execute(select(FolderStatus).where(FolderStatus.status == status_code))).scalar_one()
    st = (await db.execute(select(ServiceType).where(ServiceType.code == st_code))).scalar_one()
    inv = Invoice(
        audit_period_id=period_id,
        invoice_number=invoice_number,
        date=datetime.date(2024, 1, 10),
        id_type="CC",
        id_number="12345",
        patient_name="Paciente Test",
        service_type_id=st.id,
        folder_status_id=fs.id,
    )
    db.add(inv)
    await db.flush()
    return inv


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUpsertInvoice:
    async def test_inserts_new_invoice(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        repo = InvoiceRepo(seeded)
        fs = (await seeded.execute(select(FolderStatus).where(FolderStatus.status == "PRESENTE"))).scalar_one()
        st = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()

        inv = await repo.upsert_invoice(
            period.id,
            "RT001",
            {
                "date": datetime.date(2024, 1, 1),
                "id_type": "CC",
                "id_number": "1",
                "patient_name": "P",
                "service_type_id": st.id,
                "folder_status_id": fs.id,
            },
        )
        assert inv.id is not None
        assert inv.invoice_number == "RT001"

    async def test_upsert_is_idempotent(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        repo = InvoiceRepo(seeded)
        fs = (await seeded.execute(select(FolderStatus).where(FolderStatus.status == "PRESENTE"))).scalar_one()
        st = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
        data = {
            "date": datetime.date(2024, 1, 1),
            "id_type": "CC",
            "id_number": "1",
            "patient_name": "P",
            "service_type_id": st.id,
            "folder_status_id": fs.id,
        }

        inv1 = await repo.upsert_invoice(period.id, "RT002", data)
        inv2 = await repo.upsert_invoice(period.id, "RT002", data)
        assert inv1.id == inv2.id


class TestGetInvoiceNumbersByStatus:
    async def test_returns_only_matching_status(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        await _seed_invoice(seeded, period.id, "RT010", "PRESENTE")
        await _seed_invoice(seeded, period.id, "RT011", "FALTANTE")

        repo = InvoiceRepo(seeded)
        presente = await repo.get_invoice_numbers_by_status(period.id, "PRESENTE")
        assert "RT010" in presente
        assert "RT011" not in presente

    async def test_returns_empty_for_no_matches(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        repo = InvoiceRepo(seeded)
        result = await repo.get_invoice_numbers_by_status(period.id, "AUDITADA")
        assert result == []


class TestBatchUpdateFolderStatus:
    async def test_updates_matching_invoices(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        await _seed_invoice(seeded, period.id, "RT020", "PRESENTE")
        await _seed_invoice(seeded, period.id, "RT021", "PRESENTE")

        repo = InvoiceRepo(seeded)
        count = await repo.batch_update_folder_status(period.id, ["RT020"], "FALTANTE")
        assert count == 1

        faltante = await repo.get_invoice_numbers_by_status(period.id, "FALTANTE")
        assert "RT020" in faltante
        presente = await repo.get_invoice_numbers_by_status(period.id, "PRESENTE")
        assert "RT021" in presente  # unchanged

    async def test_raises_for_unknown_status(self, seeded: AsyncSession):
        repo = InvoiceRepo(seeded)
        with pytest.raises(ValueError, match="BOGUS"):
            await repo.batch_update_folder_status(1, ["X"], "BOGUS")

    async def test_empty_list_returns_zero(self, seeded: AsyncSession):
        repo = InvoiceRepo(seeded)
        count = await repo.batch_update_folder_status(1, [], "PRESENTE")
        assert count == 0

    async def test_logs_warning_when_trigger_overrides_presente(self, seeded: AsyncSession, caplog):
        from sqlalchemy import text

        from app.models.finding import MissingFile
        from app.models.rules import DocType

        # Recreate the guard_invoice_presente_status trigger from
        # alembic/versions/05e49443f9b5_initial_schema.py.
        # The db_session/seeded fixtures use Base.metadata.create_all (not alembic
        # migrations), so this raw-SQL trigger doesn't exist unless created here.
        # DDL is transactional in Postgres, so this is rolled back with the rest
        # of the test's transaction — no cleanup needed, no effect on other tests.
        await seeded.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION guard_invoice_presente_status()
                RETURNS TRIGGER AS $$
                DECLARE
                    v_presente_id   INTEGER;
                    v_pendiente_id  INTEGER;
                BEGIN
                    SELECT id INTO v_presente_id  FROM folder_statuses WHERE status = 'PRESENTE';
                    SELECT id INTO v_pendiente_id FROM folder_statuses WHERE status = 'PENDIENTE';
                    IF NEW.folder_status_id = v_presente_id THEN
                        IF EXISTS (
                            SELECT 1 FROM missing_files
                            WHERE invoice_id = NEW.id AND resolved_at IS NULL
                        ) THEN
                            NEW.folder_status_id := v_pendiente_id;
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )
        await seeded.execute(
            text(
                """
                CREATE TRIGGER trg_guard_invoice_presente_status
                BEFORE UPDATE OF folder_status_id ON invoices
                FOR EACH ROW EXECUTE FUNCTION guard_invoice_presente_status();
                """
            )
        )

        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        inv = await _seed_invoice(seeded, period.id, "RT060", "PENDIENTE")

        dt = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()
        seeded.add(MissingFile(invoice_id=inv.id, doc_type_id=dt.id, expected_path=""))
        await seeded.flush()

        repo = InvoiceRepo(seeded)
        with caplog.at_level("WARNING"):
            updated = await repo.batch_update_folder_status(period.id, ["RT060"], "PRESENTE")

        assert updated == 1
        assert any("trigger overrode" in record.message for record in caplog.records)

        pendiente = await repo.get_invoice_numbers_by_status(period.id, "PENDIENTE")
        assert "RT060" in pendiente  # trigger's silent correction still applies


class TestGetOrganizableInvoices:
    async def test_returns_presente_without_findings(self, seeded: AsyncSession):
        from app.models.finding import MissingFile

        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        await _seed_invoice(seeded, period.id, "RT030", "PRESENTE")
        inv_dirty = await _seed_invoice(seeded, period.id, "RT031", "PRESENTE")

        # Add an unresolved finding to inv_dirty
        from app.models.rules import DocType

        dt = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()
        seeded.add(MissingFile(invoice_id=inv_dirty.id, doc_type_id=dt.id, expected_path=""))
        await seeded.flush()

        repo = InvoiceRepo(seeded)
        organizable = await repo.get_organizable_invoices(period.id)
        numbers = [inv.invoice_number for inv in organizable]
        assert "RT030" in numbers
        assert "RT031" not in numbers

    async def test_excludes_non_presente(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        await _seed_invoice(seeded, period.id, "RT040", "FALTANTE")
        repo = InvoiceRepo(seeded)
        organizable = await repo.get_organizable_invoices(period.id)
        assert not any(inv.invoice_number == "RT040" for inv in organizable)


class TestGetServiceTypeDistribution:
    async def test_groups_by_service_type(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        await _seed_invoice(seeded, period.id, "RT050", st_code="GENERAL")
        await _seed_invoice(seeded, period.id, "RT051", st_code="GENERAL")
        await _seed_invoice(seeded, period.id, "RT052", st_code="URGENCIAS")

        repo = InvoiceRepo(seeded)
        dist = await repo.get_service_type_distribution(period.id)
        assert dist["GENERAL"] == 2
        assert dist["URGENCIAS"] == 1

    async def test_empty_period_returns_empty_dict(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)
        repo = InvoiceRepo(seeded)
        assert await repo.get_service_type_distribution(period.id) == {}


class TestFolderStatusIdHasNoImplicitDefault:
    async def test_creating_invoice_without_folder_status_id_fails(self, seeded: AsyncSession):
        inst = await _seed_institution(seeded)
        period = await _seed_period(seeded, inst.id)

        inv = Invoice(
            audit_period_id=period.id,
            invoice_number="RT070",
            date=datetime.date(2024, 1, 10),
            id_type="CC",
            id_number="1",
            patient_name="Test",
            # folder_status_id intentionally omitted
        )
        seeded.add(inv)
        with pytest.raises(Exception):  # IntegrityError: NOT NULL violation, no implicit default
            await seeded.flush()
