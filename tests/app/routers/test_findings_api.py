"""HTTP tests for /api/missing-files — requires PostgreSQL."""

from __future__ import annotations

import datetime

import pytest
from sqlalchemy import select

from app.models.institution import Institution
from app.models.invoice import Invoice
from app.models.period import AuditPeriod
from app.models.rules import DocType, FolderStatus, ServiceType

pytestmark = pytest.mark.db


@pytest.fixture
async def invoice_and_docs(seeded):
    """One invoice (PRESENTE) plus the two seeded doc types HISTORIA and SOPORTE."""
    inst = Institution(name="FindingsApiHosp", display_name="Findings", nit="333333333", invoice_id_prefix="FA")
    seeded.add(inst)
    await seeded.flush()
    period = AuditPeriod(
        institution_id=inst.id,
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2024, 1, 31),
        period_label="2024-01",
    )
    seeded.add(period)
    await seeded.flush()
    fs = (await seeded.execute(select(FolderStatus).where(FolderStatus.status == "PRESENTE"))).scalar_one()
    st = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
    invoice = Invoice(
        audit_period_id=period.id,
        invoice_number="FA1",
        date=datetime.date(2024, 1, 10),
        id_type="CC",
        id_number="1",
        patient_name="Paciente",
        service_type_id=st.id,
        folder_status_id=fs.id,
    )
    seeded.add(invoice)
    await seeded.flush()
    docs = {d.code: d for d in (await seeded.execute(select(DocType))).scalars()}
    return invoice, docs


async def _record(client, invoice, doc, path="FA1/HCU.pdf"):
    resp = await client.post(
        "/api/missing-files",
        json={"invoice_id": invoice.id, "doc_type_id": doc.id, "expected_path": path},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


class TestMissingFiles:
    async def test_record_then_list(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        created = await _record(client, invoice, docs["HISTORIA"])
        assert created["resolved_at"] is None

        listed = (await client.get(f"/api/missing-files/{invoice.id}")).json()
        assert [f["doc_type_id"] for f in listed] == [docs["HISTORIA"].id]

    async def test_list_for_unknown_invoice_is_empty(self, client):
        resp = await client.get("/api/missing-files/999999")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_resolve_marks_resolved_at(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        await _record(client, invoice, docs["HISTORIA"])

        resp = await client.patch(
            f"/api/missing-files/{invoice.id}/{docs['HISTORIA'].id}/resolve",
            json={"resolved_at": "2024-02-01T10:00:00"},
        )
        assert resp.status_code == 200
        assert resp.json()["resolved_at"] is not None

    async def test_resolve_unknown_finding_is_404(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        resp = await client.patch(
            f"/api/missing-files/{invoice.id}/{docs['SOPORTE'].id}/resolve",
            json={"resolved_at": "2024-02-01T10:00:00"},
        )
        assert resp.status_code == 404

    async def test_delete_one_keeps_the_others(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        await _record(client, invoice, docs["HISTORIA"])
        await _record(client, invoice, docs["SOPORTE"], path="FA1/SOP.pdf")

        assert (await client.delete(f"/api/missing-files/{invoice.id}/{docs['HISTORIA'].id}")).status_code == 204

        left = (await client.get(f"/api/missing-files/{invoice.id}")).json()
        assert [f["doc_type_id"] for f in left] == [docs["SOPORTE"].id]

    async def test_delete_all_for_invoice(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        await _record(client, invoice, docs["HISTORIA"])
        await _record(client, invoice, docs["SOPORTE"], path="FA1/SOP.pdf")

        assert (await client.delete(f"/api/missing-files/{invoice.id}")).status_code == 204
        assert (await client.get(f"/api/missing-files/{invoice.id}")).json() == []

    async def test_batch_delete_counts_and_handles_empty(self, client, invoice_and_docs):
        invoice, docs = invoice_and_docs
        await _record(client, invoice, docs["HISTORIA"])

        assert (await client.post("/api/missing-files/batch-delete", json={"invoice_ids": []})).json() == {"deleted": 0}
        resp = await client.post("/api/missing-files/batch-delete", json={"invoice_ids": [invoice.id]})
        assert resp.json() == {"deleted": 1}
        assert (await client.get(f"/api/missing-files/{invoice.id}")).json() == []

    async def test_record_requires_all_fields(self, client):
        assert (await client.post("/api/missing-files", json={"invoice_id": 1})).status_code == 422
