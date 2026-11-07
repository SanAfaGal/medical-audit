"""HTTP tests for /api/invoices (listing, status changes, deletes, export) — requires PostgreSQL."""

from __future__ import annotations

import datetime
import io

import pytest
from sqlalchemy import select

from app.models.institution import Institution
from app.models.invoice import Invoice
from app.models.period import AuditPeriod
from app.models.rules import FolderStatus, ServiceType

pytestmark = pytest.mark.db


@pytest.fixture
async def period_invoices(seeded):
    """A period with 3 invoices: IA1 and IA2 PRESENTE, IA3 FALTANTE."""
    inst = Institution(name="InvApiHosp", display_name="Inv API", nit="555000111", invoice_id_prefix="IA")
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
    statuses = {s.status: s for s in (await seeded.execute(select(FolderStatus))).scalars()}
    st = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
    invoices = []
    for number, status, patient in [
        ("IA1", "PRESENTE", "Ana"),
        ("IA2", "PRESENTE", "Beto"),
        ("IA3", "FALTANTE", "Cata"),
    ]:
        inv = Invoice(
            audit_period_id=period.id,
            invoice_number=number,
            date=datetime.date(2024, 1, 10),
            id_type="CC",
            id_number="1",
            patient_name=patient,
            service_type_id=st.id,
            folder_status_id=statuses[status].id,
        )
        seeded.add(inv)
        invoices.append(inv)
    await seeded.flush()
    return period, invoices, statuses


class TestListing:
    async def test_list_returns_all_with_pagination_metadata(self, client, period_invoices):
        period, _, _ = period_invoices
        body = (await client.get("/api/invoices", params={"period_id": period.id, "page_size": 2})).json()

        assert body["total"] == 3
        assert body["page"] == 1 and body["page_size"] == 2
        assert len(body["items"]) == 2
        assert {"invoice_number", "folder_status", "missing_file_count"} <= body["items"][0].keys()

    async def test_filter_by_status_and_search(self, client, period_invoices):
        period, _, statuses = period_invoices

        by_status = (
            await client.get(
                "/api/invoices", params={"period_id": period.id, "folder_status_id": statuses["FALTANTE"].id}
            )
        ).json()
        assert [i["invoice_number"] for i in by_status["items"]] == ["IA3"]

        by_search = (await client.get("/api/invoices", params={"period_id": period.id, "search": "Beto"})).json()
        assert [i["invoice_number"] for i in by_search["items"]] == ["IA2"]

    async def test_ids_endpoint_honours_filters(self, client, period_invoices):
        period, invoices, statuses = period_invoices
        ids = (
            await client.get(
                "/api/invoices/ids", params={"period_id": period.id, "folder_status_id": statuses["PRESENTE"].id}
            )
        ).json()
        assert sorted(ids) == sorted([invoices[0].id, invoices[1].id])

    async def test_stats_and_findings_summary(self, client, period_invoices):
        period, _, _ = period_invoices
        stats = await client.get("/api/invoices/stats", params={"period_id": period.id})
        assert stats.status_code == 200
        assert isinstance(stats.json(), dict)

        summary = await client.get("/api/invoices/findings-summary", params={"period_id": period.id})
        assert summary.status_code == 200
        assert summary.json() == []

    async def test_period_id_is_required(self, client):
        assert (await client.get("/api/invoices")).status_code == 422


class TestStatusChanges:
    async def test_update_status_changes_folder_status(self, client, period_invoices):
        _, invoices, statuses = period_invoices
        resp = await client.patch(
            f"/api/invoices/{invoices[2].id}/status", json={"folder_status_id": statuses["PRESENTE"].id}
        )
        assert resp.status_code == 200
        assert resp.json()["folder_status_id"] == statuses["PRESENTE"].id

    async def test_update_status_of_missing_invoice_is_404(self, client, period_invoices):
        _, _, statuses = period_invoices
        resp = await client.patch("/api/invoices/999999/status", json={"folder_status_id": statuses["PRESENTE"].id})
        assert resp.status_code == 404

    async def test_batch_status_updates_every_invoice(self, client, period_invoices):
        period, invoices, statuses = period_invoices
        resp = await client.post(
            "/api/invoices/batch-status",
            json={"invoice_ids": [invoices[0].id, invoices[1].id], "folder_status_id": statuses["PENDIENTE"].id},
        )
        assert resp.status_code == 200

        pending = (
            await client.get(
                "/api/invoices", params={"period_id": period.id, "folder_status_id": statuses["PENDIENTE"].id}
            )
        ).json()
        assert pending["total"] == 2


class TestDeletes:
    async def test_delete_invoice_then_missing(self, client, period_invoices):
        _, invoices, _ = period_invoices
        assert (await client.delete(f"/api/invoices/{invoices[0].id}")).status_code == 204
        assert (await client.delete(f"/api/invoices/{invoices[0].id}")).status_code == 404

    async def test_batch_delete_removes_listed_invoices(self, client, period_invoices):
        period, invoices, _ = period_invoices
        resp = await client.post("/api/invoices/batch-delete", json={"invoice_ids": [invoices[0].id, invoices[1].id]})
        assert resp.status_code == 200

        assert (await client.get("/api/invoices", params={"period_id": period.id})).json()["total"] == 1

    async def test_batch_delete_rejects_empty_list(self, client):
        assert (await client.post("/api/invoices/batch-delete", json={"invoice_ids": []})).status_code == 422


class TestExport:
    async def test_export_returns_workbook_with_one_row_per_invoice(self, client, period_invoices):
        import openpyxl

        period, _, _ = period_invoices
        resp = await client.get("/api/invoices/export", params={"period_id": period.id})

        assert resp.status_code == 200
        assert "spreadsheetml" in resp.headers["content-type"]
        rows = list(openpyxl.load_workbook(io.BytesIO(resp.content)).active.iter_rows(values_only=True))
        assert len(rows) == 1 + 3  # header + 3 invoices
        assert "Factura" in rows[0]
