"""HTTP tests for the file actions on an invoice's folder (surplus files, annulled folders) — needs PostgreSQL."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.models.invoice import Invoice
from app.models.rules import DocType, FolderStatus, ServiceType

pytestmark = pytest.mark.db


@pytest.fixture
async def env(client, seeded, tmp_path):
    """Period 2024-01 of institution INVF with invoice 1 whose STAGE folder HSL1 holds a surplus file."""
    with patch("app.paths.audit_data_root", tmp_path):
        inst = (
            await client.post("/api/institutions", json={"name": "INVF", "display_name": "x", "nit": "840000001"})
        ).json()
        period = (
            await client.post(
                f"/api/institutions/{inst['id']}/periods",
                json={
                    "institution_id": inst["id"],
                    "period_label": "2024-01",
                    "date_from": "2024-01-01",
                    "date_to": "2024-01-31",
                },
            )
        ).json()
        folder_status = (
            await seeded.execute(select(FolderStatus).where(FolderStatus.status == "PRESENTE"))
        ).scalar_one()
        service_type = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
        invoice = Invoice(
            audit_period_id=period["id"],
            invoice_number="1",
            date=datetime.date(2024, 1, 10),
            id_type="CC",
            id_number="1",
            patient_name="Paciente",
            service_type_id=service_type.id,
            folder_status_id=folder_status.id,
        )
        seeded.add(invoice)
        await seeded.flush()
        docs = {d.code: d for d in (await seeded.execute(select(DocType))).scalars()}

        stage = tmp_path / "INVF" / "2024-01" / "STAGE"
        folder = stage / "HSL1"
        folder.mkdir()
        (folder / "XXX_900_HSL1.pdf").write_bytes(b"%PDF-1.4")
        yield type(
            "Env",
            (),
            {
                "client": client,
                "ids": {"institution_id": inst["id"], "period_id": period["id"]},
                "invoice": invoice,
                "docs": docs,
                "stage": stage,
                "folder": folder,
                "seeded": seeded,
            },
        )


class TestRenameSurplus:
    async def test_gives_the_file_the_document_prefix_and_resolves_the_finding(self, env):
        await env.client.post(
            "/api/missing-files",
            json={"invoice_id": env.invoice.id, "doc_type_id": env.docs["HISTORIA"].id, "expected_path": ""},
        )

        resp = await env.client.post(
            f"/api/invoices/{env.invoice.id}/rename-surplus",
            json={
                "folder_path": str(env.folder),
                "old_filename": "XXX_900_HSL1.pdf",
                "doc_type_id": env.docs["HISTORIA"].id,
            },
        )

        assert resp.status_code == 200 and resp.json() == {"ok": True, "new_filename": "HCU_900_HSL1.pdf"}
        assert (env.folder / "HCU_900_HSL1.pdf").exists() and not (env.folder / "XXX_900_HSL1.pdf").exists()
        findings = (await env.client.get(f"/api/missing-files/{env.invoice.id}")).json()
        assert findings[0]["resolved_at"] is not None

    async def test_refuses_what_it_should(self, env):
        body = {
            "folder_path": str(env.folder),
            "old_filename": "XXX_900_HSL1.pdf",
            "doc_type_id": env.docs["HISTORIA"].id,
        }
        url = f"/api/invoices/{env.invoice.id}/rename-surplus"

        no_prefix = await env.client.post(url, json={**body, "doc_type_id": env.docs["SOPORTE"].id})
        assert no_prefix.status_code == 400 and "sin prefijo" in no_prefix.json()["detail"]
        bad_name = await env.client.post(url, json={**body, "old_filename": "../x.pdf"})
        assert bad_name.status_code == 400
        missing_file = await env.client.post(url, json={**body, "old_filename": "no.pdf"})
        assert missing_file.status_code == 404
        outside = await env.client.post(url, json={**body, "folder_path": str(env.stage.parent)})
        assert outside.status_code == 400
        assert (await env.client.post("/api/invoices/999999/rename-surplus", json=body)).status_code == 404


class TestDeleteSurplus:
    async def test_deletes_only_that_file(self, env):
        (env.folder / "otro.pdf").write_bytes(b"x")
        url = f"/api/invoices/{env.invoice.id}/delete-surplus"

        resp = await env.client.post(url, json={"folder_path": str(env.folder), "filename": "XXX_900_HSL1.pdf"})

        assert resp.status_code == 200
        assert [p.name for p in env.folder.iterdir()] == ["otro.pdf"]
        again = await env.client.post(url, json={"folder_path": str(env.folder), "filename": "XXX_900_HSL1.pdf"})
        assert again.status_code == 404

    async def test_refuses_a_path_outside_the_invoice_folders(self, env):
        resp = await env.client.post(
            f"/api/invoices/{env.invoice.id}/delete-surplus",
            json={
                "folder_path": str(env.stage),
                "filename": "XXX_900_HSL1.pdf",
            },  # a phase folder, not an invoice folder
        )
        assert resp.status_code == 400
        assert (env.folder / "XXX_900_HSL1.pdf").exists()


class TestAnnulFolders:
    async def test_removes_the_folder_and_marks_the_invoice_annulled(self, env):
        resp = await env.client.post(
            "/api/invoices/annul-folders",
            json={
                **env.ids,
                "decisions": [
                    {"folder_path": str(env.folder), "invoice_id": env.invoice.id, "action": "annul"},
                    {"folder_path": str(env.stage), "invoice_id": None, "action": "annul"},  # refused: a phase folder
                    {"folder_path": "x", "invoice_id": None, "action": "skip"},
                ],
            },
        )

        body = resp.json()
        assert (body["annulled"], body["skipped"]) == (1, 1) and len(body["errors"]) == 1
        assert not env.folder.exists() and env.stage.is_dir()
        status = (
            await env.seeded.execute(
                select(FolderStatus.status)
                .join(Invoice, Invoice.folder_status_id == FolderStatus.id)
                .where(Invoice.id == env.invoice.id)
            )
        ).scalar_one()
        assert status == "ANULAR"
