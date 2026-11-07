"""HTTP tests for /api/settings — requires PostgreSQL."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.db


class TestServiceTypes:
    async def test_create_list_update(self, client):
        created = (
            await client.post("/api/settings/service-types", json={"code": "NUEVO", "display_name": "Nuevo"})
        ).json()
        assert created["priority"] == 10

        codes = [s["code"] for s in (await client.get("/api/settings/service-types")).json()]
        assert "NUEVO" in codes and "GENERAL" in codes  # seeded types are listed too

        patched = await client.patch(f"/api/settings/service-types/{created['id']}", json={"priority": 99})
        assert patched.json()["priority"] == 99

    async def test_patch_missing_is_404(self, client):
        assert (await client.patch("/api/settings/service-types/999999", json={"priority": 1})).status_code == 404

    async def test_delete_unused_then_missing(self, client):
        created = (
            await client.post("/api/settings/service-types", json={"code": "TEMP", "display_name": "Temp"})
        ).json()
        assert (await client.delete(f"/api/settings/service-types/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/settings/service-types/{created['id']}")).status_code == 404


class TestDocTypes:
    async def test_create_update_delete(self, client):
        created = (
            await client.post("/api/settings/doc-types", json={"code": "RX", "description": "Rayos X", "prefix": "RXD"})
        ).json()
        assert created["prefix"] == "RXD"

        patched = await client.patch(f"/api/settings/doc-types/{created['id']}", json={"description": "Radiografía"})
        assert patched.json()["description"] == "Radiografía"
        assert patched.json()["prefix"] == "RXD"  # untouched fields are kept

        assert (await client.delete(f"/api/settings/doc-types/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/settings/doc-types/{created['id']}")).status_code == 404

    async def test_patch_missing_is_404(self, client):
        assert (await client.patch("/api/settings/doc-types/999999", json={"description": "x"})).status_code == 404


class TestFolderStatuses:
    async def test_list_contains_seeded_statuses(self, client):
        names = {s["status"] for s in (await client.get("/api/settings/folder-statuses")).json()}
        assert {"PRESENTE", "FALTANTE", "AUDITADA"} <= names

    async def test_create_update_delete(self, client):
        created = (await client.post("/api/settings/folder-statuses", json={"status": "TEMPORAL"})).json()

        patched = await client.patch(f"/api/settings/folder-statuses/{created['id']}", json={"status": "TEMPORAL2"})
        assert patched.json()["status"] == "TEMPORAL2"

        assert (await client.delete(f"/api/settings/folder-statuses/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/settings/folder-statuses/{created['id']}")).status_code == 404

    async def test_patch_missing_is_404(self, client):
        assert (await client.patch("/api/settings/folder-statuses/999999", json={"status": "x"})).status_code == 404


class TestPrefixCorrections:
    async def test_create_update_delete(self, client):
        created = (
            await client.post("/api/settings/prefix-corrections", json={"wrong_prefix": "OPD", "correct_prefix": "OPF"})
        ).json()

        patched = await client.patch(
            f"/api/settings/prefix-corrections/{created['id']}", json={"notes": "corregido por auditoría"}
        )
        assert patched.json()["notes"] == "corregido por auditoría"
        assert patched.json()["correct_prefix"] == "OPF"

        listed = (await client.get("/api/settings/prefix-corrections")).json()
        assert any(c["id"] == created["id"] for c in listed)

        assert (await client.delete(f"/api/settings/prefix-corrections/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/settings/prefix-corrections/{created['id']}")).status_code == 404


class TestDeleteInUse:
    """Deleting a record that other rows reference answers 409 with a message naming what uses it."""

    async def test_service_type_used_by_a_service_mapping(self, client):
        inst = (
            await client.post("/api/institutions", json={"name": "INUSE_HOSP", "display_name": "x", "nit": "810000001"})
        ).json()
        service_type = (
            await client.post("/api/settings/service-types", json={"code": "ENUSO", "display_name": "En uso"})
        ).json()
        await client.post(
            f"/api/institutions/{inst['id']}/services",
            json={"raw_service": "CONSULTA", "service_type_id": service_type["id"]},
        )

        resp = await client.delete(f"/api/settings/service-types/{service_type['id']}")

        assert resp.status_code == 409
        assert "facturas o servicios" in resp.json()["detail"]

    async def test_doc_type_used_by_a_service_type_mapping(self, client, seeded):
        from sqlalchemy import select

        from app.models.rules import ServiceType

        inst = (
            await client.post("/api/institutions", json={"name": "INUSE_DOC", "display_name": "x", "nit": "810000002"})
        ).json()
        doc_type = (
            await client.post("/api/settings/doc-types", json={"code": "ENUSO", "description": "En uso"})
        ).json()
        general = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
        await client.post(
            f"/api/institutions/{inst['id']}/service-type-documents",
            json={"institution_id": inst["id"], "service_type_id": general.id, "doc_type_id": doc_type["id"]},
        )

        resp = await client.delete(f"/api/settings/doc-types/{doc_type['id']}")

        assert resp.status_code == 409
        assert "No se puede eliminar" in resp.json()["detail"]
        # the failed delete did not poison the session: the record is still there
        assert any(d["id"] == doc_type["id"] for d in (await client.get("/api/settings/doc-types")).json())
