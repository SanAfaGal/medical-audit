"""HTTP tests for the mapping sub-resources of /api/institutions — requires PostgreSQL."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from app.models.rules import DocType, ServiceType

pytestmark = pytest.mark.db


async def _institution(client, name: str) -> dict:
    nit = str(900000000 + sum(map(ord, name)))  # nit is unique: derive one per name
    resp = await client.post(
        "/api/institutions", json={"name": name, "display_name": name, "nit": nit, "invoice_id_prefix": "X"}
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


class TestContractTypes:
    async def test_create_list_update_delete(self, client):
        created = (await client.post("/api/institutions/contract-types", json={"name": "EVENTO"})).json()

        assert any(c["id"] == created["id"] for c in (await client.get("/api/institutions/contract-types")).json())

        patched = await client.patch(
            f"/api/institutions/contract-types/{created['id']}", json={"description": "Pago por evento"}
        )
        assert patched.json()["description"] == "Pago por evento"
        assert patched.json()["name"] == "EVENTO"

        assert (await client.delete(f"/api/institutions/contract-types/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/institutions/contract-types/{created['id']}")).status_code == 404

    async def test_patch_missing_is_404(self, client):
        resp = await client.patch("/api/institutions/contract-types/999999", json={"name": "X"})
        assert resp.status_code == 404


class TestAgreements:
    async def _pair(self, client) -> tuple[dict, dict]:
        admin = (await client.post("/api/institutions/administrators", json={"raw_name": "ADMIN AG"})).json()
        contract = (await client.post("/api/institutions/contracts", json={"raw_name": "CONTRATO AG"})).json()
        return admin, contract

    async def test_create_includes_relations_and_is_pending_until_typed(self, client):
        admin, contract = await self._pair(client)
        created = await client.post(
            "/api/institutions/agreements", json={"administrator_id": admin["id"], "contract_id": contract["id"]}
        )
        assert created.status_code == 201
        agreement = created.json()

        pending = (await client.get("/api/institutions/agreements", params={"pending_only": True})).json()
        assert any(a["id"] == agreement["id"] for a in pending)

        ct = (await client.post("/api/institutions/contract-types", json={"name": "CAPITA"})).json()
        patched = await client.patch(
            f"/api/institutions/agreements/{agreement['id']}", json={"contract_type_id": ct["id"]}
        )
        assert patched.status_code == 200

        pending = (await client.get("/api/institutions/agreements", params={"pending_only": True})).json()
        assert all(a["id"] != agreement["id"] for a in pending)
        assert any(a["id"] == agreement["id"] for a in (await client.get("/api/institutions/agreements")).json())

    async def test_delete_then_missing(self, client):
        admin, contract = await self._pair(client)
        agreement = (
            await client.post(
                "/api/institutions/agreements", json={"administrator_id": admin["id"], "contract_id": contract["id"]}
            )
        ).json()

        assert (await client.delete(f"/api/institutions/agreements/{agreement['id']}")).status_code == 204
        assert (await client.delete(f"/api/institutions/agreements/{agreement['id']}")).status_code == 404

    async def test_consolidate_returns_counts(self, client):
        resp = await client.post("/api/institutions/agreements/consolidate")
        assert resp.status_code == 200
        assert isinstance(resp.json(), dict)

    async def test_list_pending_administrators_and_contracts(self, client):
        admin = (await client.post("/api/institutions/administrators", json={"raw_name": "PEND ADMIN"})).json()
        contract = (await client.post("/api/institutions/contracts", json={"raw_name": "PEND CONTRATO"})).json()

        admins = (await client.get("/api/institutions/administrators", params={"pending_only": True})).json()
        contracts = (await client.get("/api/institutions/contracts", params={"pending_only": True})).json()

        assert any(a["id"] == admin["id"] for a in admins)
        assert any(c["id"] == contract["id"] for c in contracts)


class TestServices:
    async def test_create_list_update_delete(self, client, seeded):
        inst = await _institution(client, "SVC_HOSP")
        general = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()

        created = await client.post(f"/api/institutions/{inst['id']}/services", json={"raw_service": "CONSULTA"})
        assert created.status_code == 201
        service = created.json()
        assert service["service_type_id"] is None

        patched = await client.patch(
            f"/api/institutions/services/{service['id']}", json={"service_type_id": general.id}
        )
        assert patched.json()["service_type_id"] == general.id

        listed = (await client.get(f"/api/institutions/{inst['id']}/services")).json()
        assert [s["id"] for s in listed] == [service["id"]]

        assert (await client.delete(f"/api/institutions/services/{service['id']}")).status_code == 204
        assert (await client.delete(f"/api/institutions/services/{service['id']}")).status_code == 404

    async def test_create_for_missing_institution_is_404(self, client):
        resp = await client.post("/api/institutions/999999/services", json={"raw_service": "X"})
        assert resp.status_code == 404


class TestServiceTypeDocuments:
    async def test_create_list_delete(self, client, seeded):
        inst = await _institution(client, "STD_HOSP")
        general = (await seeded.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
        factura = (await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))).scalar_one()
        body = {"institution_id": inst["id"], "service_type_id": general.id, "doc_type_id": factura.id}

        created = await client.post(f"/api/institutions/{inst['id']}/service-type-documents", json=body)
        assert created.status_code == 201

        listed = (await client.get(f"/api/institutions/{inst['id']}/service-type-documents")).json()
        assert [(d["service_type_id"], d["doc_type_id"]) for d in listed] == [(general.id, factura.id)]

        url = f"/api/institutions/{inst['id']}/service-type-documents/{general.id}/{factura.id}"
        assert (await client.delete(url)).status_code == 204
        assert (await client.get(f"/api/institutions/{inst['id']}/service-type-documents")).json() == []

    async def test_missing_institution_is_404(self, client):
        body = {"institution_id": 999999, "service_type_id": 1, "doc_type_id": 1}
        assert (await client.post("/api/institutions/999999/service-type-documents", json=body)).status_code == 404
        assert (await client.get("/api/institutions/999999/service-type-documents")).status_code == 404


class TestCopyMappings:
    async def test_copies_services_from_reference_institution(self, client):
        source = await _institution(client, "COPY_SRC")
        target = await _institution(client, "COPY_DST")
        await client.post(f"/api/institutions/{source['id']}/services", json={"raw_service": "URGENCIAS"})

        resp = await client.post(
            f"/api/institutions/{target['id']}/copy-mappings", json={"source_institution_id": source["id"]}
        )

        assert resp.status_code == 200
        assert resp.json()["services"] == 1
        copied = (await client.get(f"/api/institutions/{target['id']}/services")).json()
        assert [s["raw_service"] for s in copied] == ["URGENCIAS"]

    async def test_same_source_and_target_is_400(self, client):
        inst = await _institution(client, "COPY_SELF")
        resp = await client.post(
            f"/api/institutions/{inst['id']}/copy-mappings", json={"source_institution_id": inst["id"]}
        )
        assert resp.status_code == 400

    async def test_unknown_institutions_are_404(self, client):
        inst = await _institution(client, "COPY_REAL")
        assert (
            await client.post("/api/institutions/999999/copy-mappings", json={"source_institution_id": inst["id"]})
        ).status_code == 404
        assert (
            await client.post(f"/api/institutions/{inst['id']}/copy-mappings", json={"source_institution_id": 999999})
        ).status_code == 404
