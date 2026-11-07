"""HTTP tests for /api/institutions — requires PostgreSQL."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.db

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
_SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "p",
    "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
    "client_email": "sa@p.iam.gserviceaccount.com",
}


async def _create(client, name="API_HOSP", **extra) -> dict:
    body = {"name": name, "display_name": "Hospital API", "nit": "900111222", "invoice_id_prefix": "AH", **extra}
    resp = await client.post("/api/institutions", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


class TestInstitutions:
    async def test_create_hides_secrets_and_flags_their_presence(self, client):
        created = await _create(client, sihos_user="u", sihos_password="s3cret")

        assert created["has_sihos_password"] is True
        assert created["has_drive_credentials"] is False
        assert "sihos_password" not in created
        assert "s3cret" not in json.dumps(created)

    async def test_list_and_get_return_created_institution(self, client):
        created = await _create(client)

        listed = (await client.get("/api/institutions")).json()
        assert any(i["id"] == created["id"] for i in listed)
        assert (await client.get(f"/api/institutions/{created['id']}")).json()["name"] == "API_HOSP"

    async def test_get_missing_is_404(self, client):
        assert (await client.get("/api/institutions/999999")).status_code == 404

    async def test_loopback_portal_url_is_rejected(self, client):
        resp = await client.post(
            "/api/institutions",
            json={"name": "SSRF", "display_name": "x", "nit": "1", "sihos_base_url": "http://127.0.0.1:8080"},
        )
        assert resp.status_code == 422

    async def test_name_with_path_separators_is_rejected(self, client):
        resp = await client.post("/api/institutions", json={"name": "../evil", "display_name": "x", "nit": "1"})
        assert resp.status_code == 422

    async def test_delete_then_get_is_404(self, client):
        created = await _create(client)
        assert (await client.delete(f"/api/institutions/{created['id']}")).status_code == 204
        assert (await client.get(f"/api/institutions/{created['id']}")).status_code == 404


class TestLogo:
    async def test_no_logo_is_404(self, client):
        created = await _create(client)
        assert (await client.get(f"/api/institutions/{created['id']}/logo")).status_code == 404

    async def test_upload_then_serve(self, client):
        created = await _create(client)
        up = await client.post(
            f"/api/institutions/{created['id']}/logo", files={"file": ("logo.png", _PNG, "image/png")}
        )
        assert up.status_code == 200

        got = await client.get(f"/api/institutions/{created['id']}/logo")
        assert got.status_code == 200
        assert got.content == _PNG
        assert got.headers["content-type"] == "image/png"

    async def test_unsupported_mime_is_422(self, client):
        created = await _create(client)
        resp = await client.post(
            f"/api/institutions/{created['id']}/logo", files={"file": ("x.svg", b"<svg/>", "image/svg+xml")}
        )
        assert resp.status_code == 422

    async def test_upload_to_missing_institution_is_404(self, client):
        resp = await client.post("/api/institutions/999999/logo", files={"file": ("l.png", _PNG, "image/png")})
        assert resp.status_code == 404


class TestDriveCredentials:
    async def test_valid_service_account_is_stored(self, client):
        created = await _create(client)
        resp = await client.post(
            f"/api/institutions/{created['id']}/drive-credentials",
            files={"file": ("sa.json", json.dumps(_SERVICE_ACCOUNT).encode(), "application/json")},
        )
        assert resp.status_code == 200
        assert (await client.get(f"/api/institutions/{created['id']}")).json()["has_drive_credentials"] is True

    async def test_not_json_is_422(self, client):
        created = await _create(client)
        resp = await client.post(
            f"/api/institutions/{created['id']}/drive-credentials",
            files={"file": ("sa.json", b"not json", "application/json")},
        )
        assert resp.status_code == 422

    async def test_json_without_service_account_keys_is_422(self, client):
        created = await _create(client)
        resp = await client.post(
            f"/api/institutions/{created['id']}/drive-credentials",
            files={"file": ("sa.json", b'{"type": "service_account"}', "application/json")},
        )
        assert resp.status_code == 422

    async def test_oversized_file_is_413(self, client):
        created = await _create(client)
        resp = await client.post(
            f"/api/institutions/{created['id']}/drive-credentials",
            files={"file": ("sa.json", b"x" * (64 * 1024 + 10), "application/json")},
        )
        assert resp.status_code == 413


class TestAdministrators:
    async def test_create_update_delete_lifecycle(self, client):
        created = (await client.post("/api/institutions/administrators", json={"raw_name": "EPS RAW"})).json()
        assert created["canonical_name"] is None

        patched = await client.patch(
            f"/api/institutions/administrators/{created['id']}", json={"canonical_name": "EPS CANON"}
        )
        assert patched.status_code == 200
        assert patched.json()["canonical_name"] == "EPS CANON"

        assert (await client.delete(f"/api/institutions/administrators/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/institutions/administrators/{created['id']}")).status_code == 404

    async def test_patch_missing_is_404(self, client):
        resp = await client.patch("/api/institutions/administrators/999999", json={"canonical_name": "X"})
        assert resp.status_code == 404


class TestContracts:
    async def test_create_update_delete_lifecycle(self, client):
        created = (await client.post("/api/institutions/contracts", json={"raw_name": "CONTRATO RAW"})).json()

        patched = await client.patch(f"/api/institutions/contracts/{created['id']}", json={"canonical_name": "C1"})
        assert patched.json()["canonical_name"] == "C1"

        assert (await client.delete(f"/api/institutions/contracts/{created['id']}")).status_code == 204
        assert (await client.delete(f"/api/institutions/contracts/{created['id']}")).status_code == 404

    async def test_patch_missing_is_404(self, client):
        resp = await client.patch("/api/institutions/contracts/999999", json={"canonical_name": "X"})
        assert resp.status_code == 404


class TestMissingSubresources:
    async def test_update_missing_agreement_is_404(self, client):
        resp = await client.patch("/api/institutions/agreements/999999", json={"contract_type_id": None})
        assert resp.status_code == 404

    async def test_update_missing_service_is_404(self, client):
        resp = await client.patch("/api/institutions/services/999999", json={"service_type_id": None})
        assert resp.status_code == 404

    async def test_services_of_missing_institution_is_404(self, client):
        assert (await client.get("/api/institutions/999999/services")).status_code == 404
