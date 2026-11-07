"""HTTP tests for /api/institutions/.../periods (creation, disk usage, purge, delete) — requires PostgreSQL."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytestmark = pytest.mark.db

_PERIOD = {"date_from": "2024-01-01", "date_to": "2024-01-31", "period_label": "2024-01"}


@pytest.fixture
def audit_root(tmp_path):
    root = tmp_path / "audit"
    root.mkdir()
    with patch("app.paths.audit_data_root", root):
        yield root


async def _institution(client, name="PER_HOSP") -> dict:
    resp = await client.post("/api/institutions", json={"name": name, "display_name": name, "nit": "800000001"})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _period(client, inst) -> dict:
    resp = await client.post(f"/api/institutions/{inst['id']}/periods", json={**_PERIOD, "institution_id": inst["id"]})
    assert resp.status_code == 201, resp.text
    return resp.json()


class TestPeriods:
    async def test_create_builds_phase_folders_and_is_listed(self, client, audit_root):
        inst = await _institution(client)
        period = await _period(client, inst)

        for phase in ("DRIVE", "STAGE", "AUDIT"):
            assert (audit_root / "PER_HOSP" / "2024-01" / phase).is_dir()
        listed = (await client.get(f"/api/institutions/{inst['id']}/periods")).json()
        assert [p["id"] for p in listed] == [period["id"]]

    async def test_create_twice_returns_the_same_period(self, client, audit_root):
        inst = await _institution(client)
        first = await _period(client, inst)
        second = await _period(client, inst)
        assert first["id"] == second["id"]

    async def test_unknown_institution_is_404(self, client, audit_root):
        assert (await client.get("/api/institutions/999999/periods")).status_code == 404
        assert (
            await client.post("/api/institutions/999999/periods", json={**_PERIOD, "institution_id": 999999})
        ).status_code == 404


class TestDiskUsage:
    async def test_counts_bytes_per_phase(self, client, audit_root):
        inst = await _institution(client)
        period = await _period(client, inst)
        base = audit_root / "PER_HOSP" / "2024-01"
        (base / "STAGE" / "HSL1").mkdir()
        (base / "STAGE" / "HSL1" / "a.pdf").write_bytes(b"x" * 100)
        (base / "AUDIT" / "b.pdf").write_bytes(b"y" * 50)

        body = (await client.get(f"/api/institutions/periods/{period['id']}/disk-usage")).json()

        assert body["stage_bytes"] == 100
        assert body["audit_bytes"] == 50
        assert body["drive_bytes"] == 0
        assert body["total_bytes"] == 150
        assert body["exists"] is True

    async def test_unknown_period_is_404(self, client, audit_root):
        assert (await client.get("/api/institutions/periods/999999/disk-usage")).status_code == 404


class TestPurgeAndDelete:
    async def test_purge_removes_folders_but_keeps_the_period(self, client, audit_root):
        inst = await _institution(client)
        period = await _period(client, inst)
        (audit_root / "PER_HOSP" / "2024-01" / "STAGE" / "a.pdf").write_bytes(b"x")

        resp = await client.post(f"/api/institutions/periods/{period['id']}/purge-disk")

        assert resp.status_code == 200
        assert not (audit_root / "PER_HOSP" / "2024-01").exists()
        listed = (await client.get(f"/api/institutions/{inst['id']}/periods")).json()
        assert [p["id"] for p in listed] == [period["id"]]

    async def test_purge_unknown_period_is_404(self, client, audit_root):
        assert (await client.post("/api/institutions/periods/999999/purge-disk")).status_code == 404

    async def test_delete_removes_period_and_folders(self, client, audit_root):
        inst = await _institution(client)
        period = await _period(client, inst)

        assert (await client.delete(f"/api/institutions/periods/{period['id']}")).status_code == 204

        assert (await client.get(f"/api/institutions/{inst['id']}/periods")).json() == []
        assert not (audit_root / "PER_HOSP" / "2024-01").exists()

    async def test_delete_unknown_period_is_a_noop(self, client, audit_root):
        assert (await client.delete("/api/institutions/periods/999999")).status_code == 204
