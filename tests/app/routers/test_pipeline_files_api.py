"""HTTP tests for the file endpoints of /api/pipeline (non-PDF review, preview, AUDIT zip, Drive zip upload)."""

from __future__ import annotations

import io
import zipfile
from unittest.mock import patch

import fitz
import pytest

pytestmark = pytest.mark.db


def _png() -> bytes:
    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 4, 4), False)
    pix.clear_with(200)
    return pix.tobytes("png")


_PNG = _png()


@pytest.fixture
async def env(client, tmp_path):
    """A period with its DRIVE/STAGE/AUDIT folders created; ids ready for the requests."""
    with patch("app.paths.audit_data_root", tmp_path):
        inst = (
            await client.post("/api/institutions", json={"name": "PIPE", "display_name": "x", "nit": "830000001"})
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
        base = tmp_path / "PIPE" / "2024-01"
        yield type(
            "Env",
            (),
            {
                "client": client,
                "ids": {"institution_id": inst["id"], "period_id": period["id"]},
                "stage": base / "STAGE",
                "audit": base / "AUDIT",
                "drive": base / "DRIVE",
            },
        )


class TestProcessNonPdf:
    async def _post(self, env, decisions):
        return await env.client.post("/api/pipeline/process-non-pdf", json={**env.ids, "decisions": decisions})

    async def test_deletes_and_converts_images(self, env):
        (env.stage / "nota.txt").write_text("x")
        (env.stage / "foto.png").write_bytes(_PNG)

        resp = await self._post(
            env,
            [{"rel_path": "nota.txt", "action": "delete"}, {"rel_path": "foto.png", "action": "convert"}],
        )

        body = resp.json()
        assert resp.status_code == 200
        assert (body["deleted"], body["converted"], body["errors"]) == (1, 1, [])
        assert not (env.stage / "nota.txt").exists()
        assert (env.stage / "foto.pdf").exists() or any(env.stage.glob("foto*.pdf"))

    async def test_reports_each_problem_without_stopping(self, env):
        (env.stage / "dato.xyz").write_text("x")

        resp = await self._post(
            env,
            [
                {"rel_path": "../../etc/passwd", "action": "delete"},
                {"rel_path": "no-existe.txt", "action": "delete"},
                {"rel_path": "dato.xyz", "action": "convert"},
            ],
        )

        errors = resp.json()["errors"]
        assert resp.json()["deleted"] == 0 and resp.json()["converted"] == 0
        assert errors[0].startswith("Ruta inválida") and errors[1].startswith("Archivo no encontrado")
        assert errors[2].startswith("Formato no convertible")
        assert (env.stage / "dato.xyz").exists()

    async def test_missing_stage_folder_is_400_and_bad_action_is_422(self, env):
        assert (await self._post(env, [{"rel_path": "x", "action": "explode"}])).status_code == 422
        env.stage.rmdir()
        assert (await self._post(env, [])).status_code == 400


class TestFilePreview:
    async def test_serves_an_image_with_its_media_type(self, env):
        (env.stage / "foto.png").write_bytes(_PNG)
        resp = await env.client.get("/api/pipeline/file-preview", params={**env.ids, "rel_path": "foto.png"})
        assert resp.status_code == 200 and resp.headers["content-type"] == "image/png"
        assert resp.content == _PNG

    async def test_unknown_extension_falls_back_to_octet_stream(self, env):
        (env.stage / "datos.bin").write_bytes(b"\x00\x01")
        resp = await env.client.get("/api/pipeline/file-preview", params={**env.ids, "rel_path": "datos.bin"})
        assert resp.headers["content-type"] == "application/octet-stream"

    async def test_rejects_escaping_and_missing_paths(self, env):
        escaping = await env.client.get("/api/pipeline/file-preview", params={**env.ids, "rel_path": "../../x"})
        assert escaping.status_code == 400
        missing = await env.client.get("/api/pipeline/file-preview", params={**env.ids, "rel_path": "nada.png"})
        assert missing.status_code == 404


class TestAuditZip:
    async def test_zips_the_audit_folder_with_a_safe_file_name(self, env):
        (env.audit / "EPS").mkdir()
        (env.audit / "EPS" / "a.pdf").write_bytes(b"%PDF-1.4")

        resp = await env.client.get("/api/pipeline/stream-audit-zip", params=env.ids)

        assert resp.status_code == 200
        assert 'filename="PIPE_2024_01.zip"' in resp.headers["content-disposition"]
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            assert [n.replace("\\", "/") for n in zf.namelist()] == ["EPS/a.pdf"]

    async def test_empty_or_missing_audit_folder_is_404(self, env):
        assert (await env.client.get("/api/pipeline/stream-audit-zip", params=env.ids)).status_code == 404
        env.audit.rmdir()
        assert (await env.client.get("/api/pipeline/stream-audit-zip", params=env.ids)).status_code == 404


class TestLoadDriveZip:
    async def test_extracts_into_drive(self, env):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("FAC1/a.pdf", b"%PDF-1.4")
        resp = await env.client.post(
            "/api/pipeline/load-drive-zip",
            data={k: str(v) for k, v in env.ids.items()},
            files={"file": ("drive.zip", buf.getvalue(), "application/zip")},
        )
        assert resp.status_code == 200 and resp.json()["extracted"] == 1
        assert [p.name for p in env.drive.rglob("*.pdf")] == ["a.pdf"]

    async def test_a_zip_slip_member_is_a_400(self, env):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("a/../../../evil.txt", b"x")
        resp = await env.client.post(
            "/api/pipeline/load-drive-zip",
            data={k: str(v) for k, v in env.ids.items()},
            files={"file": ("evil.zip", buf.getvalue(), "application/zip")},
        )
        assert resp.status_code == 400
        assert not (env.drive.parent.parent / "evil.txt").exists()
