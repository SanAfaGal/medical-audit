"""HTTP tests for /api/explorer through the real app (decorators, dependencies, serialization) — needs PostgreSQL."""

from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import patch

import fitz
import pytest

pytestmark = pytest.mark.db


def _pdf_bytes(pages: int = 1, text: str = "hola") -> bytes:
    doc = fitz.open()
    for n in range(pages):
        doc.new_page().insert_text((36, 60), f"{text} {n + 1}", fontsize=10)
    data = doc.tobytes()
    doc.close()
    return data


@pytest.fixture
async def env(client, tmp_path):
    """A period whose STAGE folder holds a.pdf (3 pages) and a subfolder; ids ready for the requests."""
    with patch("app.paths.audit_data_root", tmp_path):
        inst = (
            await client.post("/api/institutions", json={"name": "EXP", "display_name": "x", "nit": "820000001"})
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
        stage = tmp_path / "EXP" / "2024-01" / "STAGE"
        (stage / "carpeta").mkdir()
        (stage / "a.pdf").write_bytes(_pdf_bytes(3))
        ids = {"institution_id": inst["id"], "period_id": period["id"]}
        yield type("Env", (), {"ids": ids, "stage": stage, "client": client})


def _q(env, **extra):
    return {**env.ids, **extra}


class TestBrowse:
    async def test_list_orders_folders_first_and_flags_pdfs(self, env):
        resp = await env.client.get("/api/explorer/list", params=_q(env, root="STAGE"))

        assert resp.status_code == 200
        entries = resp.json()["entries"]
        assert [(e["name"], e["is_dir"]) for e in entries] == [("carpeta", True), ("a.pdf", False)]
        assert entries[1]["is_pdf"] is True and entries[1]["path"] == "STAGE/a.pdf"

    async def test_list_of_a_missing_folder_is_empty_and_of_a_file_is_400(self, env):
        assert (await env.client.get("/api/explorer/list", params=_q(env, root="STAGE", path="nada"))).json()[
            "entries"
        ] == []
        resp = await env.client.get("/api/explorer/list", params=_q(env, root="STAGE", path="a.pdf"))
        assert resp.status_code == 400 and resp.json()["detail"] == "La ruta no es un directorio"

    async def test_path_traversal_is_a_400_with_a_message(self, env):
        resp = await env.client.get("/api/explorer/list", params=_q(env, root="STAGE", path="../../.."))
        assert resp.status_code == 400
        assert "fuera del directorio" in resp.json()["detail"]

    async def test_period_of_another_institution_is_404(self, env):
        resp = await env.client.get(
            "/api/explorer/list", params={"institution_id": 999999, "period_id": env.ids["period_id"]}
        )
        assert resp.status_code == 404

    async def test_search_finds_by_name_fragment(self, env):
        resp = await env.client.get("/api/explorer/search", params=_q(env, root="STAGE", q="CARP"))
        assert [e["name"] for e in resp.json()["entries"]] == ["carpeta"]

    async def test_serve_file_streams_the_pdf_and_refuses_other_types(self, env):
        (env.stage / "nota.txt").write_text("x")
        ok = await env.client.get("/api/explorer/file", params=_q(env, path="STAGE/a.pdf"))
        assert ok.status_code == 200 and ok.headers["content-type"] == "application/pdf"
        assert ok.content.startswith(b"%PDF")
        bad = await env.client.get("/api/explorer/file", params=_q(env, path="STAGE/nota.txt"))
        assert bad.status_code == 400
        assert (await env.client.get("/api/explorer/file", params=_q(env, path="STAGE/no.pdf"))).status_code == 404

    async def test_thumbnails_one_png_per_page(self, env):
        resp = await env.client.get("/api/explorer/thumbnails", params=_q(env, path="STAGE/a.pdf"))
        assert resp.status_code == 200 and resp.json()["count"] == 3


class TestFileActions:
    async def test_mkdir_rename_move_copy_delete(self, env):
        post = env.client.post

        made = await post("/api/explorer/mkdir", json={**env.ids, "path": "STAGE", "name": "nueva"})
        assert made.status_code == 200 and (env.stage / "nueva").is_dir()
        assert (
            await post("/api/explorer/mkdir", json={**env.ids, "path": "STAGE", "name": "nueva"})
        ).status_code == 400

        renamed = await post("/api/explorer/rename", json={**env.ids, "path": "STAGE/a.pdf", "new_name": "b.pdf"})
        assert renamed.json()["message"] == "Renombrado a b.pdf" and (env.stage / "b.pdf").exists()
        wrong_ext = await post("/api/explorer/rename", json={**env.ids, "path": "STAGE/b.pdf", "new_name": "b.txt"})
        assert wrong_ext.status_code == 400

        moved = await post("/api/explorer/move", json={**env.ids, "src": "STAGE/b.pdf", "dst_folder": "STAGE/nueva"})
        assert moved.status_code == 200 and (env.stage / "nueva" / "b.pdf").exists()

        copied = await post(
            "/api/explorer/copy", json={**env.ids, "src": "STAGE/nueva/b.pdf", "dst_folder": "STAGE/carpeta"}
        )
        assert copied.status_code == 200 and (env.stage / "carpeta" / "b.pdf").exists()
        same_place = await post(
            "/api/explorer/copy", json={**env.ids, "src": "STAGE/nueva/b.pdf", "dst_folder": "STAGE/nueva"}
        )
        assert same_place.status_code == 400

        deleted = await post("/api/explorer/delete", json={**env.ids, "path": "STAGE/nueva"})
        assert deleted.status_code == 200 and not (env.stage / "nueva").exists()
        assert (await post("/api/explorer/delete", json={**env.ids, "path": "STAGE/nueva"})).status_code == 404

    async def test_the_period_root_cannot_be_deleted(self, env):
        resp = await env.client.post("/api/explorer/delete", json={**env.ids, "path": ""})
        assert resp.status_code == 400 and "raíz" in resp.json()["detail"]

    async def test_delete_batch_reports_each_failure(self, env):
        resp = await env.client.post(
            "/api/explorer/delete-batch", json={**env.ids, "paths": ["STAGE/carpeta", "STAGE/nada"]}
        )
        body = resp.json()
        assert body["deleted"] == ["STAGE/carpeta"]
        assert len(body["errors"]) == 1 and "STAGE/nada" in body["errors"][0]


class TestPdfTools:
    async def test_merge_split_reorder(self, env):
        post = env.client.post
        (env.stage / "c.pdf").write_bytes(_pdf_bytes(2))

        merged = await post(
            "/api/explorer/merge", json={**env.ids, "paths": ["STAGE/a.pdf", "STAGE/c.pdf"], "output_name": "todo"}
        )
        assert merged.json()["message"] == "PDFs unidos en todo.pdf"
        with fitz.open(env.stage / "todo.pdf") as doc:
            assert doc.page_count == 5
        assert (
            await post("/api/explorer/merge", json={**env.ids, "paths": ["STAGE/a.pdf"], "output_name": "x"})
        ).status_code == 400

        preview = await env.client.get(
            "/api/explorer/split-preview", params=_q(env, path="STAGE/a.pdf", ranges="1-2, 3")
        )
        assert preview.json()["groups"] == [[0, 1], [2]]
        split = await post("/api/explorer/split", json={**env.ids, "path": "STAGE/a.pdf", "ranges": "1-2, 3"})
        assert split.json()["message"] == "PDF dividido en 2 archivo(s)"
        assert (env.stage / "a_parte001.pdf").exists() and (env.stage / "a_parte002.pdf").exists()
        assert (
            await post("/api/explorer/split", json={**env.ids, "path": "STAGE/a.pdf", "ranges": "abc"})
        ).status_code == 400

        reordered = await post("/api/explorer/reorder", json={**env.ids, "path": "STAGE/a.pdf", "page_order": [2, 0]})
        assert "2 página(s) (1 eliminada(s))" in reordered.json()["message"]
        with fitz.open(env.stage / "a.pdf") as doc:
            assert doc.page_count == 2
        bad_order = await post("/api/explorer/reorder", json={**env.ids, "path": "STAGE/a.pdf", "page_order": [0, 0]})
        assert bad_order.status_code == 400 and "repetidas" in bad_order.json()["detail"]


class TestTransfer:
    async def test_upload_keeps_pdfs_and_skips_the_rest(self, env):
        resp = await env.client.post(
            "/api/explorer/upload",
            data={**{k: str(v) for k, v in env.ids.items()}, "path": "STAGE/carpeta"},
            files=[
                ("files", ("nuevo.pdf", _pdf_bytes(1), "application/pdf")),
                ("files", ("foto.jpg", b"\xff\xd8\xff", "image/jpeg")),
            ],
        )
        body = resp.json()
        assert body["uploaded"] == ["STAGE/carpeta/nuevo.pdf"]
        assert body["skipped"] == ["foto.jpg"] and body["non_pdf_count"] == 1
        assert (env.stage / "carpeta" / "nuevo.pdf").exists()

    async def test_upload_of_a_folder_refuses_a_name_clash_before_writing(self, env):
        resp = await env.client.post(
            "/api/explorer/upload",
            data={
                **{k: str(v) for k, v in env.ids.items()},
                "path": "STAGE",
                "relative_paths_json": json.dumps(["carpeta/x.pdf"]),
            },
            files=[("files", ("x.pdf", _pdf_bytes(1), "application/pdf"))],
        )
        assert resp.status_code == 409 and not (env.stage / "carpeta" / "x.pdf").exists()

    async def test_download_file_and_zip(self, env):
        single = await env.client.get("/api/explorer/download-file", params=_q(env, rel_path="STAGE/a.pdf"))
        assert single.status_code == 200 and 'filename="a.pdf"' in single.headers["content-disposition"]

        archive = await env.client.get(
            "/api/explorer/download-zip", params={**env.ids, "rel_paths": ["STAGE/a.pdf", "STAGE/carpeta"]}
        )
        assert archive.status_code == 200
        with zipfile.ZipFile(io.BytesIO(archive.content)) as zf:
            assert "a.pdf" in zf.namelist()


class TestRoutesStillDocumented:
    async def test_every_explorer_route_is_in_the_openapi_schema(self, client):
        schema = (await client.get("/openapi.json")).json()
        paths = {p for p in schema.get("paths", {}) if p.startswith("/api/explorer/")}
        expected = {
            "list",
            "search",
            "file",
            "thumbnails",
            "rename",
            "move",
            "copy",
            "delete",
            "delete-batch",
            "mkdir",
            "merge",
            "split-preview",
            "split",
            "reorder",
            "upload",
            "download-file",
            "download-zip",
            "console-hide",
        }
        assert {p.rsplit("/", 1)[1] for p in paths} == expected
