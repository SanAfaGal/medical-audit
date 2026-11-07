"""Tests for shared response helpers: safe Content-Disposition and temp-file cleanup."""

from __future__ import annotations

import os
import time

from app.routers.responses import content_disposition, temp_file_response
from app.services.archive import sweep_stale_temp_zips


class TestContentDisposition:
    def test_plain_ascii_name(self):
        value = content_disposition("factura.pdf")
        assert value.startswith('attachment; filename="factura.pdf"')
        assert "filename*=UTF-8''factura.pdf" in value

    def test_quotes_newlines_and_separators_cannot_break_out_of_the_header(self):
        value = content_disposition('a"b\r\nSet-Cookie: x=1;c/d.pdf')
        assert "\r" not in value and "\n" not in value
        head = value.split("; filename*=")[0]
        assert head.count('"') == 2  # only the delimiters of the quoted ASCII fallback

    def test_non_ascii_names_keep_the_real_name_percent_encoded(self):
        value = content_disposition("Niño-Clínica.pdf")
        assert 'filename="Ni_o-Cl_nica.pdf"' in value
        assert "filename*=UTF-8''Ni%C3%B1o-Cl%C3%ADnica.pdf" in value

    def test_inline_variant(self):
        assert content_disposition("a.pdf", inline=True).startswith("inline;")


class TestTempFileResponse:
    async def _collect(self, response) -> bytes:
        return b"".join([chunk async for chunk in response.body_iterator])

    async def test_streams_content_then_deletes_the_file(self, tmp_path):
        f = tmp_path / "audit_x.zip"
        f.write_bytes(b"z" * 3_000_000)
        response = temp_file_response(f, "pkg.zip", "application/zip")

        assert response.headers["content-length"] == "3000000"
        assert len(await self._collect(response)) == 3_000_000
        assert not f.exists()

    async def test_deletes_the_file_even_if_the_client_disconnects_midway(self, tmp_path):
        f = tmp_path / "audit_y.zip"
        f.write_bytes(b"z" * 5_000_000)
        response = temp_file_response(f, "pkg.zip", "application/zip")

        gen = response.body_iterator
        await gen.__anext__()  # client reads one chunk, then goes away
        assert f.exists()
        await gen.aclose()

        assert not f.exists()


class TestSweepStaleTempZips:
    def test_removes_only_old_audit_zips(self, tmp_path):
        old = tmp_path / "audit_old.zip"
        fresh = tmp_path / "audit_fresh.zip"
        other = tmp_path / "keep.zip"
        for p in (old, fresh, other):
            p.write_bytes(b"x")
        past = time.time() - 7200
        os.utime(old, (past, past))
        os.utime(other, (past, past))

        assert sweep_stale_temp_zips(max_age_seconds=3600, directory=tmp_path) == 1
        assert not old.exists() and fresh.exists() and other.exists()
