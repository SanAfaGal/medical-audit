"""Regression tests for the audit P0 hardening: SSRF guard, Drive query escaping, archive limits."""

from __future__ import annotations

import io
import os
import socket
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import app.services.archive as archive
from app.core.drive import DriveSync, _q
from app.core.urlcheck import is_blocked_url, validate_portal_url
from app.schemas.institution import InstitutionCreate, InstitutionUpdate
from app.services.archive import ArchiveError, extract_archive
from app.services.pipeline_runner import _INVOICE_NUMBER_RE, _sihos_url_error


class TestPortalUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "http://192.168.1.20/sihos",  # LAN servers are legitimate
            "https://sihos.hospital.org",
            "http://10.0.0.5:8080/",
            "http://host.docker.internal:9000",
        ],
    )
    def test_accepts_normal_portals(self, url):
        assert validate_portal_url(url) == url

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost:8000/api/institutions",
            "http://127.0.0.1/",
            "http://127.1.2.3/",
            "http://169.254.169.254/latest/meta-data/",  # cloud metadata
            "http://metadata.google.internal/",
            "http://[::1]/",
            "http://[::ffff:127.0.0.1]/",
            "http://0.0.0.0/",
            "ftp://example.com/",
            "file:///etc/passwd",
            "javascript:alert(1)",
            "http://user:pass@example.com/",
            "http:///nohost",
            "",
        ],
    )
    def test_rejects_dangerous_urls(self, url):
        with pytest.raises(ValueError):
            validate_portal_url(url)

    def test_resolve_catches_names_pointing_at_loopback(self, monkeypatch):
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("127.0.0.1", 80))])
        with pytest.raises(ValueError, match="resuelve"):
            validate_portal_url("http://innocent.example.com", resolve=True)

    def test_resolve_allows_private_addresses(self, monkeypatch):
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("10.1.2.3", 80))])
        assert validate_portal_url("http://sihos.lan", resolve=True)

    def test_is_blocked_url_only_looks_at_http_hosts(self):
        assert is_blocked_url("http://169.254.169.254/x")
        assert not is_blocked_url("about:blank")
        assert not is_blocked_url("data:text/html,hola")
        assert not is_blocked_url("https://sihos.example.com/x")

    def test_runner_blocks_unsafe_stored_url(self, monkeypatch):
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("10.1.2.3", 80))])
        ok = SimpleNamespace(sihos_base_url="http://sihos.lan")
        bad = SimpleNamespace(sihos_base_url="http://localhost:8000")
        empty = SimpleNamespace(sihos_base_url=None)
        assert _sihos_url_error(ok) is None
        assert "no permitida" in (_sihos_url_error(bad) or "")
        assert "no tiene URL" in (_sihos_url_error(empty) or "")


class TestInstitutionSchemaUrl:
    def test_create_rejects_bad_url(self):
        with pytest.raises(ValidationError):
            InstitutionCreate(name="H1", display_name="H", nit="1", sihos_base_url="http://169.254.169.254/")

    def test_update_rejects_bad_url(self):
        with pytest.raises(ValidationError):
            InstitutionUpdate(sihos_base_url="file:///etc/passwd")

    def test_empty_and_none_are_allowed(self):
        assert InstitutionCreate(name="H1", display_name="H", nit="1").sihos_base_url is None
        assert InstitutionUpdate(sihos_base_url="").sihos_base_url == ""
        assert InstitutionCreate(name="H1", display_name="H", nit="1", sihos_base_url="http://10.0.0.1").sihos_base_url


class _CapturingService:
    """Stands in for the Drive API client and records the queries sent."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def files(self):
        return self

    def list(self, q, **kwargs):
        self.queries.append(q)
        return SimpleNamespace(execute=lambda: {"files": []})


class _QueryDrive(DriveSync):
    def __init__(self) -> None:  # noqa: D107 - no credentials needed
        self.service = _CapturingService()


class TestDriveQueryEscaping:
    def test_escape_quotes_and_backslashes(self):
        assert _q("a'b") == "a\\'b"
        assert _q("a\\b") == "a\\\\b"
        assert _q("plain-123") == "plain-123"

    def test_injection_attempt_stays_inside_the_string_literal(self):
        drive = _QueryDrive()
        drive._batch_search_folders(["1' or name contains '"])
        (query,) = drive.service.queries
        # every quote coming from the value must be escaped, so the OR can't split the query
        assert "name contains '1\\' or name contains \\''" in query

    def test_invoice_number_filter(self):
        for good in ("12345", "FE-12345", "FE12345.1", "A_9"):
            assert _INVOICE_NUMBER_RE.fullmatch(good)
        for bad in ("1' or '1'='1", "a b", "x;y", "", "a" * 51, "../x"):
            assert not _INVOICE_NUMBER_RE.fullmatch(bad)


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class TestArchive:
    def test_extracts_and_drops_single_wrapper_folder(self, tmp_path):
        data = _zip_bytes({"Drive/FE1/a.pdf": b"x", "Drive/FE2/b.pdf": b"y"})
        extracted = extract_archive(data, "carpetas.zip", tmp_path)
        assert sorted(extracted) == [str(Path("FE1/a.pdf")), str(Path("FE2/b.pdf"))]
        assert (tmp_path / "FE1" / "a.pdf").read_bytes() == b"x"

    def test_keeps_paths_when_several_top_level_folders(self, tmp_path):
        extract_archive(_zip_bytes({"FE1/a.pdf": b"x", "FE2/b.pdf": b"y"}), "x.zip", tmp_path)
        assert (tmp_path / "FE1" / "a.pdf").exists() and (tmp_path / "FE2" / "b.pdf").exists()

    def test_rejects_path_traversal(self, tmp_path):
        with pytest.raises(ArchiveError, match="insegura"):
            extract_archive(_zip_bytes({"a/../../evil.txt": b"x"}), "x.zip", tmp_path / "dest")
        assert not (tmp_path / "evil.txt").exists()

    def test_rejects_corrupt_zip_and_unknown_format(self, tmp_path):
        with pytest.raises(ArchiveError, match="dañado"):
            extract_archive(b"not a zip", "x.zip", tmp_path)
        with pytest.raises(ArchiveError, match="Formato no soportado"):
            extract_archive(b"x", "x.tar", tmp_path)

    def test_declared_size_limit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(archive, "MAX_UNCOMPRESSED_BYTES", 10)
        with pytest.raises(ArchiveError, match="demasiado grande"):
            extract_archive(_zip_bytes({"a/f.bin": b"x" * 100}), "x.zip", tmp_path)

    def test_member_count_limit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(archive, "MAX_MEMBERS", 2)
        with pytest.raises(ArchiveError, match="demasiados"):
            extract_archive(_zip_bytes({f"d{i}/f": b"x" for i in range(3)}), "x.zip", tmp_path)

    def test_symlinks_in_extracted_trees_are_rejected(self, tmp_path):
        target = tmp_path / "secret.txt"
        target.write_text("top secret")
        link = tmp_path / "tree" / "link"
        link.parent.mkdir()
        try:
            os.symlink(target, link)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not available on this platform/user")
        with pytest.raises(ArchiveError, match="enlace simbólico"):
            archive._reject_symlinks(tmp_path / "tree")
