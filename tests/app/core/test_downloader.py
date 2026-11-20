"""Tests for SihosDownloader — login failure must be observable, not silent."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fitz
import pytest
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from app.core.reader import DocumentReader
from app.core.downloader import (
    BlankPdfError,
    PortalResponseError,
    SihosDownloader,
    SihosLoginError,
    _print_page_to_pdf,
)


def _make_downloader(tmp_path) -> SihosDownloader:
    return SihosDownloader(
        user="u",
        password="p",
        base_url="http://example.test",
        hospital_nit="900123456",
        invoice_prefix="FEV",
        invoice_id_prefix="HSL",
        invoice_doc_code="1",
        output_dir=tmp_path,
    )


class TestBrowserSessionLoginFailure:
    async def test_login_timeout_raises_sihos_login_error(self, tmp_path):
        downloader = _make_downloader(tmp_path)

        fake_page = AsyncMock()
        fake_page.wait_for_url = AsyncMock(side_effect=PlaywrightTimeoutError("timeout"))
        fake_context = AsyncMock()
        fake_context.new_page = AsyncMock(return_value=fake_page)
        fake_browser = AsyncMock()
        fake_browser.new_context = AsyncMock(return_value=fake_context)
        fake_chromium = AsyncMock()
        fake_chromium.launch = AsyncMock(return_value=fake_browser)
        fake_pw_ctx = AsyncMock()
        fake_pw_ctx.chromium = fake_chromium
        fake_pw_cm = AsyncMock()
        fake_pw_cm.__aenter__ = AsyncMock(return_value=fake_pw_ctx)
        fake_pw_cm.__aexit__ = AsyncMock(return_value=False)

        with patch("app.core.downloader.async_playwright", return_value=fake_pw_cm):
            with pytest.raises(SihosLoginError):
                async with downloader._browser_session():
                    pass

        fake_browser.close.assert_awaited()


def _write_pdf(path: str, text: str | None) -> None:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((72, 72), text)
    doc.save(path)
    doc.close()


def _fake_page(texts: list[str | None], *, ok: bool = True, status: int = 200) -> AsyncMock:
    """A page whose successive ``pdf()`` calls write a PDF with the given texts (None = blank)."""
    contents = iter(texts)

    async def _pdf(path: str, **_kwargs) -> None:
        _write_pdf(path, next(contents))

    page = AsyncMock()
    page.goto = AsyncMock(return_value=MagicMock(ok=ok, status=status))
    page.pdf = AsyncMock(side_effect=_pdf)
    return page


@pytest.fixture
def no_sleep():
    with patch("app.core.downloader.asyncio.sleep", new=AsyncMock()) as sleep:
        yield sleep


class TestPrintPageToPdf:
    async def test_good_pdf_is_saved_on_first_attempt(self, tmp_path: Path, no_sleep):
        out = tmp_path / "f.pdf"
        page = _fake_page(["factura"])

        await _print_page_to_pdf(page, "http://x", out)

        assert page.pdf.await_count == 1
        assert out.exists()
        assert not out.with_name("f.pdf.part").exists()

    async def test_blank_then_good_retries_and_keeps_the_good_file(self, tmp_path: Path, no_sleep):
        out = tmp_path / "f.pdf"
        page = _fake_page([None, "factura"])

        await _print_page_to_pdf(page, "http://x", out)

        assert page.pdf.await_count == 2
        assert page.goto.await_count == 2
        no_sleep.assert_awaited_once()
        assert not DocumentReader.is_blank(out)

    async def test_always_blank_raises_and_leaves_no_file(self, tmp_path: Path, no_sleep):
        out = tmp_path / "f.pdf"
        page = _fake_page([None, None, None])

        with pytest.raises(BlankPdfError):
            await _print_page_to_pdf(page, "http://x", out)

        assert page.pdf.await_count == 3
        assert list(tmp_path.iterdir()) == []

    async def test_blank_result_does_not_overwrite_an_existing_good_file(self, tmp_path: Path, no_sleep):
        out = tmp_path / "f.pdf"
        _write_pdf(str(out), "bueno")
        page = _fake_page([None, None, None])

        with pytest.raises(BlankPdfError):
            await _print_page_to_pdf(page, "http://x", out)

        assert not DocumentReader.is_blank(out)

    async def test_http_error_fails_without_printing(self, tmp_path: Path, no_sleep):
        page = _fake_page(["x"], ok=False, status=500)

        with pytest.raises(PortalResponseError):
            await _print_page_to_pdf(page, "http://x", tmp_path / "f.pdf")

        page.pdf.assert_not_awaited()

    async def test_network_idle_timeout_is_tolerated(self, tmp_path: Path, no_sleep):
        page = _fake_page(["factura"])
        page.wait_for_load_state = AsyncMock(side_effect=PlaywrightTimeoutError("idle"))

        await _print_page_to_pdf(page, "http://x", tmp_path / "f.pdf")

        assert (tmp_path / "f.pdf").exists()


class TestDownloadSummary:
    async def test_failed_invoices_are_named_in_a_closing_warning(self, tmp_path: Path, no_sleep):
        downloader = _make_downloader(tmp_path)
        page = _fake_page(["ok", None, None, None])  # invoice 1 good, invoice 2 blank on all attempts
        events: list[tuple[str, str, str]] = []

        def _event(kind: str, i: int, total: int, item: str, detail: str) -> None:
            events.append((kind, item, detail))

        fake_cm = MagicMock()
        fake_cm.__aenter__ = AsyncMock(return_value=page)
        fake_cm.__aexit__ = AsyncMock(return_value=False)
        with patch.object(downloader, "_browser_session", return_value=fake_cm):
            await downloader._async_download_invoices(["1", "2"], event_fn=_event)

        kinds = [kind for kind, _, _ in events]
        assert kinds.count("item_ok") == 1
        assert kinds.count("item_error") == 1
        assert events[-1][0] == "warning"
        assert "1 de 2" in events[-1][2]
        assert "2" in events[-1][2]
