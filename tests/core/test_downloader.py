"""Tests for SihosDownloader — login failure must be observable, not silent."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from core.downloader import SihosDownloader, SihosLoginError


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

        with patch("core.downloader.async_playwright", return_value=fake_pw_cm):
            with pytest.raises(SihosLoginError):
                async with downloader._browser_session():
                    pass

        fake_browser.close.assert_awaited()
