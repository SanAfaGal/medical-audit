"""Automated invoice download from the SIHOS hospital billing portal."""

import asyncio
import logging
import sys
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import Page, Route, async_playwright

from app.core.helpers import EventFn, ProgressFn, read_lines_from_file
from app.core.helpers import emit_event as _emit
from app.core.reader import DocumentReader
from app.core.urlcheck import is_blocked_request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_INVOICE_PAGE_FORMAT: str = "A4"
_LOGIN_BUTTON_TEXT: str = "INGRESAR"
_LOGIN_URL_PATTERN: str = "**/index.php"
_USERNAME_SELECTOR: str = 'input[name="TxtLogi"]'
_PASSWORD_SELECTOR: str = 'input[name="TxtPswd"]'

# A PDF is printed from an HTML page, so it can come out blank if the portal is still rendering.
_MAX_PRINT_ATTEMPTS: int = 3
_RETRY_DELAYS: tuple[float, ...] = (2.0, 5.0)  # seconds to wait before attempt 2 and 3
_NETWORK_IDLE_TIMEOUT_MS: int = 8000


class PortalResponseError(RuntimeError):
    """The portal answered with an HTTP error instead of the printable page."""


class BlankPdfError(RuntimeError):
    """The printed PDF had no visible content on every attempt."""


async def _print_page_to_pdf(page: Page, url: str, out_path: Path) -> None:
    """Open ``url`` and save it as a PDF at ``out_path``, retrying when the result is blank.

    Printing goes to a ``.part`` file first, so a previously good ``out_path`` is only replaced by a
    PDF that has content. Raises :class:`PortalResponseError` on an HTTP error and
    :class:`BlankPdfError` when every attempt came out blank.
    """
    part_path = out_path.with_name(out_path.name + ".part")
    try:
        for attempt in range(1, _MAX_PRINT_ATTEMPTS + 1):
            response = await page.goto(url)
            if response is not None and not response.ok:
                raise PortalResponseError(f"El portal respondió HTTP {response.status}")
            try:
                # Let late scripts/images finish. Portals that keep polling never go idle: carry on.
                await page.wait_for_load_state("networkidle", timeout=_NETWORK_IDLE_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                pass
            await page.pdf(path=str(part_path), format=_INVOICE_PAGE_FORMAT, print_background=True)
            if not DocumentReader.is_blank(part_path):
                part_path.replace(out_path)
                if attempt > 1:
                    logger.info("PDF %s had content on attempt %d", out_path.name, attempt)
                return
            logger.warning("Blank PDF for %s (attempt %d/%d)", out_path.name, attempt, _MAX_PRINT_ATTEMPTS)
            if attempt < _MAX_PRINT_ATTEMPTS:
                await asyncio.sleep(_RETRY_DELAYS[attempt - 1])
        raise BlankPdfError(f"PDF en blanco tras {_MAX_PRINT_ATTEMPTS} intentos")
    finally:
        part_path.unlink(missing_ok=True)


def _report_failures(event_fn: EventFn | None, failed: list[str], total: int) -> None:
    """Emit one closing warning that names every item that did not end up with a good PDF."""
    if failed:
        _emit(event_fn, "warning", detail=f"{len(failed)} de {total} no se descargaron bien: {', '.join(failed)}")


def _first_line(exc: Exception, limit: int = 200) -> str:
    """First line of an exception message (Playwright appends a long multi-line call log)."""
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
    return text[:limit]


async def _recover_page(page: Page) -> Page:
    """Return a clean page after a failed navigation.

    After a network error (e.g. ERR_EMPTY_RESPONSE) Chromium leaves the page on an internal
    error document, and every following ``goto`` on it fails with "interrupted by another
    navigation" (even navigating to about:blank). A fresh page in the same context keeps the
    login cookies, so one bad invoice doesn't cascade into the rest of the batch.
    Falls back to the original page if a new one can't be opened.
    """
    try:
        fresh = await page.context.new_page()
    except (PlaywrightTimeoutError, PlaywrightError):
        logger.warning("Could not open a fresh page after a failed download")
        return page
    try:
        await page.close()
    except (PlaywrightTimeoutError, PlaywrightError):
        logger.warning("Could not close the broken page")
    return fresh


class SihosLoginError(RuntimeError):
    """Raised when the SIHOS portal login fails (bad credentials, portal down, layout change)."""


class SihosDownloader:
    """Downloads invoices from the SIHOS web portal using a browser session.

    Args:
        user: SIHOS portal username.
        password: SIHOS portal password.
        base_url: Base URL of the SIHOS portal.
        hospital_nit: NIT number of the hospital.
        invoice_prefix: Document type prefix for invoices (e.g. ``"FE"``).
        invoice_id_prefix: Invoice identifier prefix (e.g. ``"FA"``).
        invoice_doc_code: SIHOS document code for invoices.
        output_dir: Directory where downloaded PDFs are saved.
    """

    def __init__(
        self,
        user: str,
        password: str,
        base_url: str,
        hospital_nit: str,
        invoice_prefix: str,
        invoice_id_prefix: str,
        invoice_doc_code: str,
        output_dir: Path,
    ) -> None:
        self._user: str = user
        self._password: str = password
        self._base_url: str = base_url
        self._hospital_nit: str = hospital_nit
        self._invoice_prefix: str = invoice_prefix
        self._invoice_id_prefix: str = invoice_id_prefix
        self._invoice_doc_code: str = invoice_doc_code
        self._output_dir: Path = output_dir

    def run_from_list(
        self,
        invoice_numbers: list[str],
        progress_fn: ProgressFn | None = None,
        event_fn: EventFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Download invoices from a list of invoice numbers.

        Args:
            progress_fn: Optional ``(i, total, invoice_number)`` called after each invoice.
            event_fn: Optional ``(kind, i, total, item, detail)`` progress callback (see the
                event contract in ``app.core.helpers``).
            cancel_event: Optional ``threading.Event``; when set, the loop stops before the next item.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._download_invoices(invoice_numbers, progress_fn, event_fn, cancel_event)

    def run(self, list_path: str | Path) -> None:
        """Download invoices listed in a text file."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        invoice_list = read_lines_from_file(list_path)
        self._download_invoices(invoice_list)

    def run_medication_sheets(
        self,
        targets: list[tuple[str, str, str, str]],
        file_prefix: str,
        progress_fn: ProgressFn | None = None,
        event_fn: EventFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Download medication sheet PDFs for the given targets.

        Args:
            targets: List of ``(invoice_number, admission, id_type, id_number)`` tuples.
            file_prefix: Prefix used in the output filename (e.g. the doc type prefix).
            progress_fn: Optional callable ``(i, total, invoice_number)`` called after each download.
            event_fn: See :meth:`run_from_list`.
            cancel_event: See :meth:`run_from_list`.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        loop = asyncio.ProactorEventLoop() if sys.platform == "win32" else asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                self._async_download_medication_sheets(targets, file_prefix, progress_fn, event_fn, cancel_event)
            )
        finally:
            loop.close()

    def _download_invoices(
        self,
        invoice_list: list[str],
        progress_fn: ProgressFn | None = None,
        event_fn: EventFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Open a browser session and download each invoice."""
        loop = asyncio.ProactorEventLoop() if sys.platform == "win32" else asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_download_invoices(invoice_list, progress_fn, event_fn, cancel_event))
        finally:
            loop.close()

    @asynccontextmanager
    async def _browser_session(self, event_fn: EventFn | None = None) -> AsyncIterator[Page]:
        """Async context manager that yields an authenticated SIHOS page.

        Raises:
            SihosLoginError: if login fails. The browser is always closed on exit.
        """
        _emit(event_fn, "status", detail="Iniciando navegador...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage"],  # evita crashes en Docker por /dev/shm limitado
            )
            context = await browser.new_context()

            resolved: dict[str, bool] = {}

            async def _guard(route: Route) -> None:
                # Redirects or embedded resources must not reach loopback/metadata addresses (SSRF),
                # whether given as an IP in any notation or as a name that resolves to one.
                if await is_blocked_request(route.request.url, resolved):
                    logger.warning("Blocked request to %s", route.request.url)
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _guard)
            page = await context.new_page()
            try:
                logger.info("Logging into SIHOS as user %s", self._user)
                _emit(event_fn, "status", detail="Iniciando sesión en SIHOS...")
                await page.goto(self._base_url)
                await page.fill(_USERNAME_SELECTOR, self._user)
                await page.fill(_PASSWORD_SELECTOR, self._password)
                await page.click(f"text={_LOGIN_BUTTON_TEXT}")
                await page.wait_for_url(_LOGIN_URL_PATTERN)
            except (PlaywrightTimeoutError, PlaywrightError) as exc:
                logger.error("SIHOS login failed: %s", exc)
                await browser.close()
                raise SihosLoginError(f"No se pudo iniciar sesión en SIHOS: {exc}") from exc
            _emit(event_fn, "status", detail="Sesión SIHOS iniciada")
            try:
                yield page
            finally:
                await browser.close()

    async def _async_download_invoices(
        self,
        invoice_list: list[str],
        progress_fn: ProgressFn | None = None,
        event_fn: EventFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Async implementation of the browser-based invoice download."""
        total = len(invoice_list)
        failed: list[str] = []
        async with self._browser_session(event_fn) as session_page:
            page = session_page  # _recover_page may swap it for a fresh one
            for i, invoice_number in enumerate(invoice_list, 1):
                if cancel_event is not None and cancel_event.is_set():
                    _emit(event_fn, "cancelled", i, total)
                    break
                _emit(event_fn, "item_start", i, total, invoice_number)
                url = "{}/modulos/facturacion/imprifact.php?CodiDocu={}&NumeDocu={}&MostSubCeCo=1".format(
                    self._base_url.rstrip("/"),
                    self._invoice_doc_code,
                    invoice_number,
                )
                invoice_folder = self._output_dir / f"{self._invoice_id_prefix}{invoice_number}"
                invoice_folder.mkdir(parents=True, exist_ok=True)
                out_path = invoice_folder / (
                    f"{self._invoice_prefix}_{self._hospital_nit}_{self._invoice_id_prefix}{invoice_number}.pdf"
                )
                try:
                    await _print_page_to_pdf(page, url, out_path)
                    logger.info("Downloaded invoice %s to %s", invoice_number, out_path)
                    _emit(event_fn, "item_ok", i, total, invoice_number, "descargada")
                except (BlankPdfError, PortalResponseError) as exc:
                    logger.error("Failed to download invoice %s: %s", invoice_number, exc)
                    failed.append(invoice_number)
                    _emit(event_fn, "item_error", i, total, invoice_number, str(exc))
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    logger.error("Failed to download invoice %s: %s", invoice_number, exc)
                    failed.append(invoice_number)
                    _emit(event_fn, "item_error", i, total, invoice_number, _first_line(exc))
                    page = await _recover_page(page)
                if progress_fn:
                    progress_fn(i, total, invoice_number)
            _report_failures(event_fn, failed, total)

    async def _async_download_medication_sheets(
        self,
        targets: list[tuple[str, str, str, str]],
        file_prefix: str,
        progress_fn: ProgressFn | None = None,
        event_fn: EventFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Async implementation of medication sheet download."""
        total = len(targets)
        failed: list[str] = []
        async with self._browser_session(event_fn) as session_page:
            page = session_page  # _recover_page may swap it for a fresh one
            for i, (invoice_number, admission, id_type, id_number) in enumerate(targets, 1):
                if cancel_event is not None and cancel_event.is_set():
                    _emit(event_fn, "cancelled", i, total)
                    break
                _emit(event_fn, "item_start", i, total, invoice_number)
                url = (
                    "{}/modulos/comun/medicamentos/impriconso.php?ConsAdmi={}&TipoDocu={}&NumeUsua={}&SinModu=1".format(
                        self._base_url.rstrip("/"), admission, id_type, id_number
                    )
                )
                invoice_folder = self._output_dir / f"{self._invoice_id_prefix}{invoice_number}"
                invoice_folder.mkdir(parents=True, exist_ok=True)
                out_path = invoice_folder / (
                    f"{file_prefix}_{self._hospital_nit}_{self._invoice_id_prefix}{invoice_number}.pdf"
                )
                try:
                    await _print_page_to_pdf(page, url, out_path)
                    logger.info("Downloaded %s → %s", invoice_number, out_path)
                    _emit(event_fn, "item_ok", i, total, invoice_number, "descargada")
                except (BlankPdfError, PortalResponseError) as exc:
                    logger.error("Failed %s: %s", invoice_number, exc)
                    failed.append(invoice_number)
                    _emit(event_fn, "item_error", i, total, invoice_number, str(exc))
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    logger.error("Failed %s: %s", invoice_number, exc)
                    failed.append(invoice_number)
                    _emit(event_fn, "item_error", i, total, invoice_number, _first_line(exc))
                    page = await _recover_page(page)
                if progress_fn:
                    progress_fn(i, total, invoice_number)
            _report_failures(event_fn, failed, total)
