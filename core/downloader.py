"""Automated invoice download from the SIHOS hospital billing portal."""

import logging
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from config.settings import Settings
from core.helpers import read_lines_from_file

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_INVOICE_PAGE_FORMAT: str = "A4"
_LOGIN_BUTTON_TEXT: str = "INGRESAR"
_LOGIN_URL_PATTERN: str = "**/index.php"
_USERNAME_SELECTOR: str = 'input[name="TxtLogi"]'
_PASSWORD_SELECTOR: str = 'input[name="TxtPswd"]'


class SihosDownloader:
    """Downloads invoices from the SIHOS web portal using a browser session.

    Credentials and base URL are derived from the active hospital set in
    the environment via :class:`~config.settings.Settings`.
    """

    def __init__(self, output_dir: Path | None = None) -> None:
        self._base_url: str = Settings.sihos_base_url
        self._user: str = Settings.sihos_user
        self._password: str = Settings.sihos_password
        self._hospital_nit: str = Settings.hospital_nit
        self._invoice_prefix: str = Settings.invoice_prefix
        self._invoice_id_prefix: str = Settings.invoice_identifier_prefix
        self._invoice_doc_code: str = Settings.sihos_invoice_doc_code
        self._output_dir: Path = (
            output_dir if output_dir is not None else Settings.staging_dir
        )

    def run_from_list(self, invoice_numbers: list[str]) -> None:
        """Download invoices from a list of invoice numbers.

        Args:
            invoice_numbers: List of invoice number strings.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._download_invoices(invoice_numbers)

    def run(self, list_path: str | Path) -> None:
        """Download invoices listed in a text file.

        Args:
            list_path: Path to a text file containing one invoice number per line.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        invoice_list = read_lines_from_file(list_path)
        self._download_invoices(invoice_list)

    def _download_invoices(self, invoice_list: list[str]) -> None:
        """Open a browser session and download each invoice.

        Args:
            invoice_list: List of invoice number strings.
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            try:
                logger.info("Logging into SIHOS as user %s", self._user)
                page.goto(self._base_url)
                page.fill(_USERNAME_SELECTOR, self._user)
                page.fill(_PASSWORD_SELECTOR, self._password)
                page.click("text=%s" % _LOGIN_BUTTON_TEXT)
                page.wait_for_url(_LOGIN_URL_PATTERN)
            except (PlaywrightTimeoutError, PlaywrightError) as exc:
                logger.error("SIHOS login failed: %s", exc)
                browser.close()
                return

            for invoice_number in invoice_list:
                url = (
                    "%s/modulos/facturacion/imprifact.php"
                    "?CodiDocu=%s&NumeDocu=%s&MostSubCeCo=1"
                    % (
                        self._base_url.rstrip("/"),
                        self._invoice_doc_code,
                        invoice_number,
                    )
                )
                out_path = self._output_dir / (
                    "%s_%s_%s%s.pdf"
                    % (
                        self._invoice_prefix,
                        self._hospital_nit,
                        self._invoice_id_prefix,
                        invoice_number,
                    )
                )
                try:
                    page.goto(url)
                    page.pdf(path=str(out_path), format=_INVOICE_PAGE_FORMAT)
                    logger.info(
                        "Downloaded invoice %s to %s", invoice_number, out_path
                    )
                except (PlaywrightTimeoutError, PlaywrightError) as exc:
                    logger.error(
                        "Failed to download invoice %s: %s", invoice_number, exc
                    )

            browser.close()
