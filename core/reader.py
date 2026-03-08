"""PDF validity checking and text extraction using PyMuPDF (fitz) and pdfplumber."""

import logging
from pathlib import Path

import fitz
import pdfplumber

logger = logging.getLogger(__name__)

# Keywords that identify the service table header in Colombian healthcare invoices.
# A table row must match at least this many of them to be considered valid.
_SERVICE_HEADERS = {"item", "codigo", "nombre", "und", "fina", "cant", "unitario", "total"}
_MIN_HEADER_MATCHES = 4


def _is_service_header_row(row: list) -> bool:
    """Return True if the row looks like a service table header.

    Checks for at least ``_MIN_HEADER_MATCHES`` matches against
    ``_SERVICE_HEADERS``, case-insensitively and stripping whitespace.

    Args:
        row: List of cell values from pdfplumber (may contain None).

    Returns:
        True if the row contains enough service-table header keywords.
    """
    cells = {(cell or "").strip().lower() for cell in row}
    matches = sum(
        any(header in cell for cell in cells)
        for header in _SERVICE_HEADERS
    )
    return matches >= _MIN_HEADER_MATCHES


class DocumentReader:
    """Provides static helpers for opening and reading PDF documents."""

    @staticmethod
    def _can_open(file_path: Path) -> bool:
        """Return True if the PDF opens successfully and has at least one page.

        Args:
            file_path: Path to the PDF file.

        Returns:
            True if the file is a valid, readable PDF.
        """
        try:
            with fitz.open(file_path) as doc:
                return doc.page_count > 0
        except (fitz.FileDataError, OSError, RuntimeError):
            return False

    @staticmethod
    def _has_text_layer(file_path: Path) -> bool:
        """Return True if the PDF contains any readable text (no OCR needed).

        Args:
            file_path: Path to the PDF file.

        Returns:
            True if at least one page yields non-empty text.
        """
        try:
            with fitz.open(file_path) as doc:
                return any(page.get_text().strip() for page in doc)
        except (fitz.FileDataError, OSError, RuntimeError):
            return False

    @staticmethod
    def read_text(file_path: Path) -> str:
        """Extract all text from a PDF file.

        Args:
            file_path: Path to the PDF.

        Returns:
            Concatenated text from all pages, or an empty string on failure.
        """
        try:
            with fitz.open(file_path) as doc:
                return "".join(page.get_text() for page in doc)
        except (fitz.FileDataError, OSError, RuntimeError) as exc:
            logger.error("Error reading PDF %s: %s", file_path.name, exc)
            return ""

    @staticmethod
    def read_text_if_has_table(file_path: Path) -> str | None:
        """Extract full text from a PDF, but only if it contains a valid service table.

        Uses PyMuPDF (fitz) for fast text extraction (~5–15 ms/PDF vs ~500 ms for
        pdfplumber). Validates that the extracted text contains at least
        ``_MIN_HEADER_MATCHES`` service-table header keywords before returning.
        Returns ``None`` if no valid service table is detected, signalling that the
        invoice should be classified as DESCONOCIDO.

        Args:
            file_path: Path to the PDF.

        Returns:
            Full page text joined across all pages, or ``None`` if no service
            table header is detected.
        """
        try:
            with fitz.open(file_path) as doc:
                text = "".join(page.get_text() for page in doc)
            words = {w.strip().lower() for w in text.split()}
            matches = sum(any(h in w for w in words) for h in _SERVICE_HEADERS)
            return text if matches >= _MIN_HEADER_MATCHES else None
        except (fitz.FileDataError, OSError, RuntimeError) as exc:
            logger.error("Error reading PDF %s: %s", file_path.name, exc)
            return None

    @staticmethod
    def read_table_text(file_path: Path) -> str | None:
        """Extract text from the service table only, ignoring all other PDF content.

        Uses pdfplumber to detect tables in each page. A table is considered
        valid only if one of its rows matches the expected service-table headers
        (Item, Codigo, Nombre, UND, Fina, Cant, Unitario, Total — at least
        ``_MIN_HEADER_MATCHES`` of them must appear in one row).

        Args:
            file_path: Path to the PDF.

        Returns:
            Newline-joined rows of the first valid service table found, or
            ``None`` if no table with the expected headers is detected.
            ``None`` signals that the invoice should be marked DESCONOCIDO
            rather than classified by keyword matching.
        """
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    for table in page.extract_tables():
                        if not table:
                            continue
                        if any(_is_service_header_row(row) for row in table):
                            rows = [
                                " | ".join(cell if cell else "" for cell in row)
                                for row in table
                            ]
                            return "\n".join(rows)
            return None  # No valid service table found in this PDF
        except Exception as exc:  # pdfplumber raises various errors on corrupt PDFs
            logger.error("Error reading tables from PDF %s: %s", file_path.name, exc)
            return None

    @staticmethod
    def find_unreadable(files: list[Path]) -> list[Path]:
        """Return files that could not be opened as valid PDFs.

        Args:
            files: Candidate PDF paths.

        Returns:
            Files that failed to open.
        """
        return [f for f in files if not DocumentReader._can_open(f)]

    @staticmethod
    def find_needing_ocr(files: list[Path]) -> list[Path]:
        """Return valid PDFs that contain no readable text layer.

        Opens each file once to check both openability and text extraction,
        avoiding a redundant second ``fitz.open`` call.

        Args:
            files: Candidate PDF paths.

        Returns:
            Files that require OCR processing.
        """
        needing_ocr: list[Path] = []
        for f in files:
            try:
                with fitz.open(f) as doc:
                    if doc.page_count > 0 and not any(
                        page.get_text().strip() for page in doc
                    ):
                        needing_ocr.append(f)
            except (fitz.FileDataError, OSError, RuntimeError):
                pass
        return needing_ocr
