"""PDF validity checking and text extraction using PyMuPDF (fitz) and pdfplumber."""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PDF_CHECK_WORKERS = min(16, (os.cpu_count() or 4) * 2)

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
    matches = sum(any(header in cell for cell in cells) for header in _SERVICE_HEADERS)
    return matches >= _MIN_HEADER_MATCHES


class DocumentReader:
    """Provides static helpers for opening and reading PDF documents."""

    @staticmethod
    def _can_open(file_path: Path) -> bool:
        """Return True only if the file is a valid, openable PDF.

        Fitz soporta múltiples formatos (TIFF, JPEG, etc.) y los abre sin error
        aunque tengan extensión .pdf. doc.is_pdf garantiza que sea un PDF real,
        y doc.load_page(0) verifica que el contenido de la primera página sea accesible.
        """
        import fitz

        try:
            with fitz.open(file_path) as doc:
                if not doc.is_pdf:
                    return False
                if doc.page_count == 0:
                    return False
                doc.load_page(0)
                return True
        except Exception:
            return False

    @staticmethod
    def read_text(file_path: Path) -> str:
        """Extract all text from a PDF file."""
        import fitz

        try:
            with fitz.open(file_path) as doc:
                return "".join(page.get_text() for page in doc)
        except (fitz.FileDataError, OSError, RuntimeError) as exc:
            logger.error("Error reading PDF %s: %s", file_path.name, exc)
            return ""

    @staticmethod
    def read_table_text(file_path: Path) -> str | None:
        """Extract text from the service table only, ignoring all other PDF content."""
        import pdfplumber

        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    for table in page.extract_tables():
                        if not table:
                            continue
                        if any(_is_service_header_row(row) for row in table):
                            rows = [" | ".join(cell if cell else "" for cell in row) for row in table]
                            return "\n".join(rows)
            return None
        except Exception as exc:  # pdfplumber raises various errors on corrupt PDFs
            logger.error("Error reading tables from PDF %s: %s", file_path.name, exc)
            return None

    @staticmethod
    def find_unreadable(files: list[Path]) -> list[Path]:
        """Return files that could not be opened as valid PDFs (parallel)."""
        if not files:
            return []
        with ThreadPoolExecutor(max_workers=_PDF_CHECK_WORKERS) as pool:
            results = pool.map(DocumentReader._can_open, files)
        return [f for f, ok in zip(files, results) if not ok]

    @staticmethod
    def find_needing_ocr(files: list[Path]) -> list[Path]:
        """Return valid PDFs that contain no readable text layer (parallel)."""
        if not files:
            return []

        def _needs_ocr(f: Path) -> bool:
            import fitz

            try:
                with fitz.open(f) as doc:
                    if doc.page_count == 0:
                        return False
                    # First page is sufficient: invoice PDFs are uniformly
                    # digital or scanned across all pages.
                    return not doc[0].get_text().strip()
            except (fitz.FileDataError, OSError, RuntimeError):
                return False

        with ThreadPoolExecutor(max_workers=_PDF_CHECK_WORKERS) as pool:
            results = pool.map(_needs_ocr, files)
        return [f for f, needs in zip(files, results) if needs]
