"""PDF validity checking and text extraction using PyMuPDF (fitz) and pdfplumber."""

import logging
from pathlib import Path

import fitz
import pdfplumber

logger = logging.getLogger(__name__)


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
    def read_table_text(file_path: Path) -> str:
        """Extract text from table cells only, ignoring all other PDF content.

        Uses pdfplumber to detect tables in each page and joins each row's
        cells with \" | \". Only this structured content is returned, which
        reduces false-positive keyword matches in administrative text sections.

        Args:
            file_path: Path to the PDF.

        Returns:
            Newline-joined rows from all tables, or empty string on failure.
        """
        try:
            rows: list[str] = []
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    for table in page.extract_tables():
                        for row in table:
                            rows.append(" | ".join(cell if cell else "" for cell in row))
            return "\n".join(rows)
        except Exception as exc:  # pdfplumber raises various errors on corrupt PDFs
            logger.error("Error reading tables from PDF %s: %s", file_path.name, exc)
            return ""

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
