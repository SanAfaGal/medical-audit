"""PDF validity checking and text extraction using PyMuPDF (fitz)."""

import logging
from pathlib import Path

import fitz

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
