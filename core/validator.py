"""PDF content analysis: CUFE extraction, invoice code validation, and text search."""

import logging
import re
from pathlib import Path

from config.settings import Settings
from core.reader import DocumentReader
from core.helpers import remove_accents

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level compiled regex constants
# ---------------------------------------------------------------------------

_id_prefix = Settings.invoice_identifier_prefix
_RE_INVOICE_CODE = re.compile(rf"({_id_prefix}\d+)", re.IGNORECASE)
_RE_CUFE = re.compile(r"CUFE\s*[:]*\s*(.{64,})\n", re.IGNORECASE)
_RE_INLINE_WHITESPACE = re.compile(r"[ \t]+")
_MIN_CUFE_LENGTH: int = 64


def _collapse_inline_whitespace(text: str) -> str:
    """Remove spaces and tabs while preserving newlines.

    Args:
        text: Raw PDF text content.

    Returns:
        Text with horizontal whitespace removed.
    """
    return _RE_INLINE_WHITESPACE.sub("", text)


class InvoiceValidator:
    """Analyses PDF content to validate CUFE codes, invoice numbers, and text presence.

    Args:
        base_dir: Root directory for file operations.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)

    def extract_cufe_code(self, text: str) -> str | None:
        """Extract and normalise a CUFE code from invoice text.

        Args:
            text: Raw PDF text content.

        Returns:
            Normalised (lowercase, stripped) CUFE string, or None if not found.
        """
        match = _RE_CUFE.search(text)
        if match:
            return match.group(1).strip().lower()
        return None

    def is_cufe_valid(self, file_path: Path) -> bool:
        """Return True if the PDF contains a valid CUFE code of at least 64 chars.

        Args:
            file_path: Path to the invoice PDF.

        Returns:
            True if a valid CUFE is found in the document.
        """
        content = DocumentReader.read_text(file_path)
        cufe = self.extract_cufe_code(content)
        return bool(cufe and len(cufe) >= _MIN_CUFE_LENGTH)

    def find_missing_cufe(self, file_paths: list[Path]) -> list[Path]:
        """Return invoice files that do not contain a valid CUFE code.

        Args:
            file_paths: Invoice PDF paths to check.

        Returns:
            Files without a valid CUFE.
        """
        return [p for p in file_paths if not self.is_cufe_valid(p)]

    def find_files_with_text(
        self,
        files: list[Path],
        search_text: str,
        return_parent: bool = True,
    ) -> list[Path]:
        """Return paths of files (or their parents) containing the search text.

        Search is accent-insensitive and case-insensitive.

        Args:
            files: PDF files to inspect.
            search_text: Text to search for.
            return_parent: If True, return the parent directory; else the file.

        Returns:
            Deduplicated list of matching paths.
        """
        results: set[Path] = set()
        term = remove_accents(search_text).upper()

        for f in files:
            content = DocumentReader.read_text(f)
            if not content:
                continue
            if term in remove_accents(content).upper():
                results.add(f.parent if return_parent else f)

        return list(results)

    def find_missing_invoice_code(self, files: list[Path]) -> list[Path]:
        """Return files whose content does not contain the invoice code from their name.

        Args:
            files: PDF files to inspect.

        Returns:
            Files where the invoice code is absent from the document text.
        """
        missing: list[Path] = []
        for f in files:
            match = _RE_INVOICE_CODE.search(f.stem.upper())
            if match:
                code = match.group(1)
                content = DocumentReader.read_text(f)
                if content and code not in content.upper():
                    missing.append(f)
        return missing

    def validate_invoice_files(
        self, file_paths: list[Path]
    ) -> tuple[list[Path], list[Path]]:
        """Check invoice code and CUFE presence in a single read pass per file.

        Args:
            file_paths: Invoice PDF paths to audit.

        Returns:
            A tuple ``(missing_invoice_code, missing_cufe)`` — each is a list
            of file paths that failed the corresponding check.
        """
        missing_invoice_code: list[Path] = []
        missing_cufe: list[Path] = []

        for f in file_paths:
            content = DocumentReader.read_text(f)
            if not content:
                continue

            normalised = _collapse_inline_whitespace(content.upper())

            invoice_match = _RE_INVOICE_CODE.search(f.stem.upper())
            if invoice_match and invoice_match.group(1) not in normalised:
                missing_invoice_code.append(f)

            cufe = self.extract_cufe_code(normalised)
            if not (cufe and len(cufe) >= _MIN_CUFE_LENGTH):
                missing_cufe.append(f)

        return missing_invoice_code, missing_cufe
