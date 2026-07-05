"""PDF content analysis: CUFE extraction, invoice code validation, and text search."""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PDF_WORKERS = min(16, (os.cpu_count() or 4) * 2)

from core.helpers import remove_accents  # noqa: E402
from core.reader import DocumentReader  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level compiled regex constants (not id_prefix-dependent)
# ---------------------------------------------------------------------------

_RE_CUFE = re.compile(r"CUFE\s*[:]*\s*(.{64,})\n", re.IGNORECASE)
_RE_INLINE_WHITESPACE = re.compile(r"[ \t]+")
_MIN_CUFE_LENGTH: int = 64


def _collapse_inline_whitespace(text: str) -> str:
    """Remove spaces and tabs while preserving newlines."""
    return _RE_INLINE_WHITESPACE.sub("", text)


class InvoiceValidator:
    """Analyses PDF content to validate CUFE codes, invoice numbers, and text presence.

    Args:
        base_dir: Root directory for file operations.
        id_prefix: Invoice identifier prefix used to match invoice codes in PDFs.
    """

    def __init__(self, base_dir: Path, id_prefix: str = "", *, _reader=None) -> None:
        self.base_dir = Path(base_dir)
        _esc = re.escape(id_prefix)
        self._re_invoice_code = re.compile(rf"({_esc}\d+)$", re.IGNORECASE)
        # Allow injecting a custom reader for testing without real PDF files.
        self._read_text = _reader if _reader is not None else DocumentReader.read_text

    def extract_cufe_code(self, text: str) -> str | None:
        """Extract and normalise a CUFE code from invoice text."""
        match = _RE_CUFE.search(text)
        if match:
            return match.group(1).strip().lower()
        return None

    def is_cufe_valid(self, file_path: Path) -> bool:
        """Return True if the PDF contains a valid CUFE code of at least 64 chars."""
        content = self._read_text(file_path)
        cufe = self.extract_cufe_code(content)
        return bool(cufe and len(cufe) >= _MIN_CUFE_LENGTH)

    def find_missing_cufe(self, file_paths: list[Path]) -> list[Path]:
        """Return invoice files that do not contain a valid CUFE code."""
        return [p for p in file_paths if not self.is_cufe_valid(p)]

    def find_files_with_text(
        self,
        files: list[Path],
        search_text: str,
        return_parent: bool = True,
    ) -> list[Path]:
        """Return paths of files (or their parents) containing the search text."""
        results: set[Path] = set()
        term = remove_accents(search_text).upper()

        for f in files:
            content = self._read_text(f)
            if not content:
                continue
            if term in remove_accents(content).upper():
                results.add(f.parent if return_parent else f)

        return list(results)

    def find_files_with_table_text(
        self,
        files: list[Path],
        search_text: str,
        return_parent: bool = True,
    ) -> list[Path]:
        """Like find_files_with_text but searches only table cell content."""
        results: set[Path] = set()
        term = remove_accents(search_text).upper()

        for f in files:
            content = DocumentReader.read_table_text(f)
            if not content:
                continue
            if term in remove_accents(content).upper():
                results.add(f.parent if return_parent else f)

        return list(results)

    def find_missing_invoice_code(self, files: list[Path]) -> list[Path]:
        """Return files whose content does not contain the invoice code from their name."""
        missing: list[Path] = []
        for f in files:
            match = self._re_invoice_code.search(f.stem.upper())
            if match:
                code = match.group(1)
                content = self._read_text(f)
                if content and code not in content.upper():
                    missing.append(f)
        return missing

    def validate_invoice_files(self, file_paths: list[Path]) -> tuple[list[Path], list[Path]]:
        """Check invoice code and CUFE presence in a single read pass per file (parallel)."""
        if not file_paths:
            return [], []

        re_invoice_code = self._re_invoice_code
        read_text = self._read_text

        def _check(f: Path) -> tuple[bool, bool]:
            content = read_text(f)
            if not content:
                return False, True
            normalised = _collapse_inline_whitespace(content.upper())
            invoice_match = re_invoice_code.search(f.stem.upper())
            missing_code = bool(invoice_match and invoice_match.group(1) not in normalised)
            cufe = _RE_CUFE.search(normalised)
            cufe_val = cufe.group(1).strip() if cufe else None
            missing_cufe = not (cufe_val and len(cufe_val) >= _MIN_CUFE_LENGTH)
            return missing_code, missing_cufe

        with ThreadPoolExecutor(max_workers=_PDF_WORKERS) as pool:
            results = list(pool.map(_check, file_paths))

        missing_invoice_code = [f for f, (mc, _) in zip(file_paths, results) if mc]
        missing_cufe = [f for f, (_, mc) in zip(file_paths, results) if mc]
        return missing_invoice_code, missing_cufe
