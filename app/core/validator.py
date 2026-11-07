"""PDF content analysis: CUFE extraction, invoice code validation, and text search."""

import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PDF_WORKERS = min(16, (os.cpu_count() or 4) * 2)

from app.core.helpers import ProgressFn  # noqa: E402
from app.core.reader import DocumentReader  # noqa: E402

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

    def validate_invoice_files(
        self,
        file_paths: list[Path],
        on_progress: ProgressFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> tuple[list[Path], list[Path]]:
        """Check invoice code and CUFE presence in a single read pass per file (parallel).

        ``on_progress(done, total, file_name)`` is called after each file (from worker threads).
        When ``cancel_event`` is set the remaining files are skipped and reported as valid.
        """
        if not file_paths:
            return [], []

        total = len(file_paths)
        done_count = 0
        lock = threading.Lock()

        re_invoice_code = self._re_invoice_code
        read_text = self._read_text

        def _check(f: Path) -> tuple[bool, bool]:
            nonlocal done_count
            if cancel_event is not None and cancel_event.is_set():
                return False, False
            try:
                return _check_one(f)
            finally:
                if on_progress is not None:
                    with lock:
                        done_count += 1
                        current = done_count
                    on_progress(current, total, f.name)

        def _check_one(f: Path) -> tuple[bool, bool]:
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

        missing_invoice_code = [f for f, (mc, _) in zip(file_paths, results, strict=True) if mc]
        missing_cufe = [f for f, (_, mc) in zip(file_paths, results, strict=True) if mc]
        return missing_invoice_code, missing_cufe
