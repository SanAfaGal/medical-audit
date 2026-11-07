"""PDF validity checking and text extraction using PyMuPDF (fitz)."""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from app.core.helpers import ProgressFn

_PDF_CHECK_WORKERS = min(16, (os.cpu_count() or 4) * 2)

logger = logging.getLogger(__name__)


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
        except Exception:  # noqa: BLE001 - fitz raises many types on corrupt files; any failure = unreadable
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
    def find_unopenable(
        files: list[Path],
        on_progress: ProgressFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[Path]:
        """Return the files that are not valid, openable PDFs (corrupt, or another format named ``.pdf``).

        One pass in the calling thread: opening a PDF takes under a millisecond, so handing each file to a
        thread pool cost far more than the check itself (measured 6x slower on 6000 files).
        ``on_progress(done, total, file_name)`` is called after each file.
        """
        total = len(files)
        bad: list[Path] = []
        for index, f in enumerate(files, 1):
            if cancel_event is not None and cancel_event.is_set():
                break
            if not DocumentReader._can_open(f):
                bad.append(f)
            if on_progress is not None:
                on_progress(index, total, f.name)
        return bad

    @staticmethod
    def find_needing_ocr(
        files: list[Path],
        on_progress: ProgressFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[Path]:
        """Return valid PDFs that contain no readable text layer (parallel).

        ``on_progress(done, total, file_name)`` is called after each file; once ``cancel_event``
        is set the remaining files are skipped.
        """
        if not files:
            return []

        total = len(files)
        done_count = 0
        lock = threading.Lock()

        def _needs_ocr(f: Path) -> bool:
            nonlocal done_count
            if cancel_event is not None and cancel_event.is_set():
                return False
            try:
                return _needs_ocr_one(f)
            finally:
                if on_progress is not None:
                    with lock:
                        done_count += 1
                        current = done_count
                    on_progress(current, total, f.name)

        def _needs_ocr_one(f: Path) -> bool:
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
        return [f for f, needs in zip(files, results, strict=True) if needs]
