"""PDF validity checking and text extraction using PyMuPDF (fitz)."""

import logging
import threading
from pathlib import Path

from app.core import pdfpool
from app.core.helpers import ProgressFn

logger = logging.getLogger(__name__)

# Files handed to a worker process per task. Opening a PDF is ~1 ms, reading a first page ~5 ms.
_OPEN_CHUNK = 100
_OCR_CHUNK = 25


def _can_open_chunk(paths: list[str]) -> list[bool]:
    """Worker: DocumentReader._can_open over a chunk of paths (module level so it can be pickled)."""
    return [DocumentReader._can_open(Path(p)) for p in paths]


def _needs_ocr_chunk(paths: list[str]) -> list[bool]:
    """Worker: True for each valid PDF whose first page has no text layer."""
    import fitz

    out: list[bool] = []
    for p in paths:
        try:
            with fitz.open(p) as doc:
                # First page is sufficient: invoice PDFs are uniformly digital or scanned across all pages.
                out.append(doc.page_count > 0 and not doc[0].get_text().strip())
        except (fitz.FileDataError, OSError, RuntimeError):
            out.append(False)
    return out


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

        Scanned in worker processes (see :mod:`app.core.pdfpool`), in chunks: one task per file would cost more
        in coordination than the ~1 ms check itself (measured 6x slower than a plain loop on 6000 files).
        ``on_progress(done, total, file_name)`` is called as chunks finish.
        """
        flags = pdfpool.run_chunked(
            _can_open_chunk,
            [str(f) for f in files],
            chunk_size=_OPEN_CHUNK,
            names=[f.name for f in files],
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
        return [f for f, ok in zip(files, flags, strict=True) if ok is False]

    @staticmethod
    def find_needing_ocr(
        files: list[Path],
        on_progress: ProgressFn | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[Path]:
        """Return valid PDFs that contain no readable text layer.

        ``on_progress(done, total, file_name)`` is called as chunks finish; once ``cancel_event`` is set the
        remaining files are skipped.
        """
        flags = pdfpool.run_chunked(
            _needs_ocr_chunk,
            [str(f) for f in files],
            chunk_size=_OCR_CHUNK,
            names=[f.name for f in files],
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
        return [f for f, needs in zip(files, flags, strict=True) if needs]
