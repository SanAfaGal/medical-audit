"""PDF content analysis: CUFE extraction and invoice code validation."""

import logging
import re
import threading
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path

from app.core import pdfpool
from app.core.helpers import ProgressFn
from app.core.reader import DocumentReader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level compiled regex constants (not id_prefix-dependent)
# ---------------------------------------------------------------------------

# Results of validate_invoice_files, so VERIFY_INVOICE_CODE and VERIFY_CUFE (which compute the same thing from the
# same PDFs) read each file once. The key carries the file's mtime and size, so an edited file is recomputed.
_RESULT_CACHE: dict[tuple[str, int, int, str], tuple[bool, bool]] = {}
_RESULT_CACHE_MAX = 200_000

_RE_CUFE = re.compile(r"CUFE\s*[:]*\s*(.{64,})\n", re.IGNORECASE)
_RE_INLINE_WHITESPACE = re.compile(r"[ \t]+")
_MIN_CUFE_LENGTH: int = 64

#: Invoices handed to a worker process per task (about 13 ms of work each).
_CHUNK = 20


def _collapse_inline_whitespace(text: str) -> str:
    """Remove spaces and tabs while preserving newlines."""
    return _RE_INLINE_WHITESPACE.sub("", text)


@lru_cache(maxsize=32)
def _invoice_code_regex(id_prefix: str) -> re.Pattern[str]:
    return re.compile(rf"({re.escape(id_prefix)}\d+)$", re.IGNORECASE)


def _evaluate(path: Path, id_prefix: str, read_text: Callable[[Path], str]) -> tuple[bool, bool]:
    """(invoice number missing from the PDF text, CUFE missing or too short) for one invoice."""
    content = read_text(path)
    if not content:
        return False, True
    normalised = _collapse_inline_whitespace(content.upper())
    invoice_match = _invoice_code_regex(id_prefix).search(path.stem.upper())
    missing_code = bool(invoice_match and invoice_match.group(1) not in normalised)
    cufe = _RE_CUFE.search(normalised)
    cufe_val = cufe.group(1).strip() if cufe else None
    missing_cufe = not (cufe_val and len(cufe_val) >= _MIN_CUFE_LENGTH)
    return missing_code, missing_cufe


def _validate_chunk(items: list[tuple[str, str]]) -> list[tuple[bool, bool]]:
    """Worker (runs in the process pool): ``[(path, id_prefix), ...]`` to one result per invoice."""
    return [_evaluate(Path(path), prefix, DocumentReader.read_text) for path, prefix in items]


class InvoiceValidator:
    """Analyses PDF content to validate CUFE codes and invoice numbers.

    Args:
        base_dir: Root directory for file operations.
        id_prefix: Invoice identifier prefix used to match invoice codes in PDFs.
    """

    def __init__(self, base_dir: Path, id_prefix: str = "", *, _reader=None) -> None:
        self.base_dir = Path(base_dir)
        self._id_prefix = id_prefix
        # Allow injecting a custom reader for testing without real PDF files.
        self._read_text = _reader if _reader is not None else DocumentReader.read_text
        # Only the real reader can run in worker processes and have its results reused.
        self._real_reader = _reader is None

    def _cache_key(self, path: Path) -> tuple[str, int, int, str] | None:
        if not self._real_reader:
            return None
        try:
            stat = path.stat()
        except OSError:
            return None
        return (str(path), stat.st_mtime_ns, stat.st_size, self._id_prefix)

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
        """Check invoice code and CUFE presence in a single read pass per file.

        Files already validated (same path, size, modification time and prefix) are answered from the cache; the
        rest are read in worker processes (:mod:`app.core.pdfpool`). ``on_progress(done, total, file_name)`` is
        called as files or chunks finish. When ``cancel_event`` is set the remaining files are skipped and
        reported as valid (and not cached).
        """
        total = len(file_paths)
        results: list[tuple[bool, bool] | None] = [None] * total
        keys = [self._cache_key(f) for f in file_paths]

        pending: list[int] = []
        done = 0
        for index, key in enumerate(keys):
            hit = _RESULT_CACHE.get(key) if key is not None else None
            if hit is None:
                pending.append(index)
                continue
            results[index] = hit
            done += 1
            if on_progress is not None:
                on_progress(done, total, file_paths[index].name)

        if pending:
            cached_so_far = done
            names = [file_paths[i].name for i in pending]

            def forward(finished: int, _count: int, name: str) -> None:
                if on_progress is not None:
                    on_progress(cached_so_far + finished, total, name)

            computed: list[tuple[bool, bool] | None]
            if self._real_reader:
                computed = pdfpool.run_chunked(
                    _validate_chunk,
                    [(str(file_paths[i]), self._id_prefix) for i in pending],
                    chunk_size=_CHUNK,
                    names=names,
                    on_progress=forward,
                    cancel_event=cancel_event,
                )
            else:  # an injected reader (tests) is not picklable: run it here, one file at a time
                computed = []
                for finished, index in enumerate(pending, 1):
                    if cancel_event is not None and cancel_event.is_set():
                        computed.append(None)
                        continue
                    computed.append(_evaluate(file_paths[index], self._id_prefix, self._read_text))
                    forward(finished, len(pending), names[finished - 1])

            for index, result in zip(pending, computed, strict=True):
                if result is None:
                    continue  # skipped by cancel: never cache a placeholder
                results[index] = result
                if keys[index] is not None:
                    if len(_RESULT_CACHE) >= _RESULT_CACHE_MAX:
                        _RESULT_CACHE.clear()
                    _RESULT_CACHE[keys[index]] = result  # type: ignore[index]

        missing_invoice_code = [f for f, r in zip(file_paths, results, strict=True) if r and r[0]]
        missing_cufe = [f for f, r in zip(file_paths, results, strict=True) if r and r[1]]
        return missing_invoice_code, missing_cufe
