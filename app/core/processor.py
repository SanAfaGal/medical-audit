"""Batch Ghostscript compression for PDF documents."""

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ghostscript constants
# ---------------------------------------------------------------------------

_GS_COMPAT_LEVEL: str = "1.4"
_GS_DEFAULT_QUALITY: str = "ebook"
_GHOSTSCRIPT_WIN: str = "gswin64c"
_GHOSTSCRIPT_UNIX: str = "gs"


class DocumentProcessor:
    """Orchestrates mass compression operations on PDF files."""

    @staticmethod
    def is_ghostscript_compressed(file_path: Path) -> bool:
        """Return True if the PDF was already processed by Ghostscript.

        Ghostscript always writes its name into the /Producer metadata field.
        Reading metadata with fitz is fast (~1 ms/file) and avoids re-compressing.
        """
        try:
            import fitz

            with fitz.open(file_path) as doc:
                producer = (doc.metadata.get("producer") or "").lower()
            return "ghostscript" in producer
        except Exception:
            return False

    @staticmethod
    def compress_with_ghostscript(file_path: Path, quality: str = _GS_DEFAULT_QUALITY) -> bool:
        """Compress a PDF using Ghostscript."""
        temp = file_path.with_suffix(".opt.tmp")
        gs = _GHOSTSCRIPT_WIN if os.name == "nt" else _GHOSTSCRIPT_UNIX
        cmd = [
            gs,
            "-sDEVICE=pdfwrite",
            f"-dCompatibilityLevel={_GS_COMPAT_LEVEL}",
            f"-dPDFSETTINGS=/{quality}",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={temp}",
            str(file_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=120)
            temp.replace(file_path)
            return True
        except FileNotFoundError:
            logger.error(
                "Ghostscript (%s) not found in PATH — install it or add it to your system PATH",
                gs,
            )
            return False
        except subprocess.TimeoutExpired:
            logger.error("Ghostscript timed out for %s", file_path.name)
            if temp.exists():
                temp.unlink()
            return False
        except subprocess.CalledProcessError as exc:
            logger.error("Ghostscript compression failed for %s: %s", file_path.name, exc)
            if temp.exists():
                temp.unlink()
            return False

    @classmethod
    def batch_compress(
        cls,
        files: list[Path],
        quality: str = _GS_DEFAULT_QUALITY,
        max_workers: int | None = None,
    ) -> dict[str, int]:
        """Compress PDFs in parallel using Ghostscript.

        max_workers defaults to cpu_count (each GS call occupies ~1 core).
        Returns dict with keys: success, failed, bytes_before, bytes_after.
        """
        workers = max_workers or max(os.cpu_count() or 4, 4)
        results: dict[str, int] = {"success": 0, "failed": 0, "bytes_before": 0, "bytes_after": 0}

        def _compress_one(f: Path) -> tuple[str, int, int]:
            orig = f.stat().st_size
            ok = cls.compress_with_ghostscript(f, quality)
            if not ok:
                return "failed", orig, orig
            return "success", orig, f.stat().st_size

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_compress_one, f): f for f in files}
            for future in as_completed(futures):
                status, before, after = future.result()
                results[status] += 1
                results["bytes_before"] += before
                results["bytes_after"] += after

        return results
