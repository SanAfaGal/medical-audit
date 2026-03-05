"""Batch OCR and Ghostscript compression for PDF documents."""

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OCR / Ghostscript constants
# ---------------------------------------------------------------------------

_OCR_LANGUAGE: str = "spa+eng"
_OCR_PAGE_SEG_MODE: str = "11"
_OCR_JOBS: str = "1"
_OCR_FAST_WEB_VIEW: str = "0"
_GS_COMPAT_LEVEL: str = "1.4"
_GS_DEFAULT_QUALITY: str = "ebook"
_GHOSTSCRIPT_WIN: str = "gswin64c"
_GHOSTSCRIPT_UNIX: str = "gs"


class DocumentProcessor:
    """Orchestrates mass OCR and compression operations on PDF files."""

    @staticmethod
    def apply_ocr(file_path: Path) -> bool:
        """Apply OCR to a single PDF file in-place using ocrmypdf.

        Args:
            file_path: Path to the PDF to process.

        Returns:
            True if OCR succeeded and the file was replaced.
        """
        temp = file_path.with_suffix(".ocr.tmp")

        cmd = [
            "ocrmypdf",
            "--jobs", _OCR_JOBS,
            "-l", _OCR_LANGUAGE,
            "--redo-ocr",
            "--fast-web-view", _OCR_FAST_WEB_VIEW,
            "--tesseract-pagesegmode", _OCR_PAGE_SEG_MODE,
            "-q",
            str(file_path),
            str(temp),
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            if temp.exists():
                temp.replace(file_path)
                return True
            return False
        except FileNotFoundError:
            logger.error(
                "ocrmypdf not found in PATH — install it or add it to your system PATH"
            )
            return False
        except subprocess.CalledProcessError as exc:
            logger.error("OCR subprocess failed for %s: %s", file_path.name, exc)
            if temp.exists():
                temp.unlink()
            return False
        except OSError as exc:
            logger.error("File operation failed for %s: %s", file_path.name, exc)
            if temp.exists():
                temp.unlink()
            return False

    @staticmethod
    def compress_with_ghostscript(
        file_path: Path, quality: str = _GS_DEFAULT_QUALITY
    ) -> bool:
        """Compress a PDF using Ghostscript.

        Args:
            file_path: Path to the PDF (overwritten in-place).
            quality: Ghostscript PDF settings level (default ``"ebook"``).

        Returns:
            True if compression succeeded.
        """
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
            subprocess.run(cmd, check=True, capture_output=True)
            temp.replace(file_path)
            return True
        except FileNotFoundError:
            logger.error(
                "Ghostscript (%s) not found in PATH — install it or add it to your system PATH",
                gs,
            )
            return False
        except subprocess.CalledProcessError as exc:
            logger.error(
                "Ghostscript compression failed for %s: %s", file_path.name, exc
            )
            if temp.exists():
                temp.unlink()
            return False

    @classmethod
    def batch_ocr(
        cls, files: list[Path], max_workers: int = 4
    ) -> dict[str, int]:
        """Run OCR on a list of files in parallel with a progress bar.

        Args:
            files: PDF files to process.
            max_workers: Number of parallel worker threads.

        Returns:
            Dict with ``success`` and ``failed`` counts.
        """
        results: dict[str, int] = {"success": 0, "failed": 0}

        with tqdm(
            total=len(files),
            desc="OCR batch processing",
            unit="doc",
            colour="cyan",
        ) as pbar, ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(cls.apply_ocr, f): f for f in files}

            for future in as_completed(futures):
                f = futures[future]
                if future.result():
                    results["success"] += 1
                else:
                    results["failed"] += 1

                pbar.set_postfix_str(f.name[:15])
                pbar.update(1)

        return results
