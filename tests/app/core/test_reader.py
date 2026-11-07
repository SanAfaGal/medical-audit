"""Tests for core/reader.py — text-layer detection with progress and cancellation."""

from __future__ import annotations

import threading
from pathlib import Path

import fitz

from app.core.reader import DocumentReader


def _pdf(path: Path, text: str | None) -> Path:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((72, 72), text)
    doc.save(str(path))
    doc.close()
    return path


class TestFindNeedingOcr:
    def test_flags_only_pdfs_without_text(self, tmp_path: Path):
        with_text = _pdf(tmp_path / "text.pdf", "hola")
        blank = _pdf(tmp_path / "blank.pdf", None)
        broken = tmp_path / "broken.pdf"
        broken.write_bytes(b"not a pdf")

        assert DocumentReader.find_needing_ocr([with_text, blank, broken]) == [blank]

    def test_reports_progress_for_every_file(self, tmp_path: Path):
        files = [_pdf(tmp_path / f"{n}.pdf", "x") for n in range(4)]
        calls: list[tuple[int, int]] = []

        DocumentReader.find_needing_ocr(files, on_progress=lambda d, t, _n: calls.append((d, t)))

        assert sorted(calls) == [(n, 4) for n in range(1, 5)]

    def test_cancel_event_skips_files(self, tmp_path: Path):
        blank = _pdf(tmp_path / "blank.pdf", None)
        cancel = threading.Event()
        cancel.set()

        assert DocumentReader.find_needing_ocr([blank], cancel_event=cancel) == []


class TestFindUnopenable:
    def test_flags_corrupt_files_and_other_formats_named_pdf(self, tmp_path: Path):
        good = _pdf(tmp_path / "good.pdf", "hola")
        corrupt = tmp_path / "corrupt.pdf"
        corrupt.write_bytes(b"%PDF-1.4 truncated")
        not_a_pdf = tmp_path / "photo.pdf"
        not_a_pdf.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)

        assert DocumentReader.find_unopenable([good, corrupt, not_a_pdf]) == [corrupt, not_a_pdf]

    def test_reports_progress_in_order_and_honours_cancel(self, tmp_path: Path):
        files = [_pdf(tmp_path / f"{n}.pdf", "x") for n in range(3)]
        calls: list[tuple[int, int]] = []

        DocumentReader.find_unopenable(files, on_progress=lambda d, t, _n: calls.append((d, t)))
        assert calls == [(1, 3), (2, 3), (3, 3)]

        cancel = threading.Event()
        cancel.set()
        calls.clear()
        DocumentReader.find_unopenable(files, on_progress=lambda d, t, _n: calls.append((d, t)), cancel_event=cancel)
        assert calls == []
