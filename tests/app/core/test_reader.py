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
