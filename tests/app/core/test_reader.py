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


def _scan_pdf(path: Path, dark: bool) -> Path:
    """A PDF whose only content is an image (no text layer): a white sheet or one with a dark block."""
    width, height = 200, 280
    pix = fitz.Pixmap(fitz.csGRAY, fitz.IRect(0, 0, width, height), False)
    pix.set_rect(pix.irect, (255,))
    if dark:
        pix.set_rect(fitz.IRect(40, 60, 160, 120), (0,))
    doc = fitz.open()
    page = doc.new_page()
    page.insert_image(page.rect, pixmap=pix)
    doc.save(str(path))
    doc.close()
    return path


class TestIsBlank:
    def test_page_with_text_is_not_blank(self, tmp_path: Path):
        assert DocumentReader.is_blank(_pdf(tmp_path / "text.pdf", "hola")) is False

    def test_empty_page_is_blank(self, tmp_path: Path):
        assert DocumentReader.is_blank(_pdf(tmp_path / "empty.pdf", None)) is True

    def test_scanned_white_sheet_is_blank(self, tmp_path: Path):
        assert DocumentReader.is_blank(_scan_pdf(tmp_path / "white.pdf", dark=False)) is True

    def test_scanned_page_with_content_is_not_blank(self, tmp_path: Path):
        assert DocumentReader.is_blank(_scan_pdf(tmp_path / "scan.pdf", dark=True)) is False

    def test_one_page_with_content_is_enough(self, tmp_path: Path):
        doc = fitz.open()
        doc.new_page()
        doc.new_page().insert_text((72, 72), "pagina dos")
        path = tmp_path / "two.pdf"
        doc.save(str(path))
        doc.close()
        assert DocumentReader.is_blank(path) is False

    def test_corrupt_file_counts_as_blank(self, tmp_path: Path):
        path = tmp_path / "bad.pdf"
        path.write_bytes(b"not a pdf")
        assert DocumentReader.is_blank(path) is True
