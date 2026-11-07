"""Tests for core/pdfpool.py and the scans built on it. These start real worker processes (spawn)."""

from __future__ import annotations

import os
import threading
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import fitz
import pytest

from app.core import pdfpool
from app.core.reader import DocumentReader
from app.core.validator import InvoiceValidator

_CUFE = "a1b2c3d4" * 12


def _pdf(path: Path, text: str | None) -> Path:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((36, 60), text, fontsize=8)
    doc.save(str(path))
    doc.close()
    return path


@pytest.fixture(scope="module")
def invoices(tmp_path_factory) -> list[Path]:
    """60 invoices (above pdfpool.MIN_PARALLEL): every 10th has no CUFE, every 7th is a blank page."""
    folder = tmp_path_factory.mktemp("pool")
    files = []
    for n in range(60):
        if n % 7 == 0:
            text = None
        elif n % 10 == 0:
            text = f"Factura HSL{n}"
        else:
            text = f"Factura HSL{n}\nCUFE: {_CUFE}\n"
        files.append(_pdf(folder / f"FEV_900_HSL{n}.pdf", text))
    return files


@pytest.fixture(autouse=True)
def _fresh_pool():
    yield
    pdfpool.shutdown()


class TestPoolMatchesInThreadResults:
    def test_validate_gives_the_same_answer_with_and_without_the_pool(self, invoices, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "1")
        serial = InvoiceValidator(invoices[0].parent, "HSL").validate_invoice_files(invoices)
        from app.core import validator

        validator._RESULT_CACHE.clear()  # force the second run to really read the files
        monkeypatch.setenv("PDF_WORKERS", "3")
        pooled = InvoiceValidator(invoices[0].parent, "HSL").validate_invoice_files(invoices)

        assert pooled == serial
        assert len(serial[1]) > 0  # the data does contain files to flag

    def test_text_layer_and_open_checks_agree_too(self, invoices, monkeypatch):
        corrupt = invoices[0].parent / "corrupt.pdf"
        corrupt.write_bytes(b"%PDF-1.4 truncated")
        files = [*invoices, corrupt]

        monkeypatch.setenv("PDF_WORKERS", "1")
        serial = (DocumentReader.find_needing_ocr(files), DocumentReader.find_unopenable(files))
        monkeypatch.setenv("PDF_WORKERS", "3")
        pooled = (DocumentReader.find_needing_ocr(files), DocumentReader.find_unopenable(files))

        assert pooled == serial
        assert serial[1] == [corrupt]
        assert {f.name for f in serial[0]} == {f"FEV_900_HSL{n}.pdf" for n in range(0, 60, 7)}


class TestProgressAndCancel:
    def test_progress_reaches_the_total_and_never_goes_back(self, invoices, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "3")
        seen: list[int] = []

        DocumentReader.find_unopenable(invoices, on_progress=lambda done, total, _n: seen.append(done))

        assert seen == sorted(seen)
        assert seen[-1] == len(invoices)

    def test_cancel_stops_new_chunks_and_returns_what_finished(self, invoices, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "2")
        cancel = threading.Event()
        calls: list[int] = []

        def on_progress(done, total, _name):
            calls.append(done)
            cancel.set()  # cancel as soon as the first chunk lands

        result = pdfpool.run_chunked(
            _echo_chunk, list(range(400)), chunk_size=10, on_progress=on_progress, cancel_event=cancel
        )

        assert any(r is None for r in result), "chunks queued behind the cancel must be skipped"
        assert result[0] == 0  # whatever finished before is kept, in order

    def test_cancelled_before_start_does_no_work(self, invoices):
        cancel = threading.Event()
        cancel.set()
        assert DocumentReader.find_unopenable(invoices, cancel_event=cancel) == []


class TestFallbacks:
    def test_pool_disabled_runs_in_the_calling_thread(self, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "0")
        pids = pdfpool.run_chunked(_pid_chunk, list(range(100)), chunk_size=10)
        assert set(pids) == {os.getpid()}

    def test_small_batches_skip_the_pool(self, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "4")
        pids = pdfpool.run_chunked(_pid_chunk, list(range(pdfpool.MIN_PARALLEL - 1)), chunk_size=10)
        assert set(pids) == {os.getpid()}

    def test_large_batches_really_use_other_processes(self, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "2")
        pids = pdfpool.run_chunked(_pid_chunk, list(range(200)), chunk_size=10)
        assert os.getpid() not in set(pids)

    def test_a_crashed_worker_is_replaced_and_the_batch_retried_once(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "2")
        marker = str(tmp_path / "crashed-once")
        out = pdfpool.run_chunked(_crash_first_time, [(marker, n) for n in range(100)], chunk_size=10)
        assert out == list(range(100))

    def test_a_worker_that_keeps_crashing_raises(self, monkeypatch):
        monkeypatch.setenv("PDF_WORKERS", "2")
        with pytest.raises(BrokenProcessPool):
            pdfpool.run_chunked(_always_crash, list(range(100)), chunk_size=10)


# Workers must live at module level so spawn can import them in the child process.
def _echo_chunk(items: list[int]) -> list[int]:
    return items


def _pid_chunk(items: list[int]) -> list[int]:
    return [os.getpid() for _ in items]


def _crash_first_time(items: list[tuple[str, int]]) -> list[int]:
    marker = Path(items[0][0])
    if not marker.exists():
        marker.write_text("x")
        os._exit(1)  # simulate the parser crashing on a hostile PDF
    return [n for _, n in items]


def _always_crash(items: list[int]) -> list[int]:
    os._exit(1)
