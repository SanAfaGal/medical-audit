"""Tests for DriveSync.download_file — corrupt/blank PDFs must be retried or reported, never silent."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import fitz
import pytest

from app.core.drive import DriveSync


def _pdf_bytes(text: str | None) -> bytes:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((72, 72), text)
    data = doc.tobytes()
    doc.close()
    return data


class _ScriptedDrive(DriveSync):
    """DriveSync without credentials; each fetch writes the next scripted payload (an Exception is raised)."""

    def __init__(self, payloads: list[bytes | Exception]) -> None:  # noqa: D107
        self._problems = []
        self._payloads = iter(payloads)
        self.fetches = 0

    def _fetch_to(self, file_id: str, dest: Path) -> None:
        self.fetches += 1
        payload = next(self._payloads)
        if isinstance(payload, Exception):
            raise payload
        dest.write_bytes(payload)


@pytest.fixture(autouse=True)
def _no_sleep():
    with patch("app.core.drive.time.sleep"):
        yield


class TestDownloadFile:
    def test_good_pdf_is_saved_without_problems(self, tmp_path: Path):
        drive = _ScriptedDrive([_pdf_bytes("factura")])

        drive.download_file("id", "a.pdf", tmp_path / "F1")

        assert (tmp_path / "F1" / "a.pdf").exists()
        assert drive.take_problems() == []
        assert not list((tmp_path / "F1").glob("*.part"))

    def test_corrupt_pdf_is_fetched_again(self, tmp_path: Path):
        good = _pdf_bytes("factura")
        drive = _ScriptedDrive([b"%PDF-1.4 truncated", good])

        drive.download_file("id", "a.pdf", tmp_path / "F1")

        assert drive.fetches == 2
        assert drive.take_problems() == []
        assert (tmp_path / "F1" / "a.pdf").read_bytes() == good

    def test_always_corrupt_is_reported_and_leaves_no_file(self, tmp_path: Path):
        drive = _ScriptedDrive([b"junk"] * 3)

        drive.download_file("id", "a.pdf", tmp_path / "F1")

        assert drive.fetches == 3
        assert not (tmp_path / "F1" / "a.pdf").exists()
        [(label, reason)] = drive.take_problems()
        assert label == "F1/a.pdf"
        assert "corrupto" in reason

    def test_blank_pdf_is_kept_and_reported_without_retry(self, tmp_path: Path):
        drive = _ScriptedDrive([_pdf_bytes(None)])

        drive.download_file("id", "a.pdf", tmp_path / "F1")

        assert drive.fetches == 1  # a blank source would be just as blank the second time
        assert (tmp_path / "F1" / "a.pdf").exists()
        assert drive.take_problems() == [("F1/a.pdf", "PDF en blanco en Drive")]

    def test_http_failure_is_retried_then_reported(self, tmp_path: Path):
        error = OSError("boom")
        drive = _ScriptedDrive([error, error, error])

        drive.download_file("id", "a.pdf", tmp_path / "F1")

        assert drive.fetches == 3
        [(_, reason)] = drive.take_problems()
        assert "no se pudo descargar" in reason

    def test_existing_good_pdf_is_skipped(self, tmp_path: Path):
        folder = tmp_path / "F1"
        folder.mkdir()
        (folder / "a.pdf").write_bytes(_pdf_bytes("ya estaba"))
        drive = _ScriptedDrive([])

        drive.download_file("id", "a.pdf", folder)

        assert drive.fetches == 0

    def test_existing_blank_pdf_is_downloaded_again(self, tmp_path: Path):
        folder = tmp_path / "F1"
        folder.mkdir()
        (folder / "a.pdf").write_bytes(_pdf_bytes(None))
        good = _pdf_bytes("bueno")
        drive = _ScriptedDrive([good])

        drive.download_file("id", "a.pdf", folder)

        assert drive.fetches == 1
        assert (folder / "a.pdf").read_bytes() == good

    def test_failed_refetch_keeps_the_existing_file(self, tmp_path: Path):
        folder = tmp_path / "F1"
        folder.mkdir()
        old = _pdf_bytes(None)
        (folder / "a.pdf").write_bytes(old)
        drive = _ScriptedDrive([OSError("x")] * 3)

        drive.download_file("id", "a.pdf", folder)

        assert (folder / "a.pdf").read_bytes() == old
        assert len(drive.take_problems()) == 1

    def test_non_pdf_is_never_validated(self, tmp_path: Path):
        drive = _ScriptedDrive([b"plain text"])

        drive.download_file("id", "notes.txt", tmp_path / "F1")

        assert (tmp_path / "F1" / "notes.txt").read_bytes() == b"plain text"
        assert drive.take_problems() == []


class TestClosingSummary:
    def test_missing_dirs_emits_one_warning_naming_the_bad_files(self, tmp_path: Path):
        class _Drive(_ScriptedDrive):
            def _batch_search_folders(self, invoice_numbers):
                return {"1"}, [{"id": "f", "name": "F1"}]

            def _sync_folder_tree(self, folder_id, local_path, depth=0, cancel_event=None):
                self.download_file("x", "a.pdf", local_path)
                return 1

        drive = _Drive([_pdf_bytes(None)])
        events: list[tuple[str, str]] = []

        drive.download_missing_dirs(["1"], tmp_path, event_fn=lambda k, i, t, item, d: events.append((k, d)))

        warnings = [d for k, d in events if k == "warning"]
        assert len(warnings) == 1
        assert "1 archivo(s) con problemas" in warnings[0]
        assert "F1/a.pdf" in warnings[0]
        assert drive.take_problems() == []  # consumed by the summary
