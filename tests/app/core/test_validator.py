"""Tests for core/validator.py — CUFE regex + injectable reader."""

from __future__ import annotations

from pathlib import Path


from app.core.validator import InvoiceValidator, _collapse_inline_whitespace


# ---------------------------------------------------------------------------
# Pure regex helpers (no PDF, no filesystem)
# ---------------------------------------------------------------------------


class TestCollapseInlineWhitespace:
    def test_removes_spaces_and_tabs(self):
        assert _collapse_inline_whitespace("A  B\tC") == "ABC"

    def test_preserves_newlines(self):
        assert _collapse_inline_whitespace("A B\nC D") == "AB\nCD"

    def test_empty_string(self):
        assert _collapse_inline_whitespace("") == ""


class TestExtractCufeCode:
    _VALID_CUFE = "a" * 64  # 64-char string

    def test_extracts_valid_cufe(self):
        v = InvoiceValidator(Path("."))
        text = f"CUFE: {self._VALID_CUFE}\n"
        assert v.extract_cufe_code(text) == self._VALID_CUFE

    def test_returns_none_when_absent(self):
        v = InvoiceValidator(Path("."))
        assert v.extract_cufe_code("No CUFE here") is None

    def test_case_insensitive_cufe_label(self):
        v = InvoiceValidator(Path("."))
        text = f"cufe: {self._VALID_CUFE}\n"
        result = v.extract_cufe_code(text)
        assert result is not None

    def test_strips_whitespace_from_value(self):
        v = InvoiceValidator(Path("."))
        text = f"CUFE:   {self._VALID_CUFE}   \n"
        result = v.extract_cufe_code(text)
        assert result == self._VALID_CUFE


# ---------------------------------------------------------------------------
# Tests with injected reader (no real PDFs needed)
# ---------------------------------------------------------------------------

_VALID_CUFE = "b" * 64
_INVOICE_ID = "HSL456"


def _make_reader(content: str):
    """Return a callable that ignores file_path and returns content."""
    return lambda _path: content


class TestValidateInvoiceFiles:
    def test_returns_both_missing_lists(self, tmp_path: Path, id_prefix: str):
        good = tmp_path / f"FEV_900_{id_prefix}100.pdf"
        bad_code = tmp_path / f"FEV_900_{id_prefix}200.pdf"
        bad_cufe = tmp_path / f"FEV_900_{id_prefix}300.pdf"
        for f in [good, bad_code, bad_cufe]:
            f.write_bytes(b"")

        def reader(path: Path) -> str:
            if path == good:
                return f"{id_prefix}100\nCUFE: {_VALID_CUFE}\n"
            if path == bad_code:
                return f"NO CODE HERE\nCUFE: {_VALID_CUFE}\n"
            return f"{id_prefix}300\nNo CUFE\n"

        v = InvoiceValidator(tmp_path, id_prefix, _reader=reader)
        missing_code, missing_cufe = v.validate_invoice_files([good, bad_code, bad_cufe])
        assert bad_code in missing_code
        assert good not in missing_code
        assert bad_cufe in missing_cufe
        assert good not in missing_cufe

    def test_reports_progress_for_every_file(self, tmp_path: Path, id_prefix: str):
        files = [tmp_path / f"FEV_900_{id_prefix}{n}.pdf" for n in range(5)]
        for f in files:
            f.write_bytes(b"")
        calls: list[tuple[int, int]] = []

        v = InvoiceValidator(tmp_path, id_prefix, _reader=lambda _p: "")
        v.validate_invoice_files(files, on_progress=lambda done, total, _name: calls.append((done, total)))

        assert sorted(calls) == [(n, 5) for n in range(1, 6)]

    def test_cancel_event_skips_remaining_files(self, tmp_path: Path, id_prefix: str):
        import threading

        f = tmp_path / f"FEV_900_{id_prefix}1.pdf"
        f.write_bytes(b"")
        cancel = threading.Event()
        cancel.set()

        v = InvoiceValidator(tmp_path, id_prefix, _reader=lambda _p: "")
        assert v.validate_invoice_files([f], cancel_event=cancel) == ([], [])
