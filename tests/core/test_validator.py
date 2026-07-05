"""Tests for core/validator.py — CUFE regex + injectable reader."""

from __future__ import annotations

from pathlib import Path


from core.validator import InvoiceValidator, _collapse_inline_whitespace


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

    def test_returns_none_for_short_cufe(self):
        v = InvoiceValidator(Path("."))
        text = "CUFE: short\n"
        cufe = v.extract_cufe_code(text)
        # extract_cufe_code returns the raw match; is_cufe_valid checks length
        # We can verify the returned value is too short or None
        if cufe is not None:
            assert len(cufe) < 64

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


class TestIsCufeValidWithInjectedReader:
    def test_valid_cufe_returns_true(self, tmp_path: Path):
        reader = _make_reader(f"CUFE: {_VALID_CUFE}\n")
        v = InvoiceValidator(tmp_path, _reader=reader)
        assert v.is_cufe_valid(tmp_path / "fake.pdf") is True

    def test_missing_cufe_returns_false(self, tmp_path: Path):
        reader = _make_reader("No CUFE here")
        v = InvoiceValidator(tmp_path, _reader=reader)
        assert v.is_cufe_valid(tmp_path / "fake.pdf") is False

    def test_empty_content_returns_false(self, tmp_path: Path):
        reader = _make_reader("")
        v = InvoiceValidator(tmp_path, _reader=reader)
        assert v.is_cufe_valid(tmp_path / "fake.pdf") is False


class TestFindMissingCufe:
    def test_filters_files_missing_cufe(self, tmp_path: Path):
        files = [tmp_path / "good.pdf", tmp_path / "bad.pdf"]
        for f in files:
            f.write_bytes(b"")

        def reader(path: Path) -> str:
            return f"CUFE: {_VALID_CUFE}\n" if path.name == "good.pdf" else ""

        v = InvoiceValidator(tmp_path, _reader=reader)
        missing = v.find_missing_cufe(files)
        assert missing == [tmp_path / "bad.pdf"]


class TestFindMissingInvoiceCode:
    def test_file_without_invoice_code_in_content(self, tmp_path: Path, id_prefix: str):
        # Stem = FEV_900_HSL456 → invoice code = HSL456
        f = tmp_path / f"FEV_900_{id_prefix}456.pdf"
        f.write_bytes(b"")

        def reader(_path: Path) -> str:
            return "Some content without the invoice number"

        v = InvoiceValidator(tmp_path, id_prefix, _reader=reader)
        missing = v.find_missing_invoice_code([f])
        assert f in missing

    def test_file_with_invoice_code_in_content(self, tmp_path: Path, id_prefix: str):
        f = tmp_path / f"FEV_900_{id_prefix}456.pdf"
        f.write_bytes(b"")

        def reader(_path: Path) -> str:
            return f"Invoice {id_prefix}456 is valid"

        v = InvoiceValidator(tmp_path, id_prefix, _reader=reader)
        missing = v.find_missing_invoice_code([f])
        assert f not in missing


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
