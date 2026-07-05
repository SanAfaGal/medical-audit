"""Tests for app/services/pipeline_runner.py — dispatch + stage error handling."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.pipeline_runner import _apply_prefix_corrections, _build_context, _STAGE_HANDLERS, execute


# ---------------------------------------------------------------------------
# _build_context
# ---------------------------------------------------------------------------


class TestBuildContext:
    def test_all_keys_present(self, minimal_institution, minimal_period):
        db = AsyncMock()
        ctx = _build_context(minimal_institution, minimal_period, db, {})
        for key in ("institution", "period", "db", "base_path", "drive_path", "stage_path", "audit_path"):
            assert key in ctx

    def test_paths_derived_from_base(self, minimal_institution, minimal_period):
        db = AsyncMock()
        ctx = _build_context(minimal_institution, minimal_period, db, {})
        base = ctx["base_path"]
        assert ctx["base_path"] == base
        assert ctx["drive_path"] == base / "DRIVE"
        assert ctx["stage_path"] == base / "STAGE"
        assert ctx["audit_path"] == base / "AUDIT"

    def test_extra_keys_merged(self, minimal_institution, minimal_period):
        db = AsyncMock()
        ctx = _build_context(minimal_institution, minimal_period, db, {"invoice_numbers": ["X"]})
        assert ctx["invoice_numbers"] == ["X"]


# ---------------------------------------------------------------------------
# Stage registry
# ---------------------------------------------------------------------------


class TestStageRegistry:
    def test_all_stages_registered(self):
        expected = {
            "LOAD_AND_PROCESS",
            "RECATEGORIZE_SERVICES",
            "RUN_STAGING",
            "CHECK_NESTED_FOLDERS",
            "REMOVE_NON_PDF",
            "NORMALIZE_FILES",
            "LIST_UNREADABLE_PDFS",
            "DELETE_UNREADABLE_PDFS",
            "DOWNLOAD_INVOICES_FROM_SIHOS",
            "DOWNLOAD_MEDICATION_SHEETS",
            "VERIFY_INVOICE_CODE",
            "CHECK_INVOICE_NUMBER_ON_FILES",
            "CHECK_FOLDERS_WITH_EXTRA_TEXT",
            "NORMALIZE_DIR_NAMES",
            "CHECK_DIRS",
            "MARK_UNKNOWN_DIRS",
            "CHECK_REQUIRED_DOCS",
            "REVISAR_SOBRANTES",
            "VERIFY_CUFE",
            "ORGANIZE",
            "COMPRESS_AUDIT",
            "DOWNLOAD_DRIVE",
            "DOWNLOAD_MISSING_DOCS",
            "EXPORTAR_AUDITADOS",
        }
        assert expected == set(_STAGE_HANDLERS.keys())


# ---------------------------------------------------------------------------
# execute — dispatch and error handling
# ---------------------------------------------------------------------------


class TestExecute:
    async def test_unknown_stage_yields_error(self, minimal_institution, minimal_period):
        db = AsyncMock()
        lines = [line async for line in execute("UNKNOWN_STAGE", minimal_institution, minimal_period, db)]
        assert any("[ERROR]" in line and "desconocida" in line for line in lines)

    async def test_known_stage_runs_without_error(self, minimal_institution, minimal_period, tmp_path):
        """REMOVE_NON_PDF with empty STAGE should complete cleanly (no ERROR lines)."""
        inst = SimpleNamespace(**vars(minimal_institution))
        period = SimpleNamespace(**vars(minimal_period))
        period.period_label = "."

        # Create the STAGE directory at the path _build_context will resolve
        stage_dir = tmp_path / inst.name / "STAGE"
        stage_dir.mkdir(parents=True)

        db = AsyncMock()
        with patch("app.services.pipeline_runner.audit_data_root", tmp_path):
            lines = [line async for line in execute("REMOVE_NON_PDF", inst, period, db)]

        assert any("[INFO]" in line for line in lines)
        assert not any("[ERROR]" in line for line in lines)

    async def test_stage_exception_yields_error_line(self, minimal_institution, minimal_period):
        db = AsyncMock()

        async def failing_stage(ctx):
            yield "[INFO] starting"
            raise ValueError("something went wrong")

        original = _STAGE_HANDLERS.get("LOAD_AND_PROCESS")
        _STAGE_HANDLERS["LOAD_AND_PROCESS"] = failing_stage
        try:
            lines = [line async for line in execute("LOAD_AND_PROCESS", minimal_institution, minimal_period, db)]
            assert any("[ERROR]" in line and "falló" in line for line in lines)
        finally:
            if original:
                _STAGE_HANDLERS["LOAD_AND_PROCESS"] = original


# ---------------------------------------------------------------------------
# Individual stage tests — STAGE dir absent guard
# ---------------------------------------------------------------------------


class TestStageGuards:
    """Each stage that requires STAGE dir should yield a WARN/ERROR when absent."""

    @pytest.mark.parametrize(
        "stage_name",
        [
            "REMOVE_NON_PDF",
            "NORMALIZE_FILES",
            "LIST_UNREADABLE_PDFS",
            "DELETE_UNREADABLE_PDFS",
            "CHECK_INVOICE_NUMBER_ON_FILES",
            "CHECK_FOLDERS_WITH_EXTRA_TEXT",
            "NORMALIZE_DIR_NAMES",
            "CHECK_DIRS",
            "CHECK_REQUIRED_DOCS",
            "VERIFY_CUFE",
            "ORGANIZE",
        ],
    )
    async def test_nonexistent_stage_dir_yields_warn_or_error(
        self, stage_name: str, minimal_institution, minimal_period, tmp_path
    ):
        db = AsyncMock()
        inst = SimpleNamespace(**vars(minimal_institution))
        period = SimpleNamespace(**vars(minimal_period))

        handler = _STAGE_HANDLERS[stage_name]
        from app.services.pipeline_runner import _build_context

        # audit_data_root points to tmp_path; STAGE subdir is never created → path absent
        with patch("app.services.pipeline_runner.audit_data_root", tmp_path):
            ctx = _build_context(inst, period, db, {})
            lines = [line async for line in handler(ctx)]
        assert any("[WARN]" in line or "[ERROR]" in line for line in lines), (
            f"Stage {stage_name!r} should warn or error when STAGE dir is absent"
        )


# ---------------------------------------------------------------------------
# REMOVE_NON_PDF — happy path with real tmp_path
# ---------------------------------------------------------------------------


class TestRemoveNonPdfStage:
    async def test_scans_non_pdf_files(self, tmp_path: Path, minimal_institution, minimal_period):
        from app.services.pipeline_runner import _build_context, _STAGE_HANDLERS

        inst = SimpleNamespace(**vars(minimal_institution))
        period = SimpleNamespace(**vars(minimal_period))
        period.period_label = "."

        # _build_context resolves: audit_data_root / inst.name / period_label / "STAGE"
        # With period_label="." → audit_data_root / inst.name / "STAGE"
        stage_dir = tmp_path / inst.name / "STAGE"
        stage_dir.mkdir(parents=True)
        (stage_dir / "notes.txt").write_text("ignore me")
        (stage_dir / "valid.pdf").write_bytes(b"%PDF-1.4 test")  # minimal valid header

        db = AsyncMock()
        with patch("app.services.pipeline_runner.audit_data_root", tmp_path):
            ctx = _build_context(inst, period, db, {})
            handler = _STAGE_HANDLERS["REMOVE_NON_PDF"]
            lines = [line async for line in handler(ctx)]

        # Stage is scan-only: files are NOT deleted, [INFO] lines show counts
        assert any("Archivos no-PDF encontrados: 1" in line for line in lines), f"Lines: {lines}"
        assert (stage_dir / "notes.txt").exists()  # scan only — no deletion
        assert (stage_dir / "valid.pdf").exists()


class TestDeleteUnreadablePdfsStage:
    async def test_moves_files_to_quarantine_instead_of_deleting(self, minimal_institution, minimal_period, tmp_path):
        inst = SimpleNamespace(**vars(minimal_institution))
        period = SimpleNamespace(**vars(minimal_period))
        period.period_label = "."

        stage_dir = tmp_path / inst.name / "STAGE"
        folder = stage_dir / "HSL123"
        folder.mkdir(parents=True)
        pdf_path = folder / "FEV_900123456_HSL123.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 no text layer")

        db = AsyncMock()
        db.execute = AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: None))

        with (
            patch("app.services.pipeline_runner.audit_data_root", tmp_path),
            patch("core.reader.DocumentReader.find_needing_ocr", return_value=[pdf_path]),
        ):
            lines = [line async for line in execute("DELETE_UNREADABLE_PDFS", inst, period, db)]

        assert not pdf_path.exists()
        quarantined = stage_dir / "_CUARENTENA_SIN_TEXTO" / "HSL123" / "FEV_900123456_HSL123.pdf"
        assert quarantined.exists()
        assert any("cuarentena" in line for line in lines)
        assert not any("[ERROR]" in line for line in lines)


class TestApplyPrefixCorrections:
    def test_renames_matching_prefix(self, tmp_path):
        folder = tmp_path / "HSL123"
        folder.mkdir()
        (folder / "OPD_900123456_HSL123.pdf").write_bytes(b"")

        renamed, renames, errors = _apply_prefix_corrections(tmp_path, {"OPD": "OPF"})

        assert renamed == 1
        assert not (folder / "OPD_900123456_HSL123.pdf").exists()
        assert (folder / "OPF_900123456_HSL123.pdf").exists()
        assert errors == []

    def test_skips_and_reports_when_destination_already_exists(self, tmp_path):
        folder = tmp_path / "HSL456"
        folder.mkdir()
        (folder / "OPD_900123456_HSL456.pdf").write_bytes(b"")
        (folder / "OPF_900123456_HSL456.pdf").write_bytes(b"")  # pre-existing collision

        renamed, renames, errors = _apply_prefix_corrections(tmp_path, {"OPD": "OPF"})

        assert renamed == 0
        assert (folder / "OPD_900123456_HSL456.pdf").exists()  # untouched, not lost
        assert len(errors) == 1
        assert "ya existe" in errors[0]


class TestDownloadDriveStage:
    async def test_calls_download_missing_dirs_once_batched(self, minimal_institution, minimal_period, tmp_path):
        institution = SimpleNamespace(**vars(minimal_institution))
        institution.drive_credentials_enc = "encrypted-blob"
        period = SimpleNamespace(**vars(minimal_period))
        db = AsyncMock()

        inv_repo_instance = AsyncMock()
        inv_repo_instance.get_invoice_numbers_by_status = AsyncMock(return_value=["100", "200", "300"])
        inv_repo_instance.batch_update_folder_status = AsyncMock(return_value=2)

        fake_drive = MagicMock()
        fake_drive.download_missing_dirs = MagicMock(return_value=["100", "200"])

        with (
            patch("app.crypto.decrypt", return_value='{"type": "service_account"}'),
            patch("core.drive.DriveSync", return_value=fake_drive),
            patch("app.repositories.invoice_repo.InvoiceRepo", return_value=inv_repo_instance),
            patch("app.services.pipeline_runner.audit_data_root", tmp_path),
        ):
            ctx = _build_context(institution, period, db, {})
            [line async for line in _STAGE_HANDLERS["DOWNLOAD_DRIVE"](ctx)]

        assert fake_drive.download_missing_dirs.call_count == 1
        called_args = fake_drive.download_missing_dirs.call_args.args
        assert called_args[0] == ["100", "200", "300"]
