"""Run real verification/staging stages over a folder of generated PDFs — requires PostgreSQL (doc types)."""

from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from app.services.pipeline.registry import STAGE_HANDLERS, stage
from app.services.stage_events import ProgressJob
from tests.app.stage_helpers import run_stage as _run

CUFE = "a1b2c3d4" * 12  # 96 chars: longer than the 64 the validator requires


def _pdf(path: Path, text: str | None) -> None:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((36, 60), text, fontsize=9)
    doc.save(str(path))
    doc.close()


def _invoice(stage_path: Path, number: str, text: str | None, folder: str | None = None) -> Path:
    folder_path = stage_path / (folder or f"HSL{number}")
    folder_path.mkdir(exist_ok=True)
    pdf = folder_path / f"FEV_900123456_HSL{number}.pdf"
    _pdf(pdf, text)
    return pdf


pytestmark = pytest.mark.db


class TestVerifyStages:
    async def test_verify_cufe_flags_only_the_invoice_without_cufe(self, stage_env):
        stage_path = stage_env["stage_path"]
        _invoice(stage_path, "1", f"Factura HSL1\nCUFE: {CUFE}\n")
        _invoice(stage_path, "2", "Factura HSL2 sin codigo")

        lines = await _run("VERIFY_CUFE", stage_env)

        assert any("Sin CUFE: FEV_900123456_HSL2.pdf" in line for line in lines)
        assert not any("HSL1.pdf" in line and "Sin CUFE" in line for line in lines)
        assert any("Facturas sin CUFE: 1" in line for line in lines)
        assert any('"type": "progress"' in line and '"current": 2' in line for line in lines)

    async def test_verify_invoice_code_flags_a_pdf_that_lacks_its_own_number(self, stage_env):
        stage_path = stage_env["stage_path"]
        _invoice(stage_path, "10", "Esta factura es HSL10 de verdad")
        _invoice(stage_path, "11", "Texto sin el numero")

        lines = await _run("VERIFY_INVOICE_CODE", stage_env)

        assert any("Sin número de factura en PDF: FEV_900123456_HSL11.pdf" in line for line in lines)
        assert not any("HSL10.pdf" in line and "Sin número" in line for line in lines)
        assert any("Facturas sin código: 1" in line for line in lines)

    async def test_list_unreadable_pdfs_finds_pdfs_without_a_text_layer(self, stage_env):
        stage_path = stage_env["stage_path"]
        _invoice(stage_path, "20", "texto legible HSL20")
        _invoice(stage_path, "21", None)  # a blank page: no text layer

        lines = await _run("LIST_UNREADABLE_PDFS", stage_env)

        assert any("Sin capa de texto: FEV_900123456_HSL21.pdf" in line for line in lines)
        assert not any("HSL20.pdf" in line and "Sin capa" in line for line in lines)
        assert any("PDFs de factura encontrados: 2" in line for line in lines)

    async def test_check_invoice_number_on_files_reports_a_misfiled_document(self, stage_env):
        stage_path = stage_env["stage_path"]
        _invoice(stage_path, "30", "x")
        misfiled = _invoice(stage_path, "31", "x", folder="HSL30")  # HSL31's file inside HSL30's folder
        assert misfiled.parent.name == "HSL30"

        lines = await _run("CHECK_INVOICE_NUMBER_ON_FILES", stage_env)

        assert any("Archivo desajustado: FEV_900123456_HSL31.pdf" in line for line in lines)
        assert any("Archivos desajustados: 1" in line for line in lines)


class TestStageFolderGuard:
    async def test_a_missing_stage_folder_ends_the_stage_with_one_warning(self, stage_env):
        stage_env["stage_path"].rmdir()

        lines = await _run("VERIFY_CUFE", stage_env)

        assert len(lines) == 1
        assert lines[0].startswith("[WARN]") and "Directorio STAGE no existe" in lines[0]

    async def test_stages_that_cannot_run_without_the_folder_report_an_error(self, stage_env):
        stage_env["stage_path"].rmdir()

        lines = await _run("CHECK_NESTED_FOLDERS", stage_env)

        assert len(lines) == 1 and lines[0].startswith("[ERROR]")


class TestRegistryGuard:
    async def test_guard_skips_the_handler_body(self, tmp_path):
        ran = []

        @stage("__GUARD_TEST__", requires="audit", missing="ERROR")
        async def _handler(ctx):
            ran.append(True)
            yield "[INFO] ran"

        try:
            ctx = {"audit_path": tmp_path / "AUDIT"}
            assert [line async for line in STAGE_HANDLERS["__GUARD_TEST__"](ctx)][0].startswith("[ERROR]")
            assert ran == []
            ctx["audit_path"].mkdir()
            assert [line async for line in STAGE_HANDLERS["__GUARD_TEST__"](ctx)] == ["[INFO] ran"]
        finally:
            STAGE_HANDLERS.pop("__GUARD_TEST__", None)


class TestProgressJob:
    async def test_streams_the_bar_and_exposes_the_result(self):
        def work(on_progress, cancel):
            for i in range(1, 4):
                on_progress(i, 3, f"f{i}")
            return "done"

        job = ProgressJob(work, {}, label="Cosas", total=3)
        lines = [line async for line in job]

        assert job.result == "done"
        assert lines[0].startswith("[DATA]") and '"current": 0' in lines[0] and '"total": 3' in lines[0]
        assert '"label": "Cosas"' in lines[-1]


class TestRemoveNonPdf:
    async def test_reports_a_corrupt_pdf_and_a_non_pdf_for_review(self, stage_env):
        import json

        stage_path = stage_env["stage_path"]
        _invoice(stage_path, "40", "texto HSL40")
        folder = stage_path / "HSL40"
        (folder / "CRC_900123456_HSL40.pdf").write_bytes(b"%PDF-1.4 truncated")
        (folder / "foto.jpg").write_bytes(b"\xff\xd8\xff")

        lines = await _run("REMOVE_NON_PDF", stage_env)

        payload = json.loads(next(line for line in lines if line.startswith('[DATA] {"stage"'))[len("[DATA] ") :])
        assert [f["filename"] for f in payload["corrupt_pdfs"]] == ["CRC_900123456_HSL40.pdf"]
        assert [f["filename"] for f in payload["non_pdf"]] == ["foto.jpg"]
        assert any('"type": "progress"' in line and '"current": 2' in line for line in lines)

    async def test_clean_stage_reports_nothing_to_review(self, stage_env):
        _invoice(stage_env["stage_path"], "41", "texto HSL41")

        lines = await _run("REMOVE_NON_PDF", stage_env)

        assert not any(line.startswith('[DATA] {"stage"') for line in lines)
        assert any("STAGE limpio" in line for line in lines)
