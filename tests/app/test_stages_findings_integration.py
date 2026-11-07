"""Findings and folder-check stages over a real database and generated folders — requires PostgreSQL."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import select

from app.models.institution import ServiceTypeDocument
from app.models.invoice import Invoice
from app.models.rules import DocType, FolderStatus, ServiceType
from tests.app.stage_helpers import run_stage as _run

pytestmark = pytest.mark.db


async def _add_invoice(ctx, number: str, *, status: str = "PRESENTE", service_type: str = "GENERAL") -> Invoice:
    db = ctx["db"]
    folder_status = (await db.execute(select(FolderStatus).where(FolderStatus.status == status))).scalar_one()
    st = (await db.execute(select(ServiceType).where(ServiceType.code == service_type))).scalar_one()
    invoice = Invoice(
        audit_period_id=ctx["period"].id,
        invoice_number=number,
        date=datetime.date(2024, 1, 10),
        id_type="CC",
        id_number="1",
        patient_name="Paciente",
        service_type_id=st.id,
        folder_status_id=folder_status.id,
    )
    db.add(invoice)
    await db.flush()
    return invoice


async def _require_documents(ctx, *doc_codes: str, service_type: str = "GENERAL") -> None:
    db = ctx["db"]
    st = (await db.execute(select(ServiceType).where(ServiceType.code == service_type))).scalar_one()
    for code in doc_codes:
        dt = (await db.execute(select(DocType).where(DocType.code == code))).scalar_one()
        db.add(ServiceTypeDocument(institution_id=ctx["institution"].id, service_type_id=st.id, doc_type_id=dt.id))
    await db.flush()


def _files(stage_path: Path, folder: str, *names: str) -> Path:
    path = stage_path / folder
    path.mkdir(exist_ok=True)
    for name in names:
        (path / name).write_bytes(b"x")
    return path


async def _status_of(ctx, number: str) -> str:
    row = await ctx["db"].execute(
        select(FolderStatus.status)
        .join(Invoice, Invoice.folder_status_id == FolderStatus.id)
        .where(Invoice.audit_period_id == ctx["period"].id, Invoice.invoice_number == number)
    )
    return row.scalar_one()


def _data_payload(lines: list[str]):
    line = next(line for line in lines if line.startswith("[DATA] ["))
    return json.loads(line[len("[DATA] ") :])


class TestFindingsStages:
    async def test_check_required_docs_records_findings_and_marks_the_invoice_pending(self, stage_env):
        stage_path = stage_env["stage_path"]
        await _add_invoice(stage_env, "1")
        await _add_invoice(stage_env, "2")
        await _require_documents(stage_env, "FACTURA", "HISTORIA")
        _files(stage_path, "HSL1", "FEV_900123456_HSL1.pdf")  # lacks the clinical history
        _files(stage_path, "HSL2", "FEV_900123456_HSL2.pdf", "HCU_900123456_HSL2.pdf")  # complete

        lines = await _run("CHECK_REQUIRED_DOCS", stage_env)

        missing = [line for line in lines if "Faltantes (Historia clínica)" in line]
        assert len(missing) == 1 and "[1]" in missing[0]
        assert not any("HSL2" in line and "Faltantes" in line for line in lines)
        assert any("Facturas marcadas PENDIENTE: 1" in line for line in lines)
        assert any("Hallazgos totales registrados: 1" in line for line in lines)
        assert await _status_of(stage_env, "1") == "PENDIENTE"
        assert await _status_of(stage_env, "2") == "PRESENTE"

    async def test_revisar_sobrantes_suggests_the_missing_document_for_the_surplus_file(self, stage_env):
        stage_path = stage_env["stage_path"]
        await _add_invoice(stage_env, "1")
        await _require_documents(stage_env, "FACTURA", "HISTORIA")
        folder = _files(stage_path, "HSL1", "FEV_900123456_HSL1.pdf", "raro.pdf")
        await _run("CHECK_REQUIRED_DOCS", stage_env)  # opens the finding for the missing history

        lines = await _run("REVISAR_SOBRANTES", stage_env)

        payload = _data_payload(lines)
        assert payload[0]["folder_path"] == str(folder)
        assert payload[0]["sobrantes"] == [
            {
                "filename": "raro.pdf",
                "suggested_doc_type_id": payload[0]["faltantes"][0]["doc_type_id"],
                "suggested_code": "HISTORIA",
                "suggested_prefix": "HCU",
                "confidence": "high",
            }
        ]
        assert any("Total: 1 archivos sobrantes en 1 carpetas" in line for line in lines)

    async def test_revisar_sobrantes_with_nothing_surplus(self, stage_env):
        await _add_invoice(stage_env, "1")
        await _require_documents(stage_env, "FACTURA")
        _files(stage_env["stage_path"], "HSL1", "FEV_900123456_HSL1.pdf")

        lines = await _run("REVISAR_SOBRANTES", stage_env)

        assert any("No se encontraron archivos sobrantes" in line for line in lines)

    async def test_a_missing_stage_folder_is_an_error_for_revisar_sobrantes(self, stage_env):
        stage_env["stage_path"].rmdir()
        lines = await _run("REVISAR_SOBRANTES", stage_env)
        assert len(lines) == 1 and lines[0].startswith("[ERROR]")


class TestFolderChecks:
    async def test_check_dirs_marks_invoices_without_a_folder_as_missing(self, stage_env):
        await _add_invoice(stage_env, "5")
        await _add_invoice(stage_env, "6")
        _files(stage_env["stage_path"], "HSL6", "FEV_900123456_HSL6.pdf")

        lines = await _run("CHECK_DIRS", stage_env)

        assert any("Marcadas como FALTANTE: 1" in line for line in lines)
        assert await _status_of(stage_env, "5") == "FALTANTE"
        assert await _status_of(stage_env, "6") == "PRESENTE"

    async def test_mark_unknown_dirs_renames_folders_with_no_invoice(self, stage_env):
        stage_path = stage_env["stage_path"]
        await _add_invoice(stage_env, "1")
        _files(stage_path, "HSL1", "a.pdf")
        _files(stage_path, "HSL99", "a.pdf")  # looks like an invoice folder but is not in the database

        lines = await _run("MARK_UNKNOWN_DIRS", stage_env)

        assert (stage_path / "(DESCONOCIDO) HSL99").is_dir()
        assert (stage_path / "HSL1").is_dir()
        assert any("Renombradas: 1, errores: 0" in line for line in lines)

    async def test_extra_text_folders_are_reported_then_normalized(self, stage_env):
        stage_path = stage_env["stage_path"]
        invoice = await _add_invoice(stage_env, "7")
        _files(stage_path, "HSL7 COPIA", "a.pdf")

        lines = await _run("CHECK_FOLDERS_WITH_EXTRA_TEXT", stage_env)
        assert _data_payload(lines) == [
            {
                "folder_name": "HSL7 COPIA",
                "folder_path": str(stage_path / "HSL7 COPIA"),
                "invoice_number": "7",
                "invoice_id": invoice.id,
                "action": "skip",
            }
        ]

        normalized = await _run("NORMALIZE_DIR_NAMES", stage_env)
        assert any("Carpetas renombradas: 1" in line for line in normalized)
        assert (stage_path / "HSL7").is_dir() and not (stage_path / "HSL7 COPIA").exists()

    async def test_clean_stage_reports_no_extra_text_folders(self, stage_env):
        _files(stage_env["stage_path"], "HSL8", "a.pdf")
        lines = await _run("CHECK_FOLDERS_WITH_EXTRA_TEXT", stage_env)
        assert any("No se encontraron carpetas con texto extra" in line for line in lines)
