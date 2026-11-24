"""The two SIHOS download stages over a real database, with the browser downloader replaced by a recorder."""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from sqlalchemy import select

from app.crypto import encrypt
from app.models.finding import MissingFile
from app.models.invoice import Invoice
from app.models.rules import DocType, FolderStatus, ServiceType
from tests.app.stage_helpers import run_stage as _run

pytestmark = pytest.mark.db

_NO_URL_ERROR = "app.services.pipeline.stages.downloads.sihos_url_error"


class _FakeDownloader:
    """Stands in for SihosDownloader: records how it was built and what it was asked to download."""

    instances: list[_FakeDownloader] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        _FakeDownloader.instances.append(self)

    def run_from_list(self, numbers: list[str], progress_fn=None, event_fn=None, cancel_event=None) -> None:
        self.calls.append(("invoices", (sorted(numbers),)))

    def run_medication_sheets(self, targets, file_prefix, progress_fn=None, event_fn=None, cancel_event=None) -> None:
        self.calls.append(("sheets", (sorted(targets), file_prefix)))


@pytest.fixture
def sihos(monkeypatch, stage_env):
    """A context whose institution can reach SIHOS and whose downloader is the recorder."""
    _FakeDownloader.instances = []
    monkeypatch.setattr("app.core.downloader.SihosDownloader", _FakeDownloader)
    monkeypatch.setattr(_NO_URL_ERROR, lambda institution: None)
    institution = stage_env["institution"]
    institution.sihos_user = "auditor"
    institution.sihos_password = encrypt("secreto")
    institution.sihos_base_url = "http://sihos.lan/sihos"
    institution.sihos_doc_code = "FEV"
    return stage_env


async def _doc_type(ctx, code: str) -> DocType:
    return (await ctx["db"].execute(select(DocType).where(DocType.code == code))).scalar_one()


async def _pending(ctx, number: str, doc_code: str, *, admission: str | None = "ADM1", status="PENDIENTE", open_=True):
    """An invoice in ``status`` with a finding of ``doc_code`` (resolved when ``open_`` is False)."""
    db = ctx["db"]
    folder_status = (await db.execute(select(FolderStatus).where(FolderStatus.status == status))).scalar_one()
    service = (await db.execute(select(ServiceType).where(ServiceType.code == "GENERAL"))).scalar_one()
    invoice = Invoice(
        audit_period_id=ctx["period"].id,
        invoice_number=number,
        date=datetime.date(2024, 1, 10),
        id_type="CC",
        id_number=f"id{number}",
        patient_name="Paciente",
        admission=admission,
        service_type_id=service.id,
        folder_status_id=folder_status.id,
    )
    db.add(invoice)
    await db.flush()
    doc = await _doc_type(ctx, doc_code)
    resolved_at = None if open_ else datetime.datetime(2024, 1, 11)
    db.add(MissingFile(invoice_id=invoice.id, doc_type_id=doc.id, expected_path="", resolved_at=resolved_at))
    await db.flush()


class TestSihosPrerequisites:
    @pytest.mark.parametrize("stage_name", ["DOWNLOAD_INVOICES_FROM_SIHOS", "DOWNLOAD_MEDICATION_SHEETS"])
    async def test_a_document_type_is_required(self, sihos, stage_name):
        lines = await _run(stage_name, sihos)

        assert (
            len(lines) == 1 and lines[0].startswith("[ERROR]") and "No se seleccionó un tipo de documento" in lines[0]
        )

    @pytest.mark.parametrize("stage_name", ["DOWNLOAD_INVOICES_FROM_SIHOS", "DOWNLOAD_MEDICATION_SHEETS"])
    async def test_credentials_are_required(self, sihos, stage_name):
        sihos["doc_type_id"] = (await _doc_type(sihos, "FACTURA")).id
        sihos["institution"].sihos_password = None

        lines = await _run(stage_name, sihos)

        assert len(lines) == 1 and "Credenciales SIHOS no configuradas" in lines[0]

    @pytest.mark.parametrize("stage_name", ["DOWNLOAD_INVOICES_FROM_SIHOS", "DOWNLOAD_MEDICATION_SHEETS"])
    async def test_an_unsafe_portal_url_stops_the_stage(self, sihos, monkeypatch, stage_name):
        sihos["doc_type_id"] = (await _doc_type(sihos, "FACTURA")).id
        monkeypatch.setattr(_NO_URL_ERROR, lambda institution: "URL de SIHOS no permitida: localhost")

        lines = await _run(stage_name, sihos)

        assert len(lines) == 1 and lines[0].startswith("[ERROR]") and "no permitida" in lines[0]
        assert _FakeDownloader.instances == []

    @pytest.mark.parametrize("stage_name", ["DOWNLOAD_INVOICES_FROM_SIHOS", "DOWNLOAD_MEDICATION_SHEETS"])
    async def test_an_unknown_document_type_is_reported(self, sihos, stage_name):
        sihos["doc_type_id"] = 999_999

        lines = await _run(stage_name, sihos)

        assert len(lines) == 1 and "id=999999 no encontrado" in lines[0]


class TestDownloadInvoicesFromSihos:
    async def test_downloads_only_pending_invoices_with_an_open_finding_of_the_chosen_type(self, sihos):
        await _pending(sihos, "1", "FACTURA")
        await _pending(sihos, "2", "FACTURA", status="PRESENTE")  # not PENDIENTE
        await _pending(sihos, "3", "FACTURA", open_=False)  # finding already resolved
        await _pending(sihos, "4", "HISTORIA")  # another document type
        sihos["doc_type_id"] = (await _doc_type(sihos, "FACTURA")).id

        lines = await _run("DOWNLOAD_INVOICES_FROM_SIHOS", sihos)

        (downloader,) = _FakeDownloader.instances
        assert downloader.calls == [("invoices", (["1"],))]
        assert downloader.kwargs == {
            "user": "auditor",
            "password": "secreto",
            "base_url": "http://sihos.lan/sihos",
            "hospital_nit": "900123456",
            "invoice_prefix": "FEV",
            "invoice_id_prefix": "HSL",
            "invoice_doc_code": "FEV",
            "output_dir": sihos["stage_path"],
        }
        assert any("Descargando 1 factura(s) desde SIHOS" in line for line in lines)
        assert any("Descarga SIHOS completada" in line for line in lines)

    async def test_says_so_when_nothing_is_pending(self, sihos):
        sihos["doc_type_id"] = (await _doc_type(sihos, "FACTURA")).id

        lines = await _run("DOWNLOAD_INVOICES_FROM_SIHOS", sihos)

        assert any("No hay facturas PENDIENTE con hallazgo FACTURA sin resolver" in line for line in lines)
        assert _FakeDownloader.instances == []


class TestDownloadMedicationSheets:
    async def test_downloads_the_sheets_of_pending_invoices_that_have_an_admission(self, sihos):
        await _pending(sihos, "1", "HISTORIA", admission="ADM1")
        await _pending(sihos, "2", "HISTORIA", admission=None)  # nothing to look up in SIHOS
        await _pending(sihos, "3", "HISTORIA", status="PRESENTE")
        sihos["doc_type_id"] = (await _doc_type(sihos, "HISTORIA")).id

        lines = await _run("DOWNLOAD_MEDICATION_SHEETS", sihos)

        (downloader,) = _FakeDownloader.instances
        assert downloader.calls == [("sheets", ([("1", "ADM1", "CC", "id1")], "HCU"))]
        assert downloader.kwargs["invoice_prefix"] == ""  # the sheet prefix goes with the call, not the constructor
        assert downloader.kwargs["password"] == "secreto"
        assert any("Descargando 1 factura(s) desde SIHOS" in line for line in lines)
        assert any("Descarga completada" in line for line in lines)

    async def test_a_document_type_without_prefix_names_the_files_by_its_code(self, sihos):
        await _pending(sihos, "1", "SOPORTE")
        sihos["doc_type_id"] = (await _doc_type(sihos, "SOPORTE")).id

        await _run("DOWNLOAD_MEDICATION_SHEETS", sihos)

        assert _FakeDownloader.instances[0].calls == [("sheets", ([("1", "ADM1", "CC", "id1")], "SOPORTE"))]

    async def test_says_so_when_no_invoice_has_an_admission(self, sihos):
        await _pending(sihos, "1", "HISTORIA", admission=None)
        sihos["doc_type_id"] = (await _doc_type(sihos, "HISTORIA")).id

        lines = await _run("DOWNLOAD_MEDICATION_SHEETS", sihos)

        assert any("número de admisión" in line for line in lines)
        assert _FakeDownloader.instances == []


def test_the_fake_mirrors_the_real_downloader_constructor():
    """Guard: the fake mirrors the real constructor arguments, so a renamed argument fails here first."""
    import inspect

    from app.core.downloader import SihosDownloader

    assert set(inspect.signature(SihosDownloader.__init__).parameters) - {"self"} == {
        "user",
        "password",
        "base_url",
        "hospital_nit",
        "invoice_prefix",
        "invoice_id_prefix",
        "invoice_doc_code",
        "output_dir",
    }
