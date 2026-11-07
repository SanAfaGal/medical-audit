"""Pure rules behind REVISAR_SOBRANTES and CHECK_REQUIRED_DOCS: no database, no PDFs, just folders and plain data."""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.pipeline.required_docs import find_missing_documents
from app.services.pipeline.surplus import (
    NO_SUGGESTION,
    Faltante,
    InvoiceInfo,
    match_invoice,
    scan_surplus,
    snapshot_invoices,
    suggest_documents,
    surplus_files,
)


def _faltante(doc_type_id: int, code: str, prefix: str | None) -> Faltante:
    return {"doc_type_id": doc_type_id, "code": code, "description": code.title(), "prefix": prefix}


def _info(
    number: str = "HSL1", service_type_id: int | None = 1, faltantes: list[Faltante] | None = None
) -> InvoiceInfo:
    return {
        "id": 10,
        "invoice_number": number,
        "service_type_id": service_type_id,
        "admin_type": "EVENTO",
        "service_type": "General",
        "faltantes": faltantes or [],
    }


class TestMatchInvoice:
    invoices = {"HSL1": _info("HSL1"), "HSL20": _info("HSL20")}

    @pytest.mark.parametrize("folder", ["HSL1", "hsl1"])
    def test_exact_name_in_any_case(self, folder):
        assert match_invoice(folder, self.invoices) is self.invoices["HSL1"]

    @pytest.mark.parametrize("folder", ["HSL1_extra", "HSL1 extra", "PREFIJO-HSL1"])
    def test_number_followed_by_text_or_at_the_end(self, folder):
        assert match_invoice(folder, self.invoices) is self.invoices["HSL1"]

    def test_a_longer_number_does_not_match_a_shorter_one(self):
        assert match_invoice("HSL12", self.invoices) is None

    def test_unknown_folder(self):
        assert match_invoice("OTRA", self.invoices) is None


class TestSurplusFiles:
    def test_files_without_a_required_prefix_are_surplus(self, tmp_path: Path):
        for name in ("FEV_1.pdf", "hcu_1.pdf", "raro.pdf", "otro.txt"):
            (tmp_path / name).write_bytes(b"x")
        (tmp_path / "subcarpeta").mkdir()

        assert sorted(f.name for f in surplus_files(tmp_path, ["FEV", "HCU"])) == ["otro.txt", "raro.pdf"]

    def test_no_required_prefixes_means_everything_is_surplus(self, tmp_path: Path):
        (tmp_path / "a.pdf").write_bytes(b"x")
        assert [f.name for f in surplus_files(tmp_path, [])] == ["a.pdf"]


class TestSuggestDocuments:
    def test_nothing_missing_nothing_to_suggest(self):
        assert suggest_documents(["x.pdf"], []) == {}

    def test_one_file_and_one_missing_document_is_a_high_confidence_match(self):
        result = suggest_documents(["x.pdf"], [_faltante(5, "HISTORIA", "HCU")])
        assert result == {
            "x.pdf": {
                "suggested_doc_type_id": 5,
                "suggested_code": "HISTORIA",
                "suggested_prefix": "HCU",
                "confidence": "high",
            }
        }

    def test_several_files_pick_the_most_similar_prefix_with_low_confidence(self):
        faltantes = [_faltante(1, "HISTORIA", "HCU"), _faltante(2, "EPICRISIS", "EPI")]
        result = suggest_documents(["EPI_2.pdf", "zzz.pdf"], faltantes)

        assert result["EPI_2.pdf"]["suggested_code"] == "EPICRISIS"
        assert result["EPI_2.pdf"]["confidence"] == "low"
        assert "zzz.pdf" not in result  # nothing resembles it enough

    def test_missing_documents_without_prefix_cannot_be_matched(self):
        assert suggest_documents(["a.pdf", "b.pdf"], [_faltante(1, "SOPORTE", None), _faltante(2, "OTRO", None)]) == {}


class TestScanSurplus:
    def _folder(self, root: Path, name: str, *files: str) -> Path:
        folder = root / name
        folder.mkdir()
        for f in files:
            (folder / f).write_bytes(b"x")
        return folder

    def test_reports_surplus_with_suggestions_and_log_lines(self, tmp_path: Path):
        folder = self._folder(tmp_path, "HSL1", "FEV_1.pdf", "raro.pdf")
        self._folder(tmp_path, "HSL2", "FEV_2.pdf")  # nothing surplus
        self._folder(tmp_path, "DESCONOCIDA", "x.pdf")  # not an invoice
        invoices = {"HSL1": _info("HSL1", faltantes=[_faltante(5, "HISTORIA", "HCU")]), "HSL2": _info("HSL2")}
        progress: list[tuple[int, int]] = []

        scan = scan_surplus(tmp_path, invoices, {1: [7]}, {7: "FEV"}, lambda d, t, _n: progress.append((d, t)), None)

        assert scan.total == 1
        assert len(scan.items) == 1
        item = scan.items[0]
        assert item["folder_path"] == str(folder) and item["invoice_number"] == "HSL1"
        assert item["sobrantes"] == [
            {
                "filename": "raro.pdf",
                "suggested_doc_type_id": 5,
                "suggested_code": "HISTORIA",
                "suggested_prefix": "HCU",
                "confidence": "high",
            }
        ]
        assert item["faltantes"] == [{"doc_type_id": 5, "code": "HISTORIA", "description": "Historia"}]
        assert "[HSL1]" in scan.log_lines[0] and "1 sobrante(s), 1 faltante(s)" in scan.log_lines[0]
        assert progress[-1] == (3, 3)

    def test_surplus_without_missing_documents_has_no_suggestion(self, tmp_path: Path):
        self._folder(tmp_path, "HSL1", "FEV_1.pdf", "raro.pdf")
        scan = scan_surplus(tmp_path, {"HSL1": _info("HSL1")}, {1: [7]}, {7: "FEV"}, lambda *_: None, None)
        assert scan.items[0]["sobrantes"] == [{"filename": "raro.pdf", **NO_SUGGESTION}]

    def test_invoice_without_service_type_treats_every_file_as_surplus(self, tmp_path: Path):
        self._folder(tmp_path, "HSL1", "FEV_1.pdf")
        scan = scan_surplus(
            tmp_path, {"HSL1": _info("HSL1", service_type_id=None)}, {1: [7]}, {7: "FEV"}, lambda *_: None, None
        )
        assert scan.total == 1

    def test_cancel_stops_the_scan(self, tmp_path: Path):
        self._folder(tmp_path, "HSL1", "raro.pdf")
        cancel = threading.Event()
        cancel.set()
        scan = scan_surplus(tmp_path, {"HSL1": _info("HSL1")}, {}, {}, lambda *_: None, cancel)
        assert scan.items == [] and scan.total == 0


class TestSnapshotInvoices:
    def test_keeps_only_open_findings_and_plain_data(self):
        doc = SimpleNamespace(id=5, code="HISTORIA", description="Historia clínica", prefix="HCU")
        invoice = SimpleNamespace(
            id=1,
            invoice_number="hsl1",
            service_type_id=3,
            agreement=SimpleNamespace(contract_type=SimpleNamespace(name="EVENTO")),
            service_type=SimpleNamespace(display_name="General"),
            missing_files=[
                SimpleNamespace(resolved_at=None, doc_type=doc),
                SimpleNamespace(resolved_at="2024-01-01", doc_type=doc),  # already resolved
            ],
        )

        snapshot = snapshot_invoices([invoice])

        assert list(snapshot) == ["HSL1"]  # keyed by upper-cased number
        assert snapshot["HSL1"]["admin_type"] == "EVENTO" and snapshot["HSL1"]["service_type"] == "General"
        assert snapshot["HSL1"]["faltantes"] == [
            {"doc_type_id": 5, "code": "HISTORIA", "description": "Historia clínica", "prefix": "HCU"}
        ]

    def test_invoice_without_agreement_or_service_type(self):
        invoice = SimpleNamespace(
            id=1, invoice_number="X1", service_type_id=None, agreement=None, service_type=None, missing_files=[]
        )
        info = snapshot_invoices([invoice])["X1"]
        assert info["admin_type"] is None and info["service_type"] is None and info["faltantes"] == []


class TestFindMissingDocuments:
    def test_reports_only_the_invoices_that_lack_a_required_document(self, tmp_path: Path):
        for folder, files in {"HSL1": ["FEV_1.pdf"], "HSL2": ["FEV_2.pdf", "HCU_2.pdf"], "HSL3": []}.items():
            (tmp_path / folder).mkdir()
            for f in files:
                (tmp_path / folder / f).write_bytes(b"x")
        invoices = [(1, "1", 10), (2, "2", 10), (3, "3", None)]  # invoice 3 has no service type: nothing required
        progress: list[int] = []

        missing = find_missing_documents(
            tmp_path,
            "HSL",
            invoices,
            {10: [100, 200]},
            {100: ["FEV"], 200: ["HCU"]},
            lambda d, t, _n: progress.append(d),
            None,
        )

        assert missing == {1: ("1", [200])}
        assert progress == [1, 2, 3]

    def test_cancel_stops_early(self, tmp_path: Path):
        cancel = threading.Event()
        cancel.set()
        assert (
            find_missing_documents(tmp_path, "HSL", [(1, "1", 10)], {10: [1]}, {1: ["FEV"]}, lambda *_: None, cancel)
            == {}
        )
