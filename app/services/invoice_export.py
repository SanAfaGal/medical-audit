"""Excel export of a period's invoices."""

from __future__ import annotations

import io
from collections.abc import Iterable

from app.models.invoice import Invoice

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

HEADERS = [
    "Período",
    "Hospital",
    "Factura",
    "Fecha",
    "Tipo Doc.",
    "Número Doc.",
    "Paciente",
    "Admisión",
    "Administradora",
    "Tipo Contrato",
    "Contrato",
    "Servicio",
    "Operación",
    "Estado",
    "Hallazgos",
]


def invoice_row(inv: Invoice) -> list[object]:
    """One spreadsheet row, in the order of :data:`HEADERS`. Relations must already be loaded."""
    agreement = inv.agreement
    administrator = contract_type = contract = ""
    if agreement:
        if agreement.administrator:
            administrator = agreement.administrator.canonical_name or agreement.administrator.raw_name
        if agreement.contract_type:
            contract_type = agreement.contract_type.name
        if agreement.contract:
            contract = agreement.contract.canonical_name or agreement.contract.raw_name
    return [
        inv.period.period_label if inv.period else "",
        inv.period.institution.display_name if inv.period and inv.period.institution else "",
        inv.invoice_number,
        inv.date.isoformat() if inv.date else "",
        inv.id_type,
        inv.id_number,
        inv.patient_name,
        inv.admission or "",
        administrator,
        contract_type,
        contract,
        inv.service_type.display_name if inv.service_type else "",
        inv.employee or "",
        inv.folder_status.status if inv.folder_status else "",
        ", ".join(mf.doc_type.code for mf in inv.missing_files if mf.resolved_at is None and mf.doc_type),
    ]


def build_workbook(invoices: Iterable[Invoice]) -> bytes:
    """The ``.xlsx`` file (write-only mode: constant memory however many invoices there are)."""
    import openpyxl

    workbook = openpyxl.Workbook(write_only=True)
    sheet = workbook.create_sheet("Facturas")
    sheet.append(HEADERS)
    for inv in invoices:
        sheet.append(invoice_row(inv))
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
