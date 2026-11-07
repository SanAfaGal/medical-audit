"""Stages that fetch missing documents from SIHOS (browser) and Google Drive."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
import json

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import INVOICE_NUMBER_RE, get_executor, sihos_url_error
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage
from app.services.stage_events import progress_line, run_with_events


@stage("DOWNLOAD_INVOICES_FROM_SIHOS")
async def _download_invoices_from_sihos(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Download the factura PDF from SIHOS for PENDIENTE invoices with an unresolved finding of the chosen doc type."""
    from sqlalchemy import select

    from app.core.downloader import SihosDownloader

    from app.crypto import decrypt
    from app.models.finding import MissingFile
    from app.models.invoice import Invoice
    from app.models.rules import DocType, FolderStatus

    institution = ctx["institution"]
    stage_path: Path = ctx["stage_path"]
    db = ctx["db"]
    period = ctx["period"]
    doc_type_id: int = ctx.get("doc_type_id", 0)

    if not doc_type_id:
        yield plog(
            "ERROR", "No se seleccionó un tipo de documento. Elige uno en el panel antes de ejecutar esta etapa."
        )
        return

    if not institution.sihos_user or not institution.sihos_password:
        yield plog("ERROR", "Credenciales SIHOS no configuradas en la institución.")
        return

    if url_error := sihos_url_error(institution):
        yield plog("ERROR", url_error)
        return

    doc_type_result = await db.execute(select(DocType).where(DocType.id == doc_type_id))
    doc_type = doc_type_result.scalar_one_or_none()
    if not doc_type:
        yield plog("ERROR", f"Tipo de documento con id={doc_type_id} no encontrado.")
        return

    yield plog("INFO", f"Tipo de documento: {doc_type.code} — prefijo {doc_type.prefix or '(sin prefijo)'}")

    stmt = (
        select(Invoice.invoice_number)
        .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
        .join(MissingFile, MissingFile.invoice_id == Invoice.id)
        .where(Invoice.audit_period_id == period.id)
        .where(FolderStatus.status == "PENDIENTE")
        .where(MissingFile.doc_type_id == doc_type_id)
        .where(MissingFile.resolved_at.is_(None))
        .distinct()
    )
    result = await db.execute(stmt)
    invoice_numbers = [row.invoice_number for row in result.all()]

    if not invoice_numbers:
        yield plog("INFO", f"No hay facturas PENDIENTE con hallazgo {doc_type.code} sin resolver.")
        return

    yield plog("INFO", f"Descargando {len(invoice_numbers)} factura(s) desde SIHOS...")

    password = decrypt(institution.sihos_password)
    downloader = SihosDownloader(
        user=institution.sihos_user,
        password=password,
        base_url=institution.sihos_base_url or "",
        hospital_nit=institution.nit,
        invoice_prefix=doc_type.prefix or "",
        invoice_id_prefix=institution.invoice_id_prefix or "",
        invoice_doc_code=institution.sihos_doc_code or "",
        output_dir=stage_path,
    )

    yield progress_line(0, len(invoice_numbers), label="Facturas")
    async for line in run_with_events(
        lambda event_fn, cancel_event: downloader.run_from_list(
            invoice_numbers, event_fn=event_fn, cancel_event=cancel_event
        ),
        ctx,
        noun="factura",
        verb="Descargando",
        label="Facturas",
    ):
        yield line
    yield plog("INFO", "Descarga SIHOS completada.")


@stage("DOWNLOAD_MEDICATION_SHEETS")
async def _download_medication_sheets(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Download PDFs for PENDIENTE invoices with an unresolved finding of the chosen doc type."""
    from sqlalchemy import select

    from app.core.downloader import SihosDownloader

    from app.crypto import decrypt
    from app.models.finding import MissingFile
    from app.models.invoice import Invoice
    from app.models.rules import DocType, FolderStatus

    institution = ctx["institution"]
    stage_path: Path = ctx["stage_path"]
    db = ctx["db"]
    period = ctx["period"]
    doc_type_id: int = ctx.get("doc_type_id", 0)

    if not doc_type_id:
        yield plog(
            "ERROR", "No se seleccionó un tipo de documento. Elige uno en el panel antes de ejecutar esta etapa."
        )
        return

    if not institution.sihos_user or not institution.sihos_password:
        yield plog("ERROR", "Credenciales SIHOS no configuradas en la institución.")
        return

    if url_error := sihos_url_error(institution):
        yield plog("ERROR", url_error)
        return

    doc_type_result = await db.execute(select(DocType).where(DocType.id == doc_type_id))
    doc_type = doc_type_result.scalar_one_or_none()
    if not doc_type:
        yield plog("ERROR", f"Tipo de documento con id={doc_type_id} no encontrado.")
        return

    yield plog("INFO", f"Tipo de documento seleccionado: {doc_type.code} — {doc_type.description}")

    stmt = (
        select(Invoice.invoice_number, Invoice.admission, Invoice.id_type, Invoice.id_number)
        .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
        .join(MissingFile, MissingFile.invoice_id == Invoice.id)
        .where(Invoice.audit_period_id == period.id)
        .where(Invoice.admission.isnot(None))
        .where(FolderStatus.status == "PENDIENTE")
        .where(MissingFile.doc_type_id == doc_type_id)
        .where(MissingFile.resolved_at.is_(None))
        .distinct()
    )
    result = await db.execute(stmt)
    targets: list[tuple[str, str, str, str]] = [
        (row.invoice_number, row.admission, row.id_type, row.id_number) for row in result.all()
    ]

    if not targets:
        yield plog("INFO", f"No hay facturas PENDIENTE con hallazgo {doc_type.code} sin resolver y número de admisión.")
        return

    yield plog("INFO", f"Descargando {len(targets)} factura(s) desde SIHOS...")

    password = decrypt(institution.sihos_password)
    downloader = SihosDownloader(
        user=institution.sihos_user,
        password=password,
        base_url=institution.sihos_base_url or "",
        hospital_nit=institution.nit,
        invoice_prefix="",
        invoice_id_prefix=institution.invoice_id_prefix or "",
        invoice_doc_code=institution.sihos_doc_code or "",
        output_dir=stage_path,
    )

    file_prefix = doc_type.prefix or doc_type.code
    yield progress_line(0, len(targets), label="Facturas")
    async for line in run_with_events(
        lambda event_fn, cancel_event: downloader.run_medication_sheets(
            targets, file_prefix, event_fn=event_fn, cancel_event=cancel_event
        ),
        ctx,
        noun="factura",
        verb="Descargando",
        label="Facturas",
    ):
        yield line
    yield plog("INFO", "Descarga completada.")


@stage("DOWNLOAD_DRIVE")
async def _download_drive(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Download invoice folders from Google Drive."""
    from app.core.drive import DriveSync

    from app.crypto import decrypt
    from app.repositories.invoice_repo import InvoiceRepo

    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not institution.drive_credentials_enc:
        yield plog("ERROR", "Credenciales de Drive no configuradas en la institución.")
        return

    manual_list: list[str] | None = ctx.get("invoice_numbers") or None
    inv_repo: InvoiceRepo | None = None

    if manual_list is not None:
        # The numbers end up inside Drive search queries: accept only plain identifiers.
        missing = [n for n in manual_list if INVOICE_NUMBER_RE.fullmatch(n)]
        if len(missing) != len(manual_list):
            yield plog("WARN", f"Se ignoraron {len(manual_list) - len(missing)} número(s) con caracteres no válidos.")
        yield plog("INFO", f"Modo manual: {len(missing)} carpeta(s) ingresadas.")
        if not missing:
            return
    else:
        inv_repo = InvoiceRepo(db)
        missing = await inv_repo.get_invoice_numbers_by_status(period.id, "FALTANTE")
        yield plog("INFO", f"Facturas FALTANTE a buscar en Drive: {len(missing)}")
        if not missing:
            yield plog("INFO", "No hay facturas FALTANTE.")
            return

    creds = json.loads(decrypt(institution.drive_credentials_enc))
    drive = DriveSync(credentials_dict=creds)

    stage_path.mkdir(parents=True, exist_ok=True)
    # Search by bare invoice number so folders with separators or extra text
    # (e.g. "FE-12345", "FE12345 YA") are matched. download_missing_dirs batches
    # the search internally (see core/drive.py::_batch_search_folders) and reports
    # progress per Drive folder through the shared event contract.
    result: dict[str, list[str]] = {}

    def _sync(event_fn, cancel_event) -> None:
        result["found"] = drive.download_missing_dirs(missing, stage_path, event_fn=event_fn, cancel_event=cancel_event)

    async for line in run_with_events(_sync, ctx, noun="carpeta", verb="Descargando", label="Carpetas"):
        yield line
    downloaded = result.get("found", [])
    yield plog("INFO", f"Facturas encontradas en Drive: {len(downloaded)}/{len(missing)}")

    if manual_list is None and downloaded:
        assert inv_repo is not None  # manual_list is None → the else branch above set it
        updated = await inv_repo.batch_update_folder_status(period.id, downloaded, "PRESENTE")
        await db.commit()
        yield plog("INFO", f"Facturas actualizadas a PRESENTE: {updated}")


@stage("DOWNLOAD_MISSING_DOCS")
async def _download_missing_docs(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Download specific missing document files from Google Drive."""
    from app.core.drive import DriveSync

    from app.crypto import decrypt
    from app.repositories.finding_repo import MissingFileRepo
    from app.repositories.rules_repo import RulesRepo

    executor = get_executor()
    stage_path: Path = ctx["stage_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db = ctx["db"]

    if not institution.drive_credentials_enc:
        yield plog("ERROR", "Credenciales de Drive no configuradas en la institución.")
        return

    finding_repo = MissingFileRepo(db)
    rules_repo = RulesRepo(db)

    grouped = await finding_repo.get_findings_grouped_by_invoice(period.id)
    yield plog("INFO", f"Facturas con documentos faltantes: {len(grouped)}")

    if not grouped:
        yield plog("INFO", "No hay documentos faltantes.")
        return

    doc_types = await rules_repo.get_doc_types()
    prefix_by_code = {dt.code: dt.prefix for dt in doc_types if dt.prefix}

    creds = json.loads(decrypt(institution.drive_credentials_enc))
    drive = DriveSync(credentials_dict=creds)

    id_prefix = institution.invoice_id_prefix or ""

    # Build all (file_name, dest_folder) pairs upfront across every invoice.
    # dest_folder is created on demand by DriveSync.download_file.
    download_requests: list[tuple[str, Path]] = [
        (
            f"{prefix_by_code[code]}_{institution.nit}_{id_prefix}{invoice_number}.pdf",
            stage_path / (id_prefix + invoice_number),
        )
        for invoice_number, doc_codes in grouped.items()
        for code in doc_codes
        if code in prefix_by_code
    ]

    yield plog("INFO", f"Buscando {len(download_requests)} documento(s) en Drive...")
    found = not_found = 0
    total_docs = len(download_requests)
    yield progress_line(0, total_docs, label="Documentos")
    for i, request in enumerate(download_requests, 1):
        f, nf = await executor(drive.download_specific_files, [request])
        found += f
        not_found += nf
        status = "✓" if f else "✗ no encontrado"
        yield plog("INFO", f"[{i}/{total_docs}] {request[0]} — {status}")
        yield progress_line(i, total_docs, request[0], "Documentos")
    yield plog("INFO", f"Encontrados: {found}, no encontrados: {not_found}")
