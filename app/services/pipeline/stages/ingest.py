"""Ingestion stages: load the SIHOS Excel and re-apply service mappings."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.pipeline.context import StageContext
from app.services.pipeline.helpers import get_executor, sihos_excel_name
from app.services.pipeline.logfmt import plog
from app.services.pipeline.registry import stage


@stage("LOAD_AND_PROCESS")
async def _load_and_process(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Load SIHOS Excel and upsert invoices. Requires ctx['file_bytes'] and ctx['period_code']."""
    from app.services.billing import ingest

    file_bytes: bytes | None = ctx.get("file_bytes")

    if not file_bytes:
        yield plog("ERROR", "No se proporcionó el archivo Excel de SIHOS.")
        return

    yield plog("INFO", "Leyendo Excel SIHOS...")
    result = await ingest(file_bytes, ctx["institution"], ctx["period"].id, ctx["db"])
    yield plog("INFO", f"Insertadas: {result['inserted']} facturas")
    if result["skipped"]:
        yield plog("WARN", f"Omitidas (admin sin mapear): {result['skipped']}")
    for admin in result["unknown_admins"]:
        yield plog("WARN", f"Administradora sin mapear: {admin}")
    for contract in result["unknown_contracts"]:
        yield plog("WARN", f"Contrato sin mapear: {contract}")
    for service in result["unknown_services"]:
        yield plog("WARN", f"Servicio sin reclasificar (GENERAL): {service}")

    # Guardar el Excel para permitir re-categorización posterior
    base_path: Path = ctx["base_path"]
    executor = get_executor()

    def _save_excel(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    excel_name = sihos_excel_name(ctx["institution"], ctx["period"])
    await executor(_save_excel, base_path / excel_name, file_bytes)
    yield plog("INFO", f"Excel guardado como {excel_name} para futuras re-categorizaciones.")


@stage("RECATEGORIZE_SERVICES")
async def _recategorize_services(ctx: StageContext) -> AsyncGenerator[str, None]:
    """Re-apply current service mappings to all invoices in the period without reloading from SIHOS."""
    from app.repositories.invoice_repo import InvoiceRepo
    from app.repositories.rules_repo import RulesRepo
    from app.services.billing import load_excel, _normalize

    base_path: Path = ctx["base_path"]
    institution = ctx["institution"]
    period = ctx["period"]
    db: AsyncSession = ctx["db"]

    excel_path = base_path / sihos_excel_name(institution, period)
    if not excel_path.exists():
        yield plog("ERROR", "No se encontró el Excel guardado. Ejecuta primero 'Cargar reporte SIHOS'.")
        return

    yield plog("INFO", f"Leyendo Excel guardado: {excel_path.name}")
    executor = get_executor()
    file_bytes = await executor(excel_path.read_bytes)
    raw_df = await executor(load_excel, file_bytes)
    df = await executor(_normalize, raw_df)

    # Cargar mapeos actuales de servicios
    rules_repo = RulesRepo(db)

    from sqlalchemy import select as sa_select
    from app.models.institution import Service as ServiceModel

    result = await db.execute(sa_select(ServiceModel).where(ServiceModel.institution_id == institution.id))
    service_map: dict[str, int | None] = {s.raw_service: s.service_type_id for s in result.scalars().all()}

    service_types = await rules_repo.get_service_types()
    priority_map: dict[int, int] = {st.id: st.priority for st in service_types}

    # Recalcular service_type_id por factura
    invoice_service_ids: dict[str, set[int]] = {}
    for _, row in df.iterrows():
        raw_service = str(row.get("SERVICIO", "") or "").strip()
        invoice_number = str(row["FACTURA"])
        st_id = service_map.get(raw_service)
        if st_id is not None:
            invoice_service_ids.setdefault(invoice_number, set()).add(st_id)

    updates: dict[str, int | None] = {}
    for invoice_number, st_ids in invoice_service_ids.items():
        best = max(st_ids, key=lambda sid: priority_map.get(sid, 0)) if st_ids else None
        updates[invoice_number] = best

    yield plog("INFO", f"Facturas a actualizar: {len(updates)}")

    inv_repo = InvoiceRepo(db)
    updated = await inv_repo.batch_update_service_type(period.id, updates)
    await db.commit()
    yield plog("INFO", f"Tipos de servicio actualizados: {updated} factura(s).")
