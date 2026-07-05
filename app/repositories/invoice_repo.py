"""Async repository for invoices and audit periods."""

from __future__ import annotations

import datetime
import logging

from sqlalchemy import delete, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.finding import MissingFile
from app.models.institution import Administrator, Agreement, Contract, ContractType
from app.models.invoice import Invoice
from app.models.period import AuditPeriod
from app.models.rules import FolderStatus, ServiceType

logger = logging.getLogger(__name__)


class InvoiceRepo:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Periods
    # ------------------------------------------------------------------

    async def get_periods(self, institution_id: int) -> list[AuditPeriod]:
        result = await self.db.execute(
            select(AuditPeriod)
            .where(AuditPeriod.institution_id == institution_id)
            .order_by(AuditPeriod.date_from.desc())
        )
        return list(result.scalars().all())

    async def get_or_create_period(
        self,
        institution_id: int,
        date_from: datetime.date,
        date_to: datetime.date,
        period_label: str,
    ) -> AuditPeriod:
        stmt = (
            pg_insert(AuditPeriod)
            .values(
                institution_id=institution_id,
                date_from=date_from,
                date_to=date_to,
                period_label=period_label,
            )
            .on_conflict_do_nothing(index_elements=["institution_id", "date_from", "date_to", "period_label"])
            .returning(AuditPeriod)
        )
        result = await self.db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            result2 = await self.db.execute(
                select(AuditPeriod).where(
                    AuditPeriod.institution_id == institution_id,
                    AuditPeriod.date_from == date_from,
                    AuditPeriod.date_to == date_to,
                    AuditPeriod.period_label == period_label,
                )
            )
            row = result2.scalar_one()
        return row

    async def get_period_by_id(self, period_id: int) -> AuditPeriod | None:
        return await self.db.get(AuditPeriod, period_id)

    async def delete_period(self, period_id: int) -> None:
        """Delete a period and cascade-delete all invoices and missing files."""
        period = await self.db.get(AuditPeriod, period_id)
        if period:
            await self.db.delete(period)
            await self.db.flush()

    # ------------------------------------------------------------------
    # Invoices
    # ------------------------------------------------------------------

    async def get_by_id(self, invoice_id: int) -> Invoice | None:
        return await self.db.get(Invoice, invoice_id)

    async def upsert_invoice(self, audit_period_id: int, invoice_number: str, data: dict) -> Invoice:
        """Insert invoice; on conflict do nothing (preserve existing missing files)."""
        stmt = (
            pg_insert(Invoice)
            .values(audit_period_id=audit_period_id, invoice_number=invoice_number, **data)
            .on_conflict_do_nothing(index_elements=["audit_period_id", "invoice_number"])
            .returning(Invoice)
        )
        result = await self.db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            result2 = await self.db.execute(
                select(Invoice).where(
                    Invoice.audit_period_id == audit_period_id,
                    Invoice.invoice_number == invoice_number,
                )
            )
            row = result2.scalar_one()
        return row

    async def filter_invoices(
        self,
        audit_period_id: int,
        folder_status_id: int | None = None,
        service_type_id: int | None = None,
        admin_canonical: str | None = None,
        admin_type: str | None = None,
        contract_canonical: str | None = None,
        search: str | None = None,
        has_finding_doc_type_id: int | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[Invoice], int]:
        """Return paginated invoices with eager-loaded FK relationships, plus total count."""
        q = select(Invoice).where(Invoice.audit_period_id == audit_period_id)

        if folder_status_id is not None:
            q = q.where(Invoice.folder_status_id == folder_status_id)
        if service_type_id is not None:
            q = q.where(Invoice.service_type_id == service_type_id)

        if admin_canonical is not None or admin_type is not None or contract_canonical is not None:
            q = q.join(Agreement, Invoice.agreement_id == Agreement.id)
            if admin_canonical is not None:
                q = q.join(Administrator, Agreement.administrator_id == Administrator.id).where(
                    Administrator.canonical_name == admin_canonical
                )
            if admin_type is not None:
                q = q.join(ContractType, Agreement.contract_type_id == ContractType.id).where(
                    ContractType.name == admin_type
                )
            if contract_canonical is not None:
                q = q.join(Contract, Agreement.contract_id == Contract.id).where(
                    Contract.canonical_name == contract_canonical
                )

        if search:
            terms = [t.strip() for t in search.split(";") if t.strip()]
            if terms:
                q = q.where(
                    or_(
                        *[
                            or_(
                                Invoice.invoice_number.ilike(f"%{t.upper()}%"),
                                Invoice.patient_name.ilike(f"%{t}%"),
                            )
                            for t in terms
                        ]
                    )
                )
        if has_finding_doc_type_id is not None:
            subq = select(MissingFile.id).where(
                MissingFile.invoice_id == Invoice.id,
                MissingFile.doc_type_id == has_finding_doc_type_id,
                MissingFile.resolved_at.is_(None),
            )
            q = q.where(exists(subq))

        count_q = select(func.count()).select_from(q.subquery())
        total_result = await self.db.execute(count_q)
        total = total_result.scalar_one()

        q = (
            q.order_by(Invoice.invoice_number)
            .offset((page - 1) * page_size)
            .limit(page_size)
            .options(
                selectinload(Invoice.folder_status),
                selectinload(Invoice.service_type),
                selectinload(Invoice.agreement).selectinload(Agreement.administrator),
                selectinload(Invoice.agreement).selectinload(Agreement.contract),
                selectinload(Invoice.agreement).selectinload(Agreement.contract_type),
                selectinload(Invoice.missing_files),
            )
        )
        result = await self.db.execute(q)
        return list(result.scalars().all()), total

    async def update_folder_status(self, invoice_id: int, folder_status_id: int) -> None:
        result = await self.db.execute(
            update(Invoice)
            .where(Invoice.id == invoice_id)
            .values(folder_status_id=folder_status_id)
            .returning(Invoice.folder_status_id)
        )
        await self.db.flush()
        actual = result.scalar_one_or_none()
        if actual is not None and actual != folder_status_id:
            logger.warning(
                "update_folder_status: trigger overrode requested folder_status_id=%s with %s for invoice_id=%s",
                folder_status_id, actual, invoice_id,
            )

    async def batch_update_status(self, invoice_ids: list[int], folder_status_id: int) -> None:
        await self.db.execute(
            update(Invoice).where(Invoice.id.in_(invoice_ids)).values(folder_status_id=folder_status_id)
        )
        await self.db.flush()

    async def batch_update_service_type(self, period_id: int, updates: dict[str, int | None]) -> int:
        """Update service_type_id for invoices by invoice_number. Returns count updated."""
        count = 0
        for invoice_number, service_type_id in updates.items():
            result = await self.db.execute(
                update(Invoice)
                .where(Invoice.audit_period_id == period_id, Invoice.invoice_number == invoice_number)
                .values(service_type_id=service_type_id)
            )
            count += result.rowcount  # type: ignore[attr-defined]
        await self.db.flush()
        return count

    async def delete_invoice(self, invoice_id: int) -> bool:
        invoice = await self.db.get(Invoice, invoice_id)
        if not invoice:
            return False
        await self.db.delete(invoice)
        await self.db.flush()
        return True

    async def batch_delete_invoices(self, invoice_ids: list[int]) -> int:
        result = await self.db.execute(delete(Invoice).where(Invoice.id.in_(invoice_ids)))
        await self.db.flush()
        return result.rowcount  # type: ignore[attr-defined, no-any-return]

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    async def get_all_invoice_numbers(self, period_id: int) -> set[str]:
        """Return all invoice numbers for a period regardless of folder status."""
        q = select(Invoice.invoice_number).where(Invoice.audit_period_id == period_id)
        result = await self.db.execute(q)
        return set(result.scalars().all())

    async def get_invoice_numbers_by_status(self, period_id: int, status_code: str) -> list[str]:
        """Return invoice numbers for a period filtered by folder status code."""
        q = (
            select(Invoice.invoice_number)
            .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
            .where(
                Invoice.audit_period_id == period_id,
                FolderStatus.status == status_code,
            )
        )
        result = await self.db.execute(q)
        return list(result.scalars().all())

    async def get_invoices_by_status_code(self, period_id: int, status_code: str) -> list[Invoice]:
        """Return Invoice objects for a period filtered by folder status code."""
        q = (
            select(Invoice)
            .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
            .where(
                Invoice.audit_period_id == period_id,
                FolderStatus.status == status_code,
            )
        )
        result = await self.db.execute(q)
        return list(result.scalars().all())

    async def batch_update_folder_status(self, period_id: int, invoice_numbers: list[str], status_code: str) -> int:
        """Update folder_status for invoices matching the given numbers.

        Returns:
            Number of rows updated.
        """
        if not invoice_numbers:
            return 0
        fs_result = await self.db.execute(select(FolderStatus).where(FolderStatus.status == status_code))
        fs = fs_result.scalar_one_or_none()
        if not fs:
            raise ValueError(f"FolderStatus '{status_code}' not found")
        result = await self.db.execute(
            update(Invoice)
            .where(
                Invoice.audit_period_id == period_id,
                Invoice.invoice_number.in_(invoice_numbers),
            )
            .values(folder_status_id=fs.id)
            .returning(Invoice.invoice_number, Invoice.folder_status_id)
        )
        rows = result.all()
        await self.db.flush()
        corrected = [num for num, actual_status_id in rows if actual_status_id != fs.id]
        if corrected:
            logger.warning(
                "batch_update_folder_status: trigger overrode requested status %r for %d invoice(s): %s",
                status_code, len(corrected), corrected,
            )
        return len(rows)

    async def get_organizable_invoices(self, period_id: int) -> list[Invoice]:
        """Return PRESENTE invoices with no unresolved missing files, with agreement loaded."""
        q = (
            select(Invoice)
            .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
            .outerjoin(
                MissingFile,
                (MissingFile.invoice_id == Invoice.id) & (MissingFile.resolved_at.is_(None)),
            )
            .where(
                Invoice.audit_period_id == period_id,
                FolderStatus.status == "PRESENTE",
                MissingFile.id.is_(None),
            )
            .options(
                selectinload(Invoice.agreement).selectinload(Agreement.administrator),
                selectinload(Invoice.agreement).selectinload(Agreement.contract),
                selectinload(Invoice.agreement).selectinload(Agreement.contract_type),
            )
        )
        result = await self.db.execute(q)
        return list(result.scalars().all())

    async def batch_update_to_auditada(self, period_id: int, invoice_numbers: list[str]) -> int:
        """Update invoices to AUDITADA status by invoice number.

        Returns:
            Number of rows updated.
        """
        return await self.batch_update_folder_status(period_id, invoice_numbers, "AUDITADA")

    async def get_invoice_ids(
        self,
        audit_period_id: int,
        folder_status_id: int | None = None,
        service_type_id: int | None = None,
        admin_canonical: str | None = None,
        admin_type: str | None = None,
        contract_canonical: str | None = None,
        search: str | None = None,
        has_finding_doc_type_id: int | None = None,
    ) -> list[int]:
        """Return all invoice IDs matching the given filters (no pagination)."""
        q = select(Invoice.id).where(Invoice.audit_period_id == audit_period_id)
        if folder_status_id is not None:
            q = q.where(Invoice.folder_status_id == folder_status_id)
        if service_type_id is not None:
            q = q.where(Invoice.service_type_id == service_type_id)

        if admin_canonical is not None or admin_type is not None or contract_canonical is not None:
            q = q.join(Agreement, Invoice.agreement_id == Agreement.id)
            if admin_canonical is not None:
                q = q.join(Administrator, Agreement.administrator_id == Administrator.id).where(
                    Administrator.canonical_name == admin_canonical
                )
            if admin_type is not None:
                q = q.join(ContractType, Agreement.contract_type_id == ContractType.id).where(
                    ContractType.name == admin_type
                )
            if contract_canonical is not None:
                q = q.join(Contract, Agreement.contract_id == Contract.id).where(
                    Contract.canonical_name == contract_canonical
                )

        if search:
            terms = [t.strip() for t in search.split(";") if t.strip()]
            if terms:
                q = q.where(
                    or_(
                        *[
                            or_(
                                Invoice.invoice_number.ilike(f"%{t.upper()}%"),
                                Invoice.patient_name.ilike(f"%{t}%"),
                            )
                            for t in terms
                        ]
                    )
                )
        if has_finding_doc_type_id is not None:
            subq = select(MissingFile.id).where(
                MissingFile.invoice_id == Invoice.id,
                MissingFile.doc_type_id == has_finding_doc_type_id,
                MissingFile.resolved_at.is_(None),
            )
            q = q.where(exists(subq))
        result = await self.db.execute(q.order_by(Invoice.invoice_number))
        return list(result.scalars().all())

    async def get_stats(self, period_id: int) -> dict:
        """Return summary stats for a period: counts by status + total findings."""
        status_q = (
            select(FolderStatus.status, func.count(Invoice.id))
            .join(FolderStatus, Invoice.folder_status_id == FolderStatus.id)
            .where(Invoice.audit_period_id == period_id)
            .group_by(FolderStatus.status)
        )
        status_result = await self.db.execute(status_q)
        by_status = {row[0]: row[1] for row in status_result.all()}

        total_q = select(func.count(Invoice.id)).where(Invoice.audit_period_id == period_id)
        total_result = await self.db.execute(total_q)
        total = total_result.scalar_one()

        findings_q = (
            select(func.count(MissingFile.id))
            .join(Invoice, MissingFile.invoice_id == Invoice.id)
            .where(
                Invoice.audit_period_id == period_id,
                MissingFile.resolved_at.is_(None),
            )
        )
        findings_result = await self.db.execute(findings_q)
        total_findings = findings_result.scalar_one()

        return {"total": total, "by_status": by_status, "total_findings": total_findings}

    async def get_service_type_distribution(self, period_id: int) -> dict[str, int]:
        """Return count of invoices per service type code for a period."""
        q = (
            select(ServiceType.code, func.count(Invoice.id))
            .join(ServiceType, Invoice.service_type_id == ServiceType.id)
            .where(Invoice.audit_period_id == period_id)
            .group_by(ServiceType.code)
        )
        result = await self.db.execute(q)
        return {row[0]: row[1] for row in result.all()}

    async def get_all_for_export(self, period_id: int) -> list[Invoice]:
        """Return all invoices for a period with all relationships for Excel export."""
        q = (
            select(Invoice)
            .where(Invoice.audit_period_id == period_id)
            .order_by(Invoice.invoice_number)
            .options(
                selectinload(Invoice.folder_status),
                selectinload(Invoice.service_type),
                selectinload(Invoice.agreement).selectinload(Agreement.administrator),
                selectinload(Invoice.agreement).selectinload(Agreement.contract),
                selectinload(Invoice.agreement).selectinload(Agreement.contract_type),
                selectinload(Invoice.missing_files).selectinload(MissingFile.doc_type),
                selectinload(Invoice.period).selectinload(AuditPeriod.institution),
            )
        )
        result = await self.db.execute(q)
        return list(result.scalars().all())
