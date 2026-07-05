"""Async repository for missing files (audit findings)."""

from __future__ import annotations

import datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.finding import MissingFile
from app.models.invoice import Invoice
from app.models.rules import DocType


class MissingFileRepo:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def record_missing_file(self, invoice_id: int, doc_type_id: int, expected_path: str) -> MissingFile:
        """Insert missing file record; no-op if already exists (INSERT OR IGNORE)."""
        stmt = (
            pg_insert(MissingFile)
            .values(
                invoice_id=invoice_id,
                doc_type_id=doc_type_id,
                expected_path=expected_path,
            )
            .on_conflict_do_nothing(index_elements=["invoice_id", "doc_type_id"])
            .returning(MissingFile)
        )
        result = await self.db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            result2 = await self.db.execute(
                select(MissingFile).where(
                    MissingFile.invoice_id == invoice_id,
                    MissingFile.doc_type_id == doc_type_id,
                )
            )
            row = result2.scalar_one()
        return row

    async def resolve_missing_file(self, invoice_id: int, doc_type_id: int) -> None:
        """Set resolved_at = now() for the given invoice+doc_type pair."""
        await self.db.execute(
            update(MissingFile)
            .where(
                MissingFile.invoice_id == invoice_id,
                MissingFile.doc_type_id == doc_type_id,
            )
            .values(resolved_at=datetime.datetime.utcnow())
        )
        await self.db.flush()

    async def delete_missing_file(self, invoice_id: int, doc_type_id: int) -> None:
        await self.db.execute(
            delete(MissingFile).where(
                MissingFile.invoice_id == invoice_id,
                MissingFile.doc_type_id == doc_type_id,
            )
        )
        await self.db.flush()

    async def get_for_invoice(self, invoice_id: int) -> list[MissingFile]:
        result = await self.db.execute(
            select(MissingFile).where(MissingFile.invoice_id == invoice_id).order_by(MissingFile.doc_type_id)
        )
        return list(result.scalars().all())

    async def delete_all_for_invoice(self, invoice_id: int) -> None:
        await self.db.execute(delete(MissingFile).where(MissingFile.invoice_id == invoice_id))
        await self.db.flush()

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    async def upsert_finding(self, invoice_id: int, doc_type_id: int) -> None:
        """Insert a missing-file record; no-op if already exists."""
        await self.record_missing_file(invoice_id, doc_type_id, expected_path="")

    async def bulk_upsert_findings(self, findings: list[tuple[int, int]]) -> None:
        """Bulk insert (invoice_id, doc_type_id) pairs; no-op for already-existing records."""
        if not findings:
            return
        await self.db.execute(
            pg_insert(MissingFile)
            .values([{"invoice_id": inv_id, "doc_type_id": dt_id, "expected_path": ""} for inv_id, dt_id in findings])
            .on_conflict_do_nothing(index_elements=["invoice_id", "doc_type_id"])
        )
        await self.db.flush()

    async def get_findings_grouped_by_invoice(self, period_id: int) -> dict[str, list[str]]:
        """Return unresolved findings grouped by invoice number.

        Returns:
            ``{invoice_number: [doc_type_code, ...]}``
        """
        q = (
            select(Invoice.invoice_number, DocType.code)
            .join(MissingFile, MissingFile.invoice_id == Invoice.id)
            .join(DocType, DocType.id == MissingFile.doc_type_id)
            .where(
                Invoice.audit_period_id == period_id,
                MissingFile.resolved_at.is_(None),
            )
        )
        result = await self.db.execute(q)
        grouped: dict[str, list[str]] = {}
        for invoice_number, doc_code in result.all():
            grouped.setdefault(invoice_number, []).append(doc_code)
        return grouped

    async def delete_all_findings_for_invoice(self, invoice_id: int) -> None:
        """Alias for delete_all_for_invoice."""
        await self.delete_all_for_invoice(invoice_id)

    async def get_findings_summary(self, period_id: int) -> list[dict]:
        """Return unresolved finding counts per doc type for a period, sorted descending."""
        from app.models.rules import DocType

        q = (
            select(DocType.id, DocType.code, func.count(MissingFile.invoice_id.distinct()).label("cnt"))
            .join(MissingFile, MissingFile.doc_type_id == DocType.id)
            .join(Invoice, MissingFile.invoice_id == Invoice.id)
            .where(
                Invoice.audit_period_id == period_id,
                MissingFile.resolved_at.is_(None),
            )
            .group_by(DocType.id, DocType.code)
            .order_by(func.count(MissingFile.invoice_id.distinct()).desc())
        )
        result = await self.db.execute(q)
        return [{"doc_type_id": row[0], "code": row[1], "count": row[2]} for row in result.all()]

    async def delete_all_for_invoices(self, invoice_ids: list[int]) -> int:
        """Delete all findings for multiple invoices at once. Returns rows deleted."""
        if not invoice_ids:
            return 0
        result = await self.db.execute(delete(MissingFile).where(MissingFile.invoice_id.in_(invoice_ids)))
        await self.db.flush()
        return result.rowcount  # type: ignore[attr-defined, no-any-return]
