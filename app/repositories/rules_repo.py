"""Async repository for business rules: service types, doc types, folder statuses."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import ServiceTypeDocument
from app.models.rules import DocType, FolderStatus, PrefixCorrection, ServiceType


class RulesRepo:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Service types
    # ------------------------------------------------------------------

    async def get_service_types(self) -> list[ServiceType]:
        """Return all service types ordered by priority descending."""
        result = await self.db.execute(select(ServiceType).order_by(ServiceType.priority.desc(), ServiceType.code))
        return list(result.scalars().all())

    async def get_service_type_by_code(self, code: str) -> ServiceType | None:
        result = await self.db.execute(select(ServiceType).where(ServiceType.code == code))
        return result.scalar_one_or_none()

    async def upsert_service_type(self, data: dict) -> ServiceType:
        stmt = (
            pg_insert(ServiceType)
            .values(**data)
            .on_conflict_do_update(index_elements=["code"], set_=data)
            .returning(ServiceType)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def update_service_type(self, service_type_id: int, data: dict) -> ServiceType | None:
        obj = await self.db.get(ServiceType, service_type_id)
        if not obj:
            return None
        for k, v in data.items():
            setattr(obj, k, v)
        await self.db.flush()
        return obj

    # ------------------------------------------------------------------
    # Doc types
    # ------------------------------------------------------------------

    async def get_doc_types(self) -> list[DocType]:
        result = await self.db.execute(select(DocType).order_by(DocType.code))
        return list(result.scalars().all())

    async def upsert_doc_type(self, data: dict) -> DocType:
        stmt = (
            pg_insert(DocType)
            .values(**data)
            .on_conflict_do_update(index_elements=["code"], set_=data)
            .returning(DocType)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def update_doc_type(self, doc_type_id: int, data: dict) -> DocType | None:
        obj = await self.db.get(DocType, doc_type_id)
        if not obj:
            return None
        for k, v in data.items():
            setattr(obj, k, v)
        await self.db.flush()
        return obj

    # ------------------------------------------------------------------
    # Folder statuses
    # ------------------------------------------------------------------

    async def get_folder_statuses(self) -> list[FolderStatus]:
        result = await self.db.execute(select(FolderStatus).order_by(FolderStatus.id))
        return list(result.scalars().all())

    async def get_folder_status_by_status(self, status: str) -> FolderStatus | None:
        result = await self.db.execute(select(FolderStatus).where(FolderStatus.status == status))
        return result.scalar_one_or_none()

    async def delete_service_type(self, service_type_id: int) -> bool:
        obj = await self.db.get(ServiceType, service_type_id)
        if not obj:
            return False
        await self.db.delete(obj)
        await self.db.flush()
        return True

    async def create_folder_status(self, status: str) -> FolderStatus:
        obj = FolderStatus(status=status)
        self.db.add(obj)
        await self.db.flush()
        await self.db.refresh(obj)
        return obj

    async def update_folder_status_obj(self, fs_id: int, status: str | None) -> FolderStatus | None:
        obj = await self.db.get(FolderStatus, fs_id)
        if not obj:
            return None
        if status is not None:
            obj.status = status
        await self.db.flush()
        return obj

    async def delete_folder_status(self, fs_id: int) -> bool:
        obj = await self.db.get(FolderStatus, fs_id)
        if not obj:
            return False
        await self.db.delete(obj)
        await self.db.flush()
        return True

    async def delete_doc_type(self, doc_type_id: int) -> bool:
        obj = await self.db.get(DocType, doc_type_id)
        if not obj:
            return False
        await self.db.delete(obj)
        await self.db.flush()
        return True

    async def get_doc_type_by_code(self, code: str) -> DocType | None:
        result = await self.db.execute(select(DocType).where(DocType.code == code))
        return result.scalar_one_or_none()

    async def get_doc_type_by_id(self, doc_type_id: int) -> DocType | None:
        result = await self.db.execute(select(DocType).where(DocType.id == doc_type_id))
        return result.scalar_one_or_none()

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    async def get_service_type_docs_map(self, institution_id: int) -> dict[int, list[int]]:
        """Return ``{service_type_id: [doc_type_id, ...]}`` for an institution."""
        result = await self.db.execute(
            select(ServiceTypeDocument.service_type_id, ServiceTypeDocument.doc_type_id).where(
                ServiceTypeDocument.institution_id == institution_id
            )
        )
        mapping: dict[int, list[int]] = {}
        for st_id, dt_id in result.all():
            mapping.setdefault(st_id, []).append(dt_id)
        return mapping

    async def get_active_doc_types_map(self) -> dict[int, list[str]]:
        """Return ``{doc_type_id: [prefix, ...]}`` for all doc types.

        Each DocType has a single optional prefix field.
        """
        result = await self.db.execute(select(DocType.id, DocType.prefix))
        return {dt_id: [prefix] if prefix else [] for dt_id, prefix in result.all()}

    async def get_all_active_doc_type_prefixes(self) -> list[str]:
        """Return a flat list of all non-null doc type prefixes."""
        result = await self.db.execute(select(DocType.prefix).where(DocType.prefix.isnot(None)))
        return [p for p in result.scalars().all() if p is not None]

    # ------------------------------------------------------------------
    # Prefix corrections
    # ------------------------------------------------------------------

    async def get_prefix_corrections(self) -> list[PrefixCorrection]:
        result = await self.db.execute(select(PrefixCorrection).order_by(PrefixCorrection.wrong_prefix))
        return list(result.scalars().all())

    async def get_prefix_corrections_map(self) -> dict[str, str]:
        """Return ``{wrong_prefix: correct_prefix}`` for all corrections."""
        result = await self.db.execute(select(PrefixCorrection.wrong_prefix, PrefixCorrection.correct_prefix))
        return {wrong.upper(): correct.upper() for wrong, correct in result.all()}

    async def create_prefix_correction(self, data: dict) -> PrefixCorrection:
        data = {**data, "wrong_prefix": data["wrong_prefix"].upper(), "correct_prefix": data["correct_prefix"].upper()}
        stmt = (
            pg_insert(PrefixCorrection)
            .values(**data)
            .on_conflict_do_update(index_elements=["wrong_prefix"], set_=data)
            .returning(PrefixCorrection)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def update_prefix_correction(self, correction_id: int, data: dict) -> PrefixCorrection | None:
        obj = await self.db.get(PrefixCorrection, correction_id)
        if not obj:
            return None
        if "correct_prefix" in data and data["correct_prefix"]:
            data["correct_prefix"] = data["correct_prefix"].upper()
        for k, v in data.items():
            if v is not None:
                setattr(obj, k, v)
        await self.db.flush()
        return obj

    async def delete_prefix_correction(self, correction_id: int) -> bool:
        obj = await self.db.get(PrefixCorrection, correction_id)
        if not obj:
            return False
        await self.db.delete(obj)
        await self.db.flush()
        return True

