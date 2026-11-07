"""Tests for app/repositories/institution_repo.py — requires PostgreSQL."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.institution_repo import InstitutionRepo

pytestmark = pytest.mark.db


class TestInstitutionCRUD:
    async def test_create_and_get_by_name_roundtrip(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        created = await repo.create({"name": "ROUNDTRIP_HOSP", "display_name": "Roundtrip Hosp", "nit": "555555555"})
        fetched = await repo.get_by_name("ROUNDTRIP_HOSP")
        assert fetched is not None
        assert fetched.id == created.id

    async def test_delete_institution_returns_false_when_missing(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        assert await repo.delete_institution(999999) is False


class TestAdministratorCRUD:
    async def test_delete_administrator_returns_false_when_missing(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        assert await repo.delete_administrator(999999) is False

    async def test_create_then_delete_administrator(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        admin = await repo.create_administrator("TEST ADMIN", None)
        deleted = await repo.delete_administrator(admin.id)
        assert deleted is True
        assert await repo.delete_administrator(admin.id) is False  # already gone


class TestCopyMappings:
    async def _inst(self, repo: InstitutionRepo, name: str, nit: str):
        return await repo.create({"name": name, "display_name": name, "nit": nit})

    async def _first_ids(self, db: AsyncSession):
        from sqlalchemy import select

        from app.models.rules import DocType, ServiceType

        st = (await db.execute(select(ServiceType.id).limit(2))).scalars().all()
        dt = (await db.execute(select(DocType.id).limit(2))).scalars().all()
        return st, dt

    async def test_copies_services_and_documents_to_empty_target(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        src = await self._inst(repo, "COPY_SRC", "700000001")
        dst = await self._inst(repo, "COPY_DST", "700000002")
        st, dt = await self._first_ids(seeded)
        await repo.create_service(src.id, "CONSULTA X", st[0])
        await repo.create_service(src.id, "URGENCIAS X", None)
        await repo.upsert_service_type_document(src.id, st[0], dt[0])

        result = await repo.copy_mappings(src.id, dst.id)

        assert result == {"services": 2, "documents": 1}
        services = {s.raw_service: s.service_type_id for s in await repo.get_services(dst.id)}
        assert services == {"CONSULTA X": st[0], "URGENCIAS X": None}
        assert [(d.service_type_id, d.doc_type_id) for d in await repo.get_service_type_documents(dst.id)] == [
            (st[0], dt[0])
        ]

    async def test_replaces_existing_target_mappings_and_keeps_source(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        src = await self._inst(repo, "COPY_SRC2", "700000003")
        dst = await self._inst(repo, "COPY_DST2", "700000004")
        st, dt = await self._first_ids(seeded)
        await repo.create_service(src.id, "NUEVO", st[0])
        await repo.create_service(dst.id, "VIEJO", st[0])
        await repo.upsert_service_type_document(dst.id, st[0], dt[0])

        await repo.copy_mappings(src.id, dst.id)

        assert [s.raw_service for s in await repo.get_services(dst.id)] == ["NUEVO"]
        assert await repo.get_service_type_documents(dst.id) == []
        assert [s.raw_service for s in await repo.get_services(src.id)] == ["NUEVO"]

    async def test_empty_source_clears_target(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        src = await self._inst(repo, "COPY_SRC3", "700000005")
        dst = await self._inst(repo, "COPY_DST3", "700000006")
        st, _ = await self._first_ids(seeded)
        await repo.create_service(dst.id, "VIEJO", st[0])

        result = await repo.copy_mappings(src.id, dst.id)

        assert result == {"services": 0, "documents": 0}
        assert await repo.get_services(dst.id) == []


class TestConsolidateAgreements:
    async def test_no_duplicates_is_a_noop(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        result = await repo.consolidate_agreements()
        assert result == {"agreements_deleted": 0, "invoices_redirected": 0}
