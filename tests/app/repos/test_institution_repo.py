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


class TestConsolidateAgreements:
    async def test_no_duplicates_is_a_noop(self, seeded: AsyncSession):
        repo = InstitutionRepo(seeded)
        result = await repo.consolidate_agreements()
        assert result == {"agreements_deleted": 0, "invoices_redirected": 0}
