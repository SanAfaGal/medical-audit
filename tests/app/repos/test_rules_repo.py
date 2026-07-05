"""Tests for app/repositories/rules_repo.py — requires PostgreSQL."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import Institution, ServiceTypeDocument
from app.repositories.rules_repo import RulesRepo

pytestmark = pytest.mark.db


class TestGetDocTypeByCode:
    async def test_finds_existing(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        dt = await repo.get_doc_type_by_code("FACTURA")
        assert dt is not None
        assert dt.prefix == "FEV"

    async def test_returns_none_for_unknown(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        assert await repo.get_doc_type_by_code("NONEXISTENT") is None


class TestGetAllActiveDocTypePrefixes:
    async def test_returns_non_null_prefixes(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        prefixes = await repo.get_all_active_doc_type_prefixes()
        assert "FEV" in prefixes
        assert "HCU" in prefixes

    async def test_excludes_null_prefix(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        prefixes = await repo.get_all_active_doc_type_prefixes()
        # SOPORTE has prefix=None — should not appear
        assert None not in prefixes


class TestGetActiveDocTypesMap:
    async def test_returns_dict_with_prefix_list(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        mapping = await repo.get_active_doc_types_map()
        # All doc type IDs should be present
        assert len(mapping) == 3
        # Check that prefix is wrapped in list
        for dt_id, prefixes in mapping.items():
            assert isinstance(prefixes, list)

    async def test_null_prefix_becomes_empty_list(self, seeded: AsyncSession):
        from app.repositories.rules_repo import RulesRepo
        from app.models.rules import DocType

        repo = RulesRepo(seeded)
        mapping = await repo.get_active_doc_types_map()
        # Find the SOPORTE entry (prefix=None)
        from sqlalchemy import select

        result = await seeded.execute(select(DocType).where(DocType.code == "SOPORTE"))
        soporte = result.scalar_one()
        assert mapping[soporte.id] == []


class TestGetServiceTypeDocsMap:
    async def _create_institution(self, db: AsyncSession) -> int:
        inst = Institution(
            name="TestHosp",
            display_name="Test",
            nit="111111111",
            invoice_id_prefix="TH",
        )
        db.add(inst)
        await db.flush()
        return inst.id

    async def test_returns_grouped_map(self, seeded: AsyncSession):
        from sqlalchemy import select
        from app.models.rules import ServiceType, DocType

        repo = RulesRepo(seeded)
        inst_id = await self._create_institution(seeded)

        st_result = await seeded.execute(select(ServiceType).where(ServiceType.code == "URGENCIAS"))
        st = st_result.scalar_one()
        dt_result = await seeded.execute(select(DocType).where(DocType.code == "FACTURA"))
        dt = dt_result.scalar_one()

        seeded.add(ServiceTypeDocument(institution_id=inst_id, service_type_id=st.id, doc_type_id=dt.id))
        await seeded.flush()

        mapping = await repo.get_service_type_docs_map(inst_id)
        assert st.id in mapping
        assert dt.id in mapping[st.id]

    async def test_empty_for_institution_with_no_mappings(self, seeded: AsyncSession):
        repo = RulesRepo(seeded)
        inst_id = await self._create_institution(seeded)
        mapping = await repo.get_service_type_docs_map(inst_id)
        assert mapping == {}
