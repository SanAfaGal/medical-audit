"""Database fixtures for repository tests.

These tests require a live PostgreSQL instance.
Run them with: pytest -m db tests/app/repos/

Set the env var TEST_DATABASE_URL to override the default connection:
  export TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/dbname
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Import all models to register them with Base.metadata before create_all
import app.models.finding  # noqa: F401
import app.models.institution  # noqa: F401
import app.models.invoice  # noqa: F401
import app.models.period  # noqa: F401
import app.models.rules  # noqa: F401
from app.models.base import Base
from app.models.rules import DocType, FolderStatus, ServiceType

pytestmark = pytest.mark.db

_DEFAULT_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/medical_audit_test"
TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_DB_URL)


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(test_engine) -> AsyncSession:
    """Each test gets its own transaction, rolled back after the test."""
    async with test_engine.connect() as conn:
        trans = await conn.begin()
        factory = async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")
        async with factory() as session:
            yield session
        await trans.rollback()


@pytest_asyncio.fixture
async def seeded(db_session: AsyncSession) -> AsyncSession:
    """Seed minimum reference data needed by all repo tests."""
    for status in ["PRESENTE", "FALTANTE", "AUDITADA", "PENDIENTE", "ANULAR", "REVISAR"]:
        db_session.add(FolderStatus(status=status))
    db_session.add(ServiceType(code="GENERAL", display_name="General", priority=0))
    db_session.add(ServiceType(code="URGENCIAS", display_name="Urgencias", priority=10))
    db_session.add(DocType(code="FACTURA", description="Factura electrónica", prefix="FEV"))
    db_session.add(DocType(code="HISTORIA", description="Historia clínica", prefix="HCU"))
    db_session.add(DocType(code="SOPORTE", description="Soporte sin prefix", prefix=None))
    await db_session.flush()
    return db_session
