"""HTTP-level fixtures: the real FastAPI app served over a rolled-back test transaction.

Requires PostgreSQL (see ``tests/app/repos/conftest.py`` for ``TEST_DATABASE_URL``).
"""

from __future__ import annotations

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Reuse the DB fixtures: session engine, per-test rolled-back session and reference data.
from tests.app.repos.conftest import db_session, seeded, test_engine  # noqa: F401


@pytest_asyncio.fixture
async def client(seeded):  # noqa: F811
    from app.database import get_db
    from app.main import app

    async def _override_get_db():
        yield seeded

    app.dependency_overrides[get_db] = _override_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_db, None)
