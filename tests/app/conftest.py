"""Fixtures shared by the tests under tests/app that run pipeline stages over a real database."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.services.pipeline.context import build_context
from tests.app.repos.conftest import db_session, seeded, test_engine  # noqa: F401  (re-exported fixtures)


@pytest.fixture
async def stage_env(seeded, tmp_path):  # noqa: F811
    """Institution HOSP (invoice prefix HSL) with an empty period whose STAGE folder exists (the stage context)."""
    institution = Institution(name="HOSP", display_name="Hosp", nit="900123456", invoice_id_prefix="HSL")
    seeded.add(institution)
    await seeded.flush()
    period = AuditPeriod(
        institution_id=institution.id,
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2024, 1, 31),
        period_label="2024-01",
    )
    seeded.add(period)
    await seeded.flush()
    with patch("app.paths.audit_data_root", tmp_path):
        ctx = build_context(institution, period, seeded, {})
        ctx["stage_path"].mkdir(parents=True)
        yield ctx
