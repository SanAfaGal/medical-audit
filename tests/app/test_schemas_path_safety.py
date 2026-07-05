"""institution.name and period_label are used as literal filesystem path components —
they must be restricted to a safe character set."""

from __future__ import annotations

import datetime

import pytest
from pydantic import ValidationError

from app.schemas.institution import InstitutionCreate
from app.schemas.invoice import PeriodCreate


class TestInstitutionNameValidation:
    def test_accepts_safe_name(self):
        inst = InstitutionCreate(name="SANTA_LUCIA-2", display_name="Hospital Santa Lucia", nit="900123456")
        assert inst.name == "SANTA_LUCIA-2"

    def test_rejects_path_traversal_name(self):
        with pytest.raises(ValidationError):
            InstitutionCreate(name="../../etc", display_name="x", nit="900123456")

    def test_rejects_name_with_path_separator(self):
        with pytest.raises(ValidationError):
            InstitutionCreate(name="foo/bar", display_name="x", nit="900123456")


class TestPeriodLabelValidation:
    def test_accepts_safe_label(self):
        period = PeriodCreate(
            institution_id=1,
            date_from=datetime.date(2024, 1, 1),
            date_to=datetime.date(2024, 1, 31),
            period_label="2024-01",
        )
        assert period.period_label == "2024-01"

    def test_rejects_path_traversal_label(self):
        with pytest.raises(ValidationError):
            PeriodCreate(
                institution_id=1,
                date_from=datetime.date(2024, 1, 1),
                date_to=datetime.date(2024, 1, 31),
                period_label="../../etc",
            )
