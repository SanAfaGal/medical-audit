"""Tests for app/services/billing.py — pure pandas tests + ingest with mocked repos."""

from __future__ import annotations

import io
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from app.services.billing import _normalize, load_excel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_excel_bytes(**overrides) -> bytes:
    """Create a minimal valid SIHOS Excel as bytes."""
    row = {
        "FACTURA": "FV123456",
        "FECHA": "2024-01-15",
        "DOCUMENTO": "CC",
        "NUMERO": "12345678",
        "PACIENTE": "Juan Perez",
        "ADMINISTRADORA": "NUEVA EPS",
        "CONTRATO": "C001",
        "SERVICIO": "URGENCIAS",
        "OPERARIO": "Dr. Lopez",
        **overrides,
    }
    df = pd.DataFrame([row])
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# load_excel
# ---------------------------------------------------------------------------


class TestLoadExcel:
    def test_reads_all_expected_columns(self):
        data = load_excel(_make_excel_bytes())
        expected = {
            "FACTURA",
            "FECHA",
            "DOCUMENTO",
            "NUMERO",
            "PACIENTE",
            "ADMINISTRADORA",
            "CONTRATO",
            "SERVICIO",
            "OPERARIO",
        }
        assert set(data.columns) == expected

    def test_drops_extra_columns(self):
        # Add an extra column to the Excel
        row = {
            "FACTURA": "FV1",
            "FECHA": "2024-01-01",
            "DOCUMENTO": "CC",
            "NUMERO": "1",
            "PACIENTE": "P",
            "ADMINISTRADORA": "A",
            "CONTRATO": "C",
            "SERVICIO": "S",
            "OPERARIO": "O",
            "EXTRA_COLUMN": "should_be_dropped",
        }
        df = pd.DataFrame([row])
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        result = load_excel(buf.getvalue())
        assert "EXTRA_COLUMN" not in result.columns

    def test_missing_columns_are_absent_not_error(self):
        # Excel with only some columns
        df = pd.DataFrame([{"FACTURA": "FV1", "FECHA": "2024-01-01"}])
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        result = load_excel(buf.getvalue())
        assert "FACTURA" in result.columns
        assert "SERVICIO" not in result.columns  # not present → not in result


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_strips_and_uppercases_factura(self):
        df = pd.DataFrame(
            [
                {
                    "FACTURA": "  fv123  ",
                    "FECHA": "2024-01-15",
                    "ADMINISTRADORA": "EPS",
                }
            ]
        )
        result = _normalize(df)
        assert result["FACTURA"].iloc[0] == "FV123"

    def test_drops_rows_with_empty_factura(self):
        df = pd.DataFrame(
            [
                {"FACTURA": "FV1", "FECHA": "2024-01-15", "ADMINISTRADORA": "EPS"},
                {"FACTURA": "", "FECHA": "2024-01-15", "ADMINISTRADORA": "EPS"},
                {"FACTURA": None, "FECHA": "2024-01-15", "ADMINISTRADORA": "EPS"},
            ]
        )
        result = _normalize(df)
        assert len(result) == 1
        assert result["FACTURA"].iloc[0] == "FV1"

    def test_drops_rows_with_null_administradora(self):
        df = pd.DataFrame(
            [
                {"FACTURA": "FV1", "FECHA": "2024-01-15", "ADMINISTRADORA": "EPS"},
                {"FACTURA": "FV2", "FECHA": "2024-01-15", "ADMINISTRADORA": None},
            ]
        )
        result = _normalize(df)
        assert len(result) == 1

    def test_parses_fecha_to_date(self):
        df = pd.DataFrame(
            [
                {
                    "FACTURA": "FV1",
                    "FECHA": "2024-03-20",
                    "ADMINISTRADORA": "EPS",
                }
            ]
        )
        result = _normalize(df)
        assert result["FECHA"].iloc[0] == datetime.date(2024, 3, 20)

    def test_invalid_fecha_becomes_nat(self):
        df = pd.DataFrame(
            [
                {
                    "FACTURA": "FV1",
                    "FECHA": "not-a-date",
                    "ADMINISTRADORA": "EPS",
                }
            ]
        )
        result = _normalize(df)
        assert pd.isna(result["FECHA"].iloc[0])

    def test_returns_copy_does_not_mutate_input(self):
        df = pd.DataFrame(
            [
                {
                    "FACTURA": "  fv1  ",
                    "FECHA": "2024-01-01",
                    "ADMINISTRADORA": "EPS",
                }
            ]
        )
        original_factura = df["FACTURA"].iloc[0]
        _normalize(df)
        assert df["FACTURA"].iloc[0] == original_factura


# ---------------------------------------------------------------------------
# ingest — mocked repos
# ---------------------------------------------------------------------------


def _mock_repos(*, admins=None, contracts=None, services=None, agreements=None, service_types=None):
    """Build pre-configured (MockRulesRepo, MockInstRepo) for ingest tests."""
    admin_list = (
        admins
        if admins is not None
        else [
            SimpleNamespace(id=10, raw_name="NUEVA EPS", canonical_name="EPS NUEVA"),
        ]
    )
    contract_list = (
        contracts
        if contracts is not None
        else [
            SimpleNamespace(id=30, raw_name="C001", canonical_name="C001"),
        ]
    )
    service_list = (
        services
        if services is not None
        else [
            SimpleNamespace(id=20, raw_service="URGENCIAS", service_type_id=99),
        ]
    )
    agreement_list = (
        agreements
        if agreements is not None
        else [
            SimpleNamespace(
                id=5,
                contract_type_id=None,
                administrator=SimpleNamespace(raw_name="NUEVA EPS", canonical_name=None),
                contract=SimpleNamespace(raw_name="C001", canonical_name=None),
            ),
        ]
    )
    st_list = service_types if service_types is not None else [SimpleNamespace(id=99, priority=1)]

    MockRulesRepo = AsyncMock()
    MockRulesRepo.get_folder_status_by_status = AsyncMock(return_value=SimpleNamespace(id=2))
    MockRulesRepo.get_service_types = AsyncMock(return_value=st_list)

    MockInstRepo = AsyncMock()
    MockInstRepo.get_all_administrators = AsyncMock(return_value=admin_list)
    MockInstRepo.get_all_contracts = AsyncMock(return_value=contract_list)
    MockInstRepo.get_services = AsyncMock(return_value=service_list)
    MockInstRepo.get_agreements = AsyncMock(return_value=agreement_list)

    return MockRulesRepo, MockInstRepo


class TestIngest:
    """Tests for billing.ingest using mocked repos (current bulk-insert API)."""

    @pytest.fixture
    def mock_db(self):
        return AsyncMock()

    async def test_inserted_count_for_mapped_admin(self, mock_db):
        from app.services.billing import ingest

        mock_rules, mock_inst = _mock_repos()
        with (
            patch("app.services.billing.RulesRepo", return_value=mock_rules),
            patch("app.services.billing.InstitutionRepo", return_value=mock_inst),
        ):
            result = await ingest(_make_excel_bytes(), SimpleNamespace(id=1, name="H"), period_id=1, db=mock_db)

        assert result["inserted"] == 1
        assert result["skipped"] == 0

    async def test_unknown_admin_reported(self, mock_db):
        from app.services.billing import ingest

        mock_rules, mock_inst = _mock_repos(admins=[SimpleNamespace(id=10, raw_name="NUEVA EPS", canonical_name=None)])
        with (
            patch("app.services.billing.RulesRepo", return_value=mock_rules),
            patch("app.services.billing.InstitutionRepo", return_value=mock_inst),
        ):
            result = await ingest(_make_excel_bytes(), SimpleNamespace(id=1, name="H"), period_id=1, db=mock_db)

        assert "NUEVA EPS" in result["unknown_admins"]

    async def test_unknown_service_reported(self, mock_db):
        from app.services.billing import ingest

        mock_rules, mock_inst = _mock_repos(
            services=[SimpleNamespace(id=20, raw_service="URGENCIAS", service_type_id=None)]
        )
        with (
            patch("app.services.billing.RulesRepo", return_value=mock_rules),
            patch("app.services.billing.InstitutionRepo", return_value=mock_inst),
        ):
            result = await ingest(_make_excel_bytes(), SimpleNamespace(id=1, name="H"), period_id=1, db=mock_db)

        assert "URGENCIAS" in result["unknown_services"]

    async def test_raises_if_default_folder_status_missing(self, mock_db):
        from app.services.billing import ingest

        mock_rules = AsyncMock()
        mock_rules.get_folder_status_by_status = AsyncMock(return_value=None)
        with (
            patch("app.services.billing.RulesRepo", return_value=mock_rules),
            pytest.raises(RuntimeError, match="PRESENTE"),
        ):
            await ingest(_make_excel_bytes(), SimpleNamespace(id=1, name="H"), period_id=1, db=mock_db)
