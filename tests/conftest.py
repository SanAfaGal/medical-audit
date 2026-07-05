"""Shared fixtures used across all test tiers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import datetime

import pytest


# ---------------------------------------------------------------------------
# Filesystem fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def nit() -> str:
    return "900123456"


@pytest.fixture
def id_prefix() -> str:
    return "HSL"


@pytest.fixture
def tmp_stage(tmp_path: Path) -> Path:
    """
    Creates a minimal STAGE-like tree:

      stage/
        HSL123/
          FEV_900123456_HSL123.pdf
          CRC_900123456_HSL123.pdf
        HSL456/
          FEV_900123456_HSL456.pdf
        HSL789 EXTRA/
          FEV_900123456_HSL789.pdf
        HSL000 CUFE/
          FEV_900123456_HSL000.pdf
    """
    stage = tmp_path / "stage"
    _make_folder(stage / "HSL123", ["FEV_900123456_HSL123.pdf", "CRC_900123456_HSL123.pdf"])
    _make_folder(stage / "HSL456", ["FEV_900123456_HSL456.pdf"])
    _make_folder(stage / "HSL789 EXTRA", ["FEV_900123456_HSL789.pdf"])
    _make_folder(stage / "HSL000 CUFE", ["FEV_900123456_HSL000.pdf"])
    return stage


@pytest.fixture
def tmp_drive(tmp_path: Path) -> Path:
    """
    A DRIVE tree with nested folders (leaf folders contain files):

      drive/
        batch1/
          HSL001/
            FEV_900123456_HSL001.pdf
        batch2/
          HSL002/
            FEV_900123456_HSL002.pdf
    """
    drive = tmp_path / "drive"
    _make_folder(drive / "batch1" / "HSL001", ["FEV_900123456_HSL001.pdf"])
    _make_folder(drive / "batch2" / "HSL002", ["FEV_900123456_HSL002.pdf"])
    return drive


def _make_folder(path: Path, filenames: list[str]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        (path / name).write_bytes(b"")


# ---------------------------------------------------------------------------
# Domain object fixtures (no DB needed)
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_institution(nit: str, id_prefix: str):
    return SimpleNamespace(
        id=1,
        name="Hospital Test",
        nit=nit,
        invoice_id_prefix=id_prefix,
        base_path="/data",
        sihos_user=None,
        sihos_password=None,
        sihos_base_url=None,
        sihos_doc_code=None,
        drive_credentials_enc=None,
    )


@pytest.fixture
def minimal_period():
    return SimpleNamespace(
        id=1,
        period_label="2024-01",
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2024, 1, 31),
        institution_id=1,
    )
