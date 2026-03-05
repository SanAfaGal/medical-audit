"""Application-wide settings loaded from environment variables.

Exposes a ``Settings`` class whose attributes are derived from ``.env`` and
the hospital configuration dictionaries in ``config/hospitals.py`` and
``config/mappings.py``.
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from config.mappings import ADMIN_CONTRACT_MAPS
from config.hospitals import HOSPITALS

load_dotenv()

logger = logging.getLogger(__name__)

_KEYS_DIR = Path("config/keys")


def _require_env(name: str) -> str:
    """Return the value of a required environment variable.

    Args:
        name: Name of the environment variable.

    Returns:
        The string value of the variable.

    Raises:
        EnvironmentError: If the variable is not set or empty.
    """
    value = os.getenv(name)
    if not value:
        raise EnvironmentError("Required environment variable not set: %s" % name)
    return value


def _load_sihos_credentials(hospital: str, keys_dir: Path) -> tuple[str, str]:
    """Load SIHOS user and password for the given hospital.

    Args:
        hospital: Hospital key matching a subdirectory in keys_dir.
        keys_dir: Root directory containing per-hospital credential folders.

    Returns:
        Tuple of (user, password).

    Raises:
        OSError: If the credentials file does not exist.
        KeyError: If required fields are absent from the credentials file.
    """
    creds_path = keys_dir / hospital / "sihos.json"
    if not creds_path.exists():
        raise OSError("SIHOS credentials file not found: %s" % creds_path)
    with creds_path.open() as fh:
        data = json.load(fh)
    return data["user"], data["password"]


class Settings:
    """Application-wide settings derived from environment variables and hospital config."""

    # --- Logging ---
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # --- Hospital identity ---
    active_hospital: str = _require_env("ACTIVE_HOSPITAL")
    hospital: dict = HOSPITALS.get(active_hospital, {})
    admin_contract_map: dict = ADMIN_CONTRACT_MAPS.get(active_hospital, {})

    # --- Credentials (derived from active_hospital) ---
    drive_credentials: Path = _KEYS_DIR / active_hospital / "drive.json"
    sihos_base_url: str = hospital["SIHOS_BASE_URL"]
    _sihos = _load_sihos_credentials(active_hospital, _KEYS_DIR)
    sihos_user: str = _sihos[0]
    sihos_password: str = _sihos[1]
    del _sihos

    # --- Paths ---
    audit_week: str = _require_env("AUDIT_WEEK")
    root_path: Path = Path(_require_env("ROOT_PATH"))
    base_path: Path = Path(_require_env("BASE_PATH")) / audit_week
    docs_dir: Path = base_path / "DOCS"
    base_dir: Path = base_path / "BASE"
    archive_dir: Path = base_path / "AUDIT"
    staging_dir: Path = base_path / "STAGE"

    # --- Audit database (accumulates across all audit weeks) ---
    db_path: Path = root_path / "audit.db"
    backup_dir: Path = root_path / "backups"

    # --- Report paths ---
    sihos_report_path: Path = base_path / ("%s_SIHOS.xlsx" % audit_week)
    audit_report_path: Path = base_path / ("%s_AUDITORIA.xlsx" % audit_week)

    # --- Data schema ---
    raw_schema_columns: list[str] = [
        "Doc",
        "No Doc",
        "Fecha",
        "Documento",
        "Numero",
        "Paciente",
        "Administradora",
        "Contrato",
        "Operario",
    ]

    export_schema_columns: list[str] = [
        "Fecha",
        "Documento",
        "Numero",
        "Paciente",
        "Administradora",
        "Contrato",
        "Operario",
    ]
