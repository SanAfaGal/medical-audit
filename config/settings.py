"""Application-wide settings.

All hospital-specific configuration (credentials, SIHOS params) is stored in the
SQLite database and accessed per-run via ``AuditRepository``.

Fixed paths (DB, backups, Drive credentials) live under ``~/.medical-audit/``.
The audit working directory (``audit_path``) is user-configurable and persisted in
``~/.medical-audit/config.json``.
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_APP_DIR: Path     = Path.home() / ".medical-audit"
_CONFIG_FILE: Path = _APP_DIR / "config.json"


def _load_audit_path() -> Path:
    try:
        return Path(json.loads(_CONFIG_FILE.read_text())["audit_path"])
    except (FileNotFoundError, KeyError, ValueError):
        return _APP_DIR / "audits"


class Settings:
    """Application-wide settings."""

    # --- Fixed system paths ---
    db_path:    Path = _APP_DIR / "audit.db"
    backup_dir: Path = _APP_DIR / "backups"

    # --- Configurable audit working directory ---
    audit_path: Path = _load_audit_path()

    # --- Logging ---
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # --- Data schema (universal, not hospital-specific) ---
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

    @classmethod
    def save_audit_path(cls, path: Path) -> None:
        """Persist *path* to config.json and update the class attribute.

        Args:
            path: New audit working directory.
        """
        _APP_DIR.mkdir(parents=True, exist_ok=True)
        data: dict = {}
        try:
            data = json.loads(_CONFIG_FILE.read_text())
        except (FileNotFoundError, ValueError):
            pass
        data["audit_path"] = str(path)
        _CONFIG_FILE.write_text(json.dumps(data, indent=2))
        cls.audit_path = path
        logger.info("audit_path updated to: %s", path)

    @staticmethod
    def drive_credentials_path(hospital_key: str) -> Path:
        """Return the path to the Drive service-account JSON for a hospital.

        The file is stored under ``~/.medical-audit/.drive-credentials/``.

        Args:
            hospital_key: Hospital identifier key (e.g. ``"SANTA_LUCIA"``).

        Returns:
            Path object (may not exist yet).
        """
        return _APP_DIR / ".drive-credentials" / ("%s.json" % hospital_key)
