"""Application-wide settings.

All hospital-specific configuration (credentials, paths, SIHOS params) is
stored in the SQLite database and accessed per-run via ``AuditRepository``.
The only fixed value here is the database location.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class Settings:
    """Application-wide settings."""

    # --- Database (fixed location, no .env required) ---
    db_path: Path    = Path.home() / ".medical-audit" / "audit.db"
    backup_dir: Path = db_path.parent / "backups"

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
