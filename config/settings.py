"""Application-wide settings.

All hospital-specific configuration (credentials, SIHOS params) is stored in the
SQLite database and accessed per-run via ``AuditRepository``.

Fixed paths (DB, backups, Drive credentials) live under ``~/.medical-audit/``.
The audit working directory (``audit_path``) is user-configurable and persisted in
``~/.medical-audit/config.json``.
"""

import contextlib
import json
import logging
import logging.handlers
from pathlib import Path

logger = logging.getLogger(__name__)

_APP_DIR: Path     = Path.home() / ".medical-audit"
_CONFIG_FILE: Path = _APP_DIR / "config.json"


def _load_audit_path() -> Path:
    try:
        return Path(json.loads(_CONFIG_FILE.read_text())["audit_path"])
    except (FileNotFoundError, KeyError, ValueError):
        return _APP_DIR / "audits"


def _load_filename_fixes() -> dict[str, str]:
    try:
        return json.loads(_CONFIG_FILE.read_text()).get("filename_fixes", {})
    except (FileNotFoundError, ValueError):
        return {}


class Settings:
    """Application-wide settings."""

    # --- Fixed system paths ---
    db_path:    Path = _APP_DIR / "audit.db"
    backup_dir: Path = _APP_DIR / "backups"
    logs_dir:   Path = _APP_DIR / "logs"

    # --- Configurable audit working directory ---
    audit_path: Path = _load_audit_path()

    # --- Global filename prefix corrections (shared across all hospitals) ---
    filename_fixes: dict[str, str] = _load_filename_fixes()

    # --- Logging ---
    log_level: str = "INFO"

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

    @classmethod
    def setup_file_logging(cls) -> None:
        """Attach a rotating file handler to the root logger.

        Safe to call on every Streamlit rerun — the handler is added only once
        per server process.  Writes to ``~/.medical-audit/logs/app.log`` with
        5 MB rotation and five backup files kept.
        """
        root = logging.getLogger()
        if any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
            return

        cls.logs_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            cls.logs_dir / "app.log",
            maxBytes=5 * 1024 * 1024,  # 5 MB per file
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(file_handler)
        root.setLevel(getattr(logging, cls.log_level.upper(), logging.INFO))
        root.info("=" * 70)
        root.info("Medical Audit — server process started")
        root.info("=" * 70)

    @classmethod
    def save_audit_path(cls, path: Path) -> None:
        """Persist *path* to config.json and update the class attribute.

        Args:
            path: New audit working directory.
        """
        _APP_DIR.mkdir(parents=True, exist_ok=True)
        data: dict = {}
        with contextlib.suppress(FileNotFoundError, ValueError):
            data = json.loads(_CONFIG_FILE.read_text())
        data["audit_path"] = str(path)
        _CONFIG_FILE.write_text(json.dumps(data, indent=2))
        cls.audit_path = path
        logger.info("audit_path updated to: %s", path)

    @classmethod
    def _save_filename_fixes(cls, fixes: dict[str, str]) -> None:
        """Persist *fixes* to config.json and update the class attribute."""
        _APP_DIR.mkdir(parents=True, exist_ok=True)
        data: dict = {}
        with contextlib.suppress(FileNotFoundError, ValueError):
            data = json.loads(_CONFIG_FILE.read_text())
        data["filename_fixes"] = fixes
        _CONFIG_FILE.write_text(json.dumps(data, indent=2))
        cls.filename_fixes = fixes

    @classmethod
    def upsert_filename_fix(cls, wrong: str, correct: str) -> None:
        """Add or replace a filename prefix correction and persist it.

        Args:
            wrong: The misnamed prefix (e.g. ``"OPD"``).
            correct: The correct replacement prefix (e.g. ``"OPF"``).
        """
        fixes = dict(cls.filename_fixes)
        fixes[wrong.strip().upper()] = correct.strip().upper()
        cls._save_filename_fixes(fixes)
        logger.info("filename_fix upserted: %s → %s", wrong, correct)

    @classmethod
    def delete_filename_fix(cls, wrong: str) -> None:
        """Remove a filename prefix correction and persist the change.

        Args:
            wrong: The misnamed prefix to remove.
        """
        fixes = dict(cls.filename_fixes)
        fixes.pop(wrong, None)
        cls._save_filename_fixes(fixes)
        logger.info("filename_fix deleted: %s", wrong)

    @staticmethod
    def drive_credentials_path(hospital_key: str) -> Path:
        """Return the path to the Drive service-account JSON for a hospital.

        The file is stored under ``~/.medical-audit/credentials/``.

        Args:
            hospital_key: Hospital identifier key (e.g. ``"SANTA_LUCIA"``).

        Returns:
            Path object (may not exist yet).
        """
        return _APP_DIR / "credentials" / (f"{hospital_key}.json")
