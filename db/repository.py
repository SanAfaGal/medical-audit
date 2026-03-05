"""SQLite-backed repository for audit findings per invoice."""

import json
import logging
import shutil
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from db.schema import FINDING_LABELS, SCHEMA_DDL, FindingCode, FolderStatus, InvoiceType

logger = logging.getLogger(__name__)

_COMMENT_SEPARATOR = "; "


class AuditRepository:
    """SQLite-backed store for audit findings per invoice folder.

    One row in ``audit_findings`` represents one missing document type for a
    folder.  Deleting the row means the document has been supplied.

    Args:
        db_path: Path to the SQLite database file. Created if it does not exist.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def backup(self, backup_dir: Path, keep: int = 7) -> Path | None:
        """Copy the database to *backup_dir* and prune old backups.

        The backup file is named ``audit_{YYYY-MM-DD}.db``.  If a file with
        today's date already exists it is overwritten.  After copying, the
        oldest files beyond *keep* are deleted.

        Args:
            backup_dir: Directory where backup files are stored.
            keep: Maximum number of backup files to retain.

        Returns:
            Path of the newly created backup, or ``None`` if the database
            file does not yet exist.
        """
        if not self.db_path.exists():
            logger.warning("Cannot backup: database file not found at %s", self.db_path)
            return None
        backup_dir.mkdir(parents=True, exist_ok=True)
        dest = backup_dir / ("audit_%s.db" % date.today().isoformat())
        shutil.copy2(self.db_path, dest)
        logger.info("Database backed up to %s", dest)
        backups = sorted(backup_dir.glob("audit_*.db"))
        for old in backups[:-keep]:
            old.unlink()
            logger.info("Removed old backup: %s", old)
        return dest

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """Open a new SQLite connection with foreign-key enforcement enabled."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        """Create tables and indexes if they do not already exist, and run migrations."""
        with self._connect() as conn:
            conn.executescript(SCHEMA_DDL)
            # Idempotent column migrations for existing databases
            for stmt in (
                "ALTER TABLE invoices ADD COLUMN tipo TEXT NOT NULL DEFAULT 'GENERAL'",
                "ALTER TABLE invoices ADD COLUMN folder_status TEXT NOT NULL DEFAULT 'PRESENTE'",
                "ALTER TABLE invoices ADD COLUMN nota TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE hospitals ADD COLUMN sihos_user TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE hospitals ADD COLUMN sihos_password TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE hospitals ADD COLUMN drive_credentials_path TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE hospitals ADD COLUMN base_path TEXT NOT NULL DEFAULT ''",
            ):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # Column already exists
        # Seed hospital config from hardcoded dicts if tables are empty
        self._seed_hospitals_if_empty()
        self._seed_filename_fixes_if_empty()

    # ------------------------------------------------------------------
    # Invoice loading
    # ------------------------------------------------------------------

    def upsert_invoices(
        self, df: pd.DataFrame, hospital: str, period: str
    ) -> int:
        """Insert invoices from a processed DataFrame, ignoring existing rows.

        Uses ``INSERT OR IGNORE`` so that previously recorded findings are not
        lost when the same invoice appears in a subsequent load.

        Args:
            df: DataFrame indexed by ``Factura`` with invoice metadata columns.
            hospital: Active hospital key (e.g. ``"RAMON_MARIA_ARANA"``).
            period: Audit period string (e.g. ``"22-28"``).

        Returns:
            Number of rows newly inserted.
        """
        rows = [
            (
                hospital,
                period,
                factura,
                row.get("Fecha"),
                row.get("Documento"),
                row.get("Numero"),
                row.get("Paciente"),
                row.get("Administradora"),
                row.get("Contrato"),
                row.get("Operario"),
                row.get("Ruta"),
            )
            for factura, row in df.iterrows()
        ]

        with self._connect() as conn:
            cursor = conn.executemany(
                """
                INSERT OR IGNORE INTO invoices
                    (hospital, period, factura, fecha, documento, numero,
                     paciente, administradora, contrato, operario, ruta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            inserted = cursor.rowcount

        logger.info(
            "upsert_invoices: hospital=%s period=%s — %d submitted, %d inserted",
            hospital,
            period,
            len(rows),
            inserted,
        )
        return inserted

    # ------------------------------------------------------------------
    # Findings CRUD
    # ------------------------------------------------------------------

    def record_finding(
        self,
        hospital: str,
        period: str,
        factura: str,
        finding_type: str,
    ) -> None:
        """Record a missing document finding for an invoice folder.

        Idempotent — no-op if the same finding type is already recorded.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier (e.g. ``"FE12345"``).
            finding_type: One of the ``FindingCode`` constants.

        Raises:
            ValueError: If ``finding_type`` is not recognised.
        """
        if finding_type not in FindingCode._ALL:
            raise ValueError("Unknown finding_type: %s" % finding_type)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO audit_findings (invoice_id, finding_type)
                SELECT id, ?
                FROM invoices
                WHERE hospital = ? AND period = ? AND factura = ?
                """,
                (finding_type, hospital, period, factura),
            )

    def delete_finding(
        self,
        hospital: str,
        period: str,
        factura: str,
        finding_type: str,
    ) -> None:
        """Remove a missing-document finding (document has been supplied).

        No-op if the finding is not present.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier.
            finding_type: One of the ``FindingCode`` constants.
        """
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM audit_findings
                WHERE invoice_id = (
                    SELECT id FROM invoices
                    WHERE hospital = ? AND period = ? AND factura = ?
                )
                AND finding_type = ?
                """,
                (hospital, period, factura, finding_type),
            )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def fetch_findings(self, hospital: str, period: str, factura: str) -> list[str]:
        """Return all recorded finding types for a single invoice folder.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier.

        Returns:
            List of ``FindingCode`` strings, ordered by insertion time.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT f.finding_type
                FROM audit_findings f
                JOIN invoices i ON i.id = f.invoice_id
                WHERE i.hospital = ? AND i.period = ? AND i.factura = ?
                ORDER BY f.id
                """,
                (hospital, period, factura),
            ).fetchall()
        return [r["finding_type"] for r in rows]

    def fetch_invoices_with_findings(
        self, hospital: str, period: str
    ) -> list[str]:
        """Return invoice IDs that have at least one active finding.

        Args:
            hospital: Hospital key.
            period: Audit period string.

        Returns:
            Sorted list of factura identifiers.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT i.factura
                FROM audit_findings f
                JOIN invoices i ON i.id = f.invoice_id
                WHERE i.hospital = ? AND i.period = ?
                ORDER BY i.factura
                """,
                (hospital, period),
            ).fetchall()
        return [r["factura"] for r in rows]

    def update_tipo(
        self, hospital: str, period: str, factura: str, tipo: str
    ) -> None:
        """Set the invoice type for a single invoice.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier.
            tipo: One of the ``InvoiceType`` constants.

        Raises:
            ValueError: If ``tipo`` is not a recognised constant.
        """
        if tipo not in InvoiceType._ALL:
            raise ValueError("Unknown tipo: %s" % tipo)
        with self._connect() as conn:
            conn.execute(
                "UPDATE invoices SET tipo = ? WHERE hospital = ? AND period = ? AND factura = ?",
                (tipo, hospital, period, factura),
            )

    def fetch_by_tipo(
        self, hospital: str, period: str, tipos: str | list[str]
    ) -> list[str]:
        """Return invoice IDs whose tipo matches the given value(s).

        Args:
            hospital: Hospital key.
            period: Audit period string.
            tipos: A single ``InvoiceType`` constant or a list of them.

        Returns:
            Sorted list of factura identifiers.
        """
        if isinstance(tipos, str):
            tipos = [tipos]
        placeholders = ",".join("?" * len(tipos))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT factura FROM invoices
                WHERE hospital = ? AND period = ? AND tipo IN (%s)
                ORDER BY factura
                """ % placeholders,
                [hospital, period, *tipos],
            ).fetchall()
        return [r["factura"] for r in rows]

    def update_folder_status(
        self, hospital: str, period: str, factura: str, status: str
    ) -> None:
        """Set the folder presence status for a single invoice.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier.
            status: One of the ``FolderStatus`` constants.

        Raises:
            ValueError: If ``status`` is not a recognised constant.
        """
        if status not in FolderStatus._ALL:
            raise ValueError("Unknown folder_status: %s" % status)
        with self._connect() as conn:
            conn.execute(
                "UPDATE invoices SET folder_status = ? WHERE hospital = ? AND period = ? AND factura = ?",
                (status, hospital, period, factura),
            )

    def fetch_by_folder_status(
        self, hospital: str, period: str, statuses: str | list[str]
    ) -> list[str]:
        """Return invoice identifiers whose folder_status matches the given value(s).

        Args:
            hospital: Hospital key.
            period: Audit period string.
            statuses: A single ``FolderStatus`` constant or a list of them.

        Returns:
            Sorted list of factura identifiers.
        """
        if isinstance(statuses, str):
            statuses = [statuses]
        placeholders = ",".join("?" * len(statuses))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT factura FROM invoices
                WHERE hospital = ? AND period = ? AND folder_status IN (%s)
                ORDER BY factura
                """ % placeholders,
                [hospital, period, *statuses],
            ).fetchall()
        return [r["factura"] for r in rows]

    def update_nota(self, hospital: str, period: str, factura: str, nota: str) -> None:
        """Set the folder-level note for a single invoice.

        Args:
            hospital: Hospital key.
            period: Audit period string.
            factura: Invoice identifier.
            nota: Free-text note for the folder.
        """
        with self._connect() as conn:
            conn.execute(
                "UPDATE invoices SET nota = ? WHERE hospital = ? AND period = ? AND factura = ?",
                (nota, hospital, period, factura),
            )

    # ------------------------------------------------------------------
    # Hospital configuration
    # ------------------------------------------------------------------

    def _seed_hospitals_if_empty(self) -> None:
        """Seed hospital and mapping tables from hardcoded config if they are empty."""
        with self._connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
        if count > 0:
            return
        try:
            from config.hospitals import HOSPITALS
            from config.mappings import ADMIN_CONTRACT_MAPS
        except ImportError:
            return
        self.seed_hospitals_from_config(HOSPITALS, ADMIN_CONTRACT_MAPS)

    def _seed_filename_fixes_if_empty(self) -> None:
        """Seed filename_fixes table from all hospitals' MISNAMED_FIXER_MAP if empty."""
        with self._connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM filename_fixes").fetchone()[0]
        if count > 0:
            return
        try:
            from config.hospitals import HOSPITALS
        except ImportError:
            return
        fixes: dict[str, str] = {}
        for cfg in HOSPITALS.values():
            fixes.update(cfg.get("MISNAMED_FIXER_MAP", {}))
        with self._connect() as conn:
            for wrong, correct in fixes.items():
                conn.execute(
                    "INSERT OR IGNORE INTO filename_fixes (wrong_prefix, correct_prefix) VALUES (?, ?)",
                    (wrong, correct),
                )

    def fetch_invoice_ids(self, hospital: str, period: str) -> list[str]:
        """Return all factura IDs for a given hospital and period.

        Args:
            hospital: Hospital key.
            period: Audit period string.

        Returns:
            Sorted list of factura identifier strings.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT factura FROM invoices WHERE hospital = ? AND period = ? ORDER BY factura",
                (hospital, period),
            ).fetchall()
        return [r["factura"] for r in rows]

    def fetch_filename_fixes(self) -> dict[str, str]:
        """Return all filename prefix fixes as a dict.

        Returns:
            ``{wrong_prefix: correct_prefix}`` mapping.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT wrong_prefix, correct_prefix FROM filename_fixes ORDER BY wrong_prefix"
            ).fetchall()
        return {r["wrong_prefix"]: r["correct_prefix"] for r in rows}

    def upsert_filename_fix(self, wrong: str, correct: str) -> None:
        """Insert or replace a filename prefix fix.

        Args:
            wrong: The misnamed prefix (e.g. ``"OPD"``).
            correct: The correct replacement prefix (e.g. ``"OPF"``).
        """
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO filename_fixes (wrong_prefix, correct_prefix) VALUES (?, ?)",
                (wrong.strip().upper(), correct.strip().upper()),
            )

    def delete_filename_fix(self, wrong: str) -> None:
        """Delete a filename prefix fix by its wrong_prefix key.

        Args:
            wrong: The misnamed prefix to remove.
        """
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM filename_fixes WHERE wrong_prefix = ?", (wrong,)
            )

    def seed_hospitals_from_config(
        self, hospitals_dict: dict, mappings_dict: dict
    ) -> None:
        """Insert hospital config and admin/contract mappings from dicts.

        Uses ``INSERT OR IGNORE`` so existing rows are not overwritten.

        Args:
            hospitals_dict: Dict matching the structure of ``config.hospitals.HOSPITALS``.
            mappings_dict: Dict matching the structure of ``config.mappings.ADMIN_CONTRACT_MAPS``.
        """
        with self._connect() as conn:
            for key, cfg in hospitals_dict.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO hospitals
                        (key, display_name, nit, invoice_identifier_prefix,
                         sihos_base_url, sihos_invoice_doc_code, document_standards,
                         sihos_user, sihos_password, drive_credentials_path, base_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        key.replace("_", " ").title(),
                        cfg.get("NIT", ""),
                        cfg.get("INVOICE_IDENTIFIER_PREFIX", ""),
                        cfg.get("SIHOS_BASE_URL", ""),
                        cfg.get("SIHOS_INVOICE_DOC_CODE", ""),
                        json.dumps(cfg.get("DOCUMENT_STANDARDS", {})),
                        "",
                        "",
                        "",
                        "",
                    ),
                )
            for hospital_key, mapping in mappings_dict.items():
                for (raw_admin, raw_contract), (canonical_admin, canonical_contract) in mapping.items():
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO admin_contract_mappings
                            (hospital_key, raw_admin, raw_contract, canonical_admin, canonical_contract)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (hospital_key, raw_admin, raw_contract, canonical_admin, canonical_contract),
                    )

    def fetch_all_hospitals(self) -> list[dict]:
        """Return all hospitals as a list of dicts.

        Returns:
            List of dicts with keys matching the ``hospitals`` table columns.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, display_name, nit, invoice_identifier_prefix, "
                "sihos_base_url, sihos_invoice_doc_code, document_standards, "
                "sihos_user, sihos_password, drive_credentials_path, base_path "
                "FROM hospitals ORDER BY key"
            ).fetchall()
        return [dict(r) for r in rows]

    def fetch_hospital_config(self, key: str) -> dict:
        """Return hospital config dict compatible with ``config.hospitals.HOSPITALS[key]``.

        Args:
            key: Hospital key (e.g. ``"RAMON_MARIA_ARANA"``).

        Returns:
            Dict with NIT, INVOICE_IDENTIFIER_PREFIX, SIHOS_BASE_URL, etc.
            Returns an empty dict if the key is not found.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM hospitals WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return {}
        return {
            "NIT": row["nit"],
            "INVOICE_IDENTIFIER_PREFIX": row["invoice_identifier_prefix"],
            "SIHOS_BASE_URL": row["sihos_base_url"],
            "SIHOS_INVOICE_DOC_CODE": row["sihos_invoice_doc_code"],
            "DOCUMENT_STANDARDS": json.loads(row["document_standards"]),
            "sihos_user": row["sihos_user"],
            "sihos_password": row["sihos_password"],
            "drive_credentials_path": row["drive_credentials_path"],
            "base_path": row["base_path"],
        }

    def fetch_admin_contract_map(self, hospital_key: str) -> dict:
        """Return admin/contract mapping dict for a hospital.

        Returns a dict in the format used by ``BillingIngester``:
        ``{(raw_admin, raw_contract): (canonical_admin, canonical_contract)}``.

        Args:
            hospital_key: Hospital key.

        Returns:
            Mapping dict. Returns an empty dict if no mappings are found.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT raw_admin, raw_contract, canonical_admin, canonical_contract "
                "FROM admin_contract_mappings WHERE hospital_key = ?",
                (hospital_key,),
            ).fetchall()
        return {
            (r["raw_admin"], r["raw_contract"]): (r["canonical_admin"], r["canonical_contract"])
            for r in rows
        }

    def upsert_hospital(self, key: str, cfg: dict) -> None:
        """Insert or replace a hospital record.

        Args:
            key: Hospital key.
            cfg: Dict with keys NIT, INVOICE_IDENTIFIER_PREFIX, SIHOS_BASE_URL,
                SIHOS_INVOICE_DOC_CODE, DOCUMENT_STANDARDS, and optionally display_name.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO hospitals
                    (key, display_name, nit, invoice_identifier_prefix,
                     sihos_base_url, sihos_invoice_doc_code, document_standards,
                     sihos_user, sihos_password, drive_credentials_path, base_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    display_name              = excluded.display_name,
                    nit                       = excluded.nit,
                    invoice_identifier_prefix = excluded.invoice_identifier_prefix,
                    sihos_base_url            = excluded.sihos_base_url,
                    sihos_invoice_doc_code    = excluded.sihos_invoice_doc_code,
                    document_standards        = excluded.document_standards,
                    sihos_user                = excluded.sihos_user,
                    sihos_password            = excluded.sihos_password,
                    drive_credentials_path    = excluded.drive_credentials_path,
                    base_path                 = excluded.base_path
                """,
                (
                    key,
                    cfg.get("display_name", key.replace("_", " ").title()),
                    cfg.get("NIT", ""),
                    cfg.get("INVOICE_IDENTIFIER_PREFIX", ""),
                    cfg.get("SIHOS_BASE_URL", ""),
                    cfg.get("SIHOS_INVOICE_DOC_CODE", ""),
                    json.dumps(cfg.get("DOCUMENT_STANDARDS", {})),
                    cfg.get("sihos_user", ""),
                    cfg.get("sihos_password", ""),
                    cfg.get("drive_credentials_path", ""),
                    cfg.get("base_path", ""),
                ),
            )

    def fetch_admin_contract_mappings(self, hospital_key: str) -> list[dict]:
        """Return all admin/contract mapping rows for a hospital.

        Args:
            hospital_key: Hospital key.

        Returns:
            List of dicts with keys id, raw_admin, raw_contract,
            canonical_admin, canonical_contract.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, raw_admin, raw_contract, canonical_admin, canonical_contract "
                "FROM admin_contract_mappings WHERE hospital_key = ? "
                "ORDER BY raw_admin, raw_contract",
                (hospital_key,),
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_admin_contract_mapping(
        self,
        hospital_key: str,
        raw_admin: str,
        raw_contract: str | None,
        canonical_admin: str | None,
        canonical_contract: str | None,
    ) -> None:
        """Insert or update a single admin/contract mapping row.

        Args:
            hospital_key: Hospital key.
            raw_admin: Raw administrator name from SIHOS.
            raw_contract: Raw contract name from SIHOS (may be None).
            canonical_admin: Normalised administrator name (may be None).
            canonical_contract: Normalised contract name (may be None).
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admin_contract_mappings
                    (hospital_key, raw_admin, raw_contract, canonical_admin, canonical_contract)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hospital_key, raw_admin, raw_contract) DO UPDATE SET
                    canonical_admin    = excluded.canonical_admin,
                    canonical_contract = excluded.canonical_contract
                """,
                (hospital_key, raw_admin, raw_contract, canonical_admin, canonical_contract),
            )

    def delete_admin_contract_mapping(self, mapping_id: int) -> None:
        """Delete a single admin/contract mapping row by its primary key.

        Args:
            mapping_id: Row id from ``admin_contract_mappings``.
        """
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM admin_contract_mappings WHERE id = ?", (mapping_id,)
            )

    def fetch_hospitals_and_periods(self) -> dict[str, list[str]]:
        """Return a mapping of hospital name to list of audit periods.

        Returns:
            Dict keyed by hospital with sorted period lists as values.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT hospital, period FROM invoices ORDER BY hospital, period"
            ).fetchall()
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(row["hospital"], []).append(row["period"])
        return result

    # ------------------------------------------------------------------
    # Report builder
    # ------------------------------------------------------------------

    def to_dataframe(self, hospital: str, period: str) -> pd.DataFrame:
        """Return all invoices for a period with audit columns appended.

        Each row includes the invoice metadata plus:

        - ``Comentario`` — semicolon-joined human-readable labels of missing
          document types (e.g. ``"Firma faltante; CUFE faltante"``).
        - ``Nota`` — free-text note stored at the folder (invoice) level.

        Invoices with no findings have an empty string in ``Comentario``.

        Args:
            hospital: Hospital key.
            period: Audit period string.

        Returns:
            DataFrame indexed by ``Factura``.
        """
        with self._connect() as conn:
            invoice_rows = conn.execute(
                """
                SELECT factura, fecha, documento, numero, paciente,
                       administradora, contrato, operario, tipo, folder_status, nota
                FROM invoices
                WHERE hospital = ? AND period = ?
                ORDER BY factura
                """,
                (hospital, period),
            ).fetchall()

            finding_rows = conn.execute(
                """
                SELECT i.factura, f.finding_type
                FROM audit_findings f
                JOIN invoices i ON i.id = f.invoice_id
                WHERE i.hospital = ? AND i.period = ?
                ORDER BY f.id
                """,
                (hospital, period),
            ).fetchall()

        findings_index: dict[str, list[str]] = {}
        for row in finding_rows:
            findings_index.setdefault(row["factura"], []).append(row["finding_type"])

        records = []
        for inv in invoice_rows:
            factura = inv["factura"]
            finding_types = findings_index.get(factura, [])

            comment_parts = []
            for ft in finding_types:
                label = FINDING_LABELS.get(ft)
                if label is None:
                    logger.warning("Unknown finding_type in DB: %s", ft)
                    label = ft
                comment_parts.append(label)

            records.append(
                {
                    "Factura": factura,
                    "Fecha": inv["fecha"],
                    "Documento": inv["documento"],
                    "Numero": inv["numero"],
                    "Paciente": inv["paciente"],
                    "Administradora": inv["administradora"],
                    "Contrato": inv["contrato"],
                    "Operario": inv["operario"],
                    "Tipo": inv["tipo"],
                    "Estado carpeta": inv["folder_status"],
                    "Comentario": _COMMENT_SEPARATOR.join(comment_parts),
                    "Nota": inv["nota"],
                }
            )

        df = pd.DataFrame(records)
        if not df.empty:
            df = df.set_index("Factura")
        return df
