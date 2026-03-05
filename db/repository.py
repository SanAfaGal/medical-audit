"""SQLite-backed repository for audit findings per invoice."""

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
            ):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # Column already exists

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
