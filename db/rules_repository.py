"""Mixin with CRUD operations for the dynamic business-rules tables.

Provides read/write access to ``document_types``, ``invoice_types``, and
``folder_statuses``.  Mixed into ``AuditRepository`` — do not instantiate
this class directly.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


class RulesRepositoryMixin:
    """CRUD methods for the three dynamic business-rules tables."""

    # Subclass must provide _connect()
    def _connect(self): ...  # type: ignore[empty-body]

    # ------------------------------------------------------------------
    # document_types
    # ------------------------------------------------------------------

    def fetch_document_types(self) -> list[dict]:
        """Return all document types ordered by code.

        Returns:
            List of dicts with keys: id, code, label, prefixes (list), is_active.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, code, label, prefixes, is_active FROM document_types ORDER BY code"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["prefixes"] = json.loads(d["prefixes"])
            except (ValueError, TypeError):
                d["prefixes"] = []
            result.append(d)
        return result

    def fetch_document_labels(self) -> dict[str, str]:
        """Return a mapping of document_type code → label.

        Returns:
            Dict used for display labels (replaces the old ``FINDING_LABELS`` constant).
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT code, label FROM document_types"
            ).fetchall()
        return {r["code"]: r["label"] for r in rows}

    def upsert_document_type(
        self, code: str, label: str, prefixes: list[str], is_active: int = 1
    ) -> None:
        """Insert or update a document type.

        Args:
            code: Unique identifier (e.g. ``"FACTURA"``).
            label: Human-readable Spanish label.
            prefixes: List of filename prefix strings to search for.
            is_active: 1 if active, 0 if disabled.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO document_types (code, label, prefixes, is_active)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    label     = excluded.label,
                    prefixes  = excluded.prefixes,
                    is_active = excluded.is_active
                """,
                (code, label, json.dumps(prefixes), is_active),
            )
        logger.info("upsert_document_type: %s", code)

    def delete_document_type(self, code: str) -> None:
        """Delete a document type by code.

        Args:
            code: Document type code to remove.
        """
        with self._connect() as conn:
            conn.execute("DELETE FROM document_types WHERE code = ?", (code,))
        logger.info("delete_document_type: %s", code)

    def _build_document_standards(self) -> dict:
        """Build the legacy DOCUMENT_STANDARDS dict from the document_types table.

        Returns values as a string when there is only one prefix, or a list
        when there are multiple — matching the format the pipeline expects.

        Returns:
            Dict mapping document code → prefix string or list of strings.
        """
        doc_types = self.fetch_document_types()
        result: dict = {}
        for dt in doc_types:
            if not dt["is_active"]:
                continue
            prefixes = dt["prefixes"]
            if len(prefixes) == 1:
                result[dt["code"]] = prefixes[0]
            elif prefixes:
                result[dt["code"]] = prefixes
        return result

    # ------------------------------------------------------------------
    # invoice_types
    # ------------------------------------------------------------------

    def fetch_invoice_types(self) -> list[dict]:
        """Return all invoice types ordered by sort_order descending, then code.

        Returns:
            List of dicts with keys: id, code, display_name, keywords (list),
            required_docs (list), sort_order, is_active.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, code, display_name, keywords, required_docs, sort_order, is_active "
                "FROM invoice_types ORDER BY sort_order DESC, code"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for key in ("keywords", "required_docs"):
                try:
                    d[key] = json.loads(d[key])
                except (ValueError, TypeError):
                    d[key] = []
            result.append(d)
        return result

    def upsert_invoice_type(
        self,
        code: str,
        display_name: str,
        keywords: list[str],
        required_docs: list[str],
        sort_order: int = 0,
        is_active: int = 1,
    ) -> None:
        """Insert or update an invoice type.

        Args:
            code: Unique identifier (e.g. ``"AMBULANCIA"``).
            display_name: Human-readable Spanish name.
            keywords: Lowercase strings to search in PDF content.
            required_docs: List of document_type codes required for this invoice type.
            sort_order: Detection priority — higher value = checked first.
            is_active: 1 if active, 0 if disabled.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO invoice_types
                    (code, display_name, keywords, required_docs, sort_order, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    display_name  = excluded.display_name,
                    keywords      = excluded.keywords,
                    required_docs = excluded.required_docs,
                    sort_order    = excluded.sort_order,
                    is_active     = excluded.is_active
                """,
                (
                    code,
                    display_name,
                    json.dumps(keywords),
                    json.dumps(required_docs),
                    sort_order,
                    is_active,
                ),
            )
        logger.info("upsert_invoice_type: %s", code)

    def delete_invoice_type(self, code: str) -> None:
        """Delete an invoice type by code.

        Args:
            code: Invoice type code to remove.
        """
        with self._connect() as conn:
            conn.execute("DELETE FROM invoice_types WHERE code = ?", (code,))
        logger.info("delete_invoice_type: %s", code)

    # ------------------------------------------------------------------
    # folder_statuses
    # ------------------------------------------------------------------

    def fetch_folder_statuses(self) -> list[dict]:
        """Return all folder statuses ordered by sort_order.

        Returns:
            List of dicts with keys: id, code, label, sort_order.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, code, label, sort_order FROM folder_statuses ORDER BY sort_order, code"
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_folder_status(self, code: str, label: str, sort_order: int = 0) -> None:
        """Insert or update a folder status.

        Args:
            code: Unique identifier (e.g. ``"PRESENTE"``).
            label: Human-readable Spanish label.
            sort_order: Display order.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO folder_statuses (code, label, sort_order)
                VALUES (?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    label      = excluded.label,
                    sort_order = excluded.sort_order
                """,
                (code, label, sort_order),
            )
        logger.info("upsert_folder_status: %s", code)

    def delete_folder_status(self, code: str) -> None:
        """Delete a folder status by code.

        Args:
            code: Folder status code to remove.
        """
        with self._connect() as conn:
            conn.execute("DELETE FROM folder_statuses WHERE code = ?", (code,))
        logger.info("delete_folder_status: %s", code)
