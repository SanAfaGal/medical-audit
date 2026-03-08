"""SQLite schema DDL.

Domain constants (InvoiceType, FindingCode, FolderStatus, FINDING_LABELS)
live in ``db.constants`` — the single source of truth.
"""

# Re-exported so existing ``from db.schema import …`` calls keep working.
from db.constants import FINDING_LABELS, FindingCode, FolderStatus, InvoiceType

__all__ = ["FINDING_LABELS", "FindingCode", "FolderStatus", "InvoiceType", "SCHEMA_DDL"]

# ---------------------------------------------------------------------------
# SQLite schema DDL
# ---------------------------------------------------------------------------

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS hospitals (
    key                       TEXT PRIMARY KEY,
    display_name              TEXT NOT NULL DEFAULT '',
    nit                       TEXT NOT NULL DEFAULT '',
    invoice_identifier_prefix TEXT NOT NULL DEFAULT '',
    sihos_base_url            TEXT NOT NULL DEFAULT '',
    sihos_invoice_doc_code    TEXT NOT NULL DEFAULT '',
    sihos_user                TEXT NOT NULL DEFAULT '',
    sihos_password            TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS admin_contract_mappings (
    id                 INTEGER PRIMARY KEY,
    hospital_key       TEXT NOT NULL REFERENCES hospitals(key) ON DELETE CASCADE,
    raw_admin          TEXT NOT NULL,
    raw_contract       TEXT,
    canonical_admin    TEXT,
    canonical_contract TEXT,
    UNIQUE(hospital_key, raw_admin, raw_contract)
);

CREATE TABLE IF NOT EXISTS invoices (
    id              INTEGER PRIMARY KEY,
    hospital        TEXT NOT NULL,
    period          TEXT NOT NULL,
    factura         TEXT NOT NULL,
    fecha           TEXT,
    documento       TEXT,
    numero          TEXT,
    paciente        TEXT,
    administradora  TEXT,
    contrato        TEXT,
    operario        TEXT,
    ruta            TEXT,
    tipo            TEXT NOT NULL DEFAULT '["GENERAL"]',
    folder_status   TEXT NOT NULL DEFAULT 'PRESENTE',
    nota            TEXT NOT NULL DEFAULT '',
    UNIQUE(hospital, period, factura)
);

CREATE INDEX IF NOT EXISTS idx_invoices_period
    ON invoices(hospital, period);

CREATE TABLE IF NOT EXISTS audit_findings (
    id           INTEGER PRIMARY KEY,
    invoice_id   INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    finding_type TEXT NOT NULL,
    UNIQUE(invoice_id, finding_type)
);

CREATE INDEX IF NOT EXISTS idx_findings_invoice
    ON audit_findings(invoice_id);

CREATE TABLE IF NOT EXISTS document_types (
    id         INTEGER PRIMARY KEY,
    code       TEXT UNIQUE NOT NULL,
    label      TEXT NOT NULL,
    prefixes   TEXT NOT NULL DEFAULT '[]',
    is_active  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS invoice_types (
    id            INTEGER PRIMARY KEY,
    code          TEXT UNIQUE NOT NULL,
    display_name  TEXT NOT NULL,
    keywords      TEXT NOT NULL DEFAULT '[]',
    required_docs TEXT NOT NULL DEFAULT '[]',
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS folder_statuses (
    id         INTEGER PRIMARY KEY,
    code       TEXT UNIQUE NOT NULL,
    label      TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0
);

"""
