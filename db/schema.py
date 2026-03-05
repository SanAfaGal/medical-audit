"""Finding code, folder-status constants, plus the SQLite schema DDL."""

# ---------------------------------------------------------------------------
# Invoice types
# ---------------------------------------------------------------------------


class InvoiceType:
    """String constants that classify the type of an invoice."""

    GENERAL      = "GENERAL"
    SOAT         = "SOAT"
    LABORATORIO  = "LABORATORIO"
    URGENCIAS    = "URGENCIAS"
    POLICLINICA  = "POLICLINICA"
    ECG          = "ECG"
    RADIOGRAFIA  = "RADIOGRAFIA"

    _ALL: frozenset[str] = frozenset()


InvoiceType._ALL = frozenset(
    v for k, v in vars(InvoiceType).items() if not k.startswith("_")
)

# ---------------------------------------------------------------------------
# Finding codes
# ---------------------------------------------------------------------------


class FindingCode:
    """String constants that identify the type of an audit finding."""

    MISSING_FOLDER = "MISSING_FOLDER"
    MISSING_INVOICE = "MISSING_INVOICE"
    MISSING_HISTORIA = "MISSING_HISTORIA"
    MISSING_FIRMA = "MISSING_FIRMA"
    MISSING_VALIDACION = "MISSING_VALIDACION"
    MISSING_RESULTADOS = "MISSING_RESULTADOS"
    MISSING_BITACORA = "MISSING_BITACORA"
    MISSING_RESOLUCION = "MISSING_RESOLUCION"
    MISSING_MEDICAMENTOS = "MISSING_MEDICAMENTOS"
    MISSING_AUTORIZACION = "MISSING_AUTORIZACION"
    MISSING_ORDEN = "MISSING_ORDEN"
    MISSING_FURIPS = "MISSING_FURIPS"
    MISSING_CUFE = "MISSING_CUFE"
    MISSING_INVOICE_CODE = "MISSING_INVOICE_CODE"
    FILE_NEEDS_CORRECTION = "FILE_NEEDS_CORRECTION"

    _ALL: frozenset[str] = frozenset()


FindingCode._ALL = frozenset(
    v for k, v in vars(FindingCode).items() if not k.startswith("_")
)

# Human-readable labels for the "Comentario" column in exported reports.
FINDING_LABELS: dict[str, str] = {
    FindingCode.MISSING_FOLDER: "Carpeta faltante",
    FindingCode.MISSING_INVOICE: "Factura faltante",
    FindingCode.MISSING_HISTORIA: "Historia clinica faltante",
    FindingCode.MISSING_FIRMA: "Firma faltante",
    FindingCode.MISSING_VALIDACION: "Validacion faltante",
    FindingCode.MISSING_RESULTADOS: "Resultados faltantes",
    FindingCode.MISSING_BITACORA: "Bitacora faltante",
    FindingCode.MISSING_RESOLUCION: "Resolucion faltante",
    FindingCode.MISSING_MEDICAMENTOS: "Medicamentos faltantes",
    FindingCode.MISSING_AUTORIZACION: "Autorizacion faltante",
    FindingCode.MISSING_ORDEN: "Orden faltante",
    FindingCode.MISSING_FURIPS: "FURIPS faltante",
    FindingCode.MISSING_CUFE: "CUFE faltante",
    FindingCode.MISSING_INVOICE_CODE: "Codigo de factura no encontrado en PDF",
    FindingCode.FILE_NEEDS_CORRECTION: "Archivo requiere correccion",
}


# ---------------------------------------------------------------------------
# Folder status
# ---------------------------------------------------------------------------


class FolderStatus:
    """String constants for the physical presence state of an invoice folder."""

    PRESENT  = "PRESENTE"
    PENDING  = "PENDIENTE"
    MISSING  = "FALTANTE"

    _ALL: frozenset[str] = frozenset()


FolderStatus._ALL = frozenset(
    v for k, v in vars(FolderStatus).items() if not k.startswith("_")
)


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
    document_standards        TEXT NOT NULL DEFAULT '{}',
    misnamed_fixer_map        TEXT NOT NULL DEFAULT '{}'
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
    tipo            TEXT NOT NULL DEFAULT 'GENERAL',
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
"""
