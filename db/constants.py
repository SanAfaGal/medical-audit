"""Domain constants: invoice types, finding codes, folder statuses, and display labels.

This is the single source of truth for all enumerated domain values.
``db.schema`` keeps only the SQLite DDL; everything else lives here.
"""

from enum import StrEnum


# ---------------------------------------------------------------------------
# Invoice types
# ---------------------------------------------------------------------------


class InvoiceType(StrEnum):
    """String constants that classify the type of an invoice."""

    GENERAL     = "GENERAL"
    SOAT        = "SOAT"
    LABORATORIO = "LABORATORIO"
    URGENCIAS   = "URGENCIAS"
    POLICLINICA = "POLICLINICA"
    ECG         = "ECG"
    RADIOGRAFIA = "RADIOGRAFIA"
    AMBULANCIA  = "AMBULANCIA"
    ODONTOLOGIA = "ODONTOLOGIA"


# ---------------------------------------------------------------------------
# Finding codes
# ---------------------------------------------------------------------------


class FindingCode(StrEnum):
    """String constants that identify the type of an audit finding."""

    MISSING_FOLDER       = "MISSING_FOLDER"
    MISSING_INVOICE      = "MISSING_INVOICE"
    MISSING_HISTORIA     = "MISSING_HISTORIA"
    MISSING_FIRMA        = "MISSING_FIRMA"
    MISSING_VALIDACION   = "MISSING_VALIDACION"
    MISSING_RESULTADOS   = "MISSING_RESULTADOS"
    MISSING_BITACORA     = "MISSING_BITACORA"
    MISSING_RESOLUCION   = "MISSING_RESOLUCION"
    MISSING_AUTORIZACION = "MISSING_AUTORIZACION"
    MISSING_ORDEN        = "MISSING_ORDEN"
    MISSING_FURIPS       = "MISSING_FURIPS"
    MISSING_CUFE         = "MISSING_CUFE"


# Human-readable Spanish labels used in exported reports and UI dropdowns.
FINDING_LABELS: dict[str, str] = {
    FindingCode.MISSING_FOLDER:       "Carpeta",
    FindingCode.MISSING_INVOICE:      "Factura",
    FindingCode.MISSING_HISTORIA:     "Historia clínica",
    FindingCode.MISSING_FIRMA:        "Firma",
    FindingCode.MISSING_VALIDACION:   "Validación",
    FindingCode.MISSING_RESULTADOS:   "Resultados",
    FindingCode.MISSING_BITACORA:     "Bitácora",
    FindingCode.MISSING_RESOLUCION:   "Resolución",
    FindingCode.MISSING_AUTORIZACION: "Autorización",
    FindingCode.MISSING_ORDEN:        "Orden médica",
    FindingCode.MISSING_FURIPS:       "FURIPS",
    FindingCode.MISSING_CUFE:         "CUFE",
}


# ---------------------------------------------------------------------------
# Folder status
# ---------------------------------------------------------------------------


class FolderStatus(StrEnum):
    """String constants for the physical presence state of an invoice folder."""

    PRESENT = "PRESENTE"
    PENDING = "PENDIENTE"
    MISSING = "FALTANTE"
