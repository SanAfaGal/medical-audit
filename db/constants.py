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


# ---------------------------------------------------------------------------
# Seed data for dynamic DB tables (populated on first run)
# ---------------------------------------------------------------------------

DEFAULT_DOCUMENT_TYPES: list[dict] = [
    {"code": "FACTURA",      "label": "Factura",          "prefixes": ["FEV"]},
    {"code": "FIRMA",        "label": "Firma",            "prefixes": ["CRC"]},
    {"code": "HISTORIA",     "label": "Historia clínica", "prefixes": ["EPI", "HEV", "HAO", "HAU"]},
    {"code": "VALIDACION",   "label": "Validación",       "prefixes": ["OPF"]},
    {"code": "RESULTADOS",   "label": "Resultados",       "prefixes": ["PDX"]},
    {"code": "BITACORA",     "label": "Bitácora",         "prefixes": ["TAP"]},
    {"code": "RESOLUCION",   "label": "Resolución",       "prefixes": ["LDP"]},
    {"code": "MEDICAMENTOS", "label": "Medicamentos",     "prefixes": ["HAM"]},
    {"code": "AUTORIZACION", "label": "Autorización",     "prefixes": ["PDE"]},
    {"code": "CARPETA",      "label": "Carpeta",          "prefixes": []},
    {"code": "ORDEN",        "label": "Orden médica",     "prefixes": []},
    {"code": "FURIPS",       "label": "FURIPS",           "prefixes": []},
    {"code": "CUFE",         "label": "CUFE",             "prefixes": []},
]

DEFAULT_INVOICE_TYPES: list[dict] = [
    {"code": "GENERAL",     "display_name": "General",            "keywords": [],                    "required_docs": [],                                                                       "sort_order": 0},
    {"code": "SOAT",        "display_name": "SOAT",               "keywords": ["soat"],              "required_docs": [],                                                                       "sort_order": 10},
    {"code": "LABORATORIO", "display_name": "Laboratorio",        "keywords": ["laboratorio clinico"], "required_docs": ["FACTURA", "RESULTADOS", "FIRMA", "VALIDACION"],                     "sort_order": 20},
    {"code": "ECG",         "display_name": "Electrocardiograma", "keywords": ["electrocard"],       "required_docs": ["FACTURA", "RESULTADOS", "FIRMA", "VALIDACION"],                        "sort_order": 20},
    {"code": "RADIOGRAFIA", "display_name": "Radiografía",        "keywords": ["radiograf"],         "required_docs": ["FACTURA", "RESULTADOS", "FIRMA", "VALIDACION"],                        "sort_order": 20},
    {"code": "ODONTOLOGIA", "display_name": "Odontología",        "keywords": ["odontolog"],         "required_docs": ["FACTURA", "HISTORIA", "FIRMA", "VALIDACION"],                          "sort_order": 20},
    {"code": "POLICLINICA", "display_name": "Policlínica",        "keywords": ["p909000"],           "required_docs": [],                                                                       "sort_order": 20},
    {"code": "URGENCIAS",   "display_name": "Urgencias",          "keywords": ["urgencia"],          "required_docs": ["FACTURA", "HISTORIA", "FIRMA", "AUTORIZACION"],                        "sort_order": 20},
    {"code": "AMBULANCIA",  "display_name": "Ambulancia",         "keywords": ["ambulancia"],        "required_docs": ["FACTURA", "HISTORIA", "FIRMA", "AUTORIZACION", "BITACORA", "RESOLUCION"], "sort_order": 30},
]

DEFAULT_FOLDER_STATUSES: list[dict] = [
    {"code": "PRESENTE",  "label": "Presente",  "sort_order": 0},
    {"code": "PENDIENTE", "label": "Pendiente", "sort_order": 1},
    {"code": "FALTANTE",  "label": "Faltante",  "sort_order": 2},
    {"code": "AUDITADA",  "label": "Auditada",  "sort_order": 3},
]

# Migration map: old MISSING_* finding codes → new clean codes
FINDING_CODE_MIGRATION: dict[str, str] = {
    "MISSING_INVOICE":      "FACTURA",
    "MISSING_HISTORIA":     "HISTORIA",
    "MISSING_FIRMA":        "FIRMA",
    "MISSING_VALIDACION":   "VALIDACION",
    "MISSING_RESULTADOS":   "RESULTADOS",
    "MISSING_BITACORA":     "BITACORA",
    "MISSING_RESOLUCION":   "RESOLUCION",
    "MISSING_AUTORIZACION": "AUTORIZACION",
    "MISSING_ORDEN":        "ORDEN",
    "MISSING_FURIPS":       "FURIPS",
    "MISSING_CUFE":         "CUFE",
    "MISSING_FOLDER":       "CARPETA",
}
