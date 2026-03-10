"""Stage metadata for the audit pipeline: labels, descriptions, and group layout."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageInfo:
    """Metadata for a single pipeline stage shown in the UI."""

    label: str
    description: str


STAGES: dict[str, StageInfo] = {
    "LOAD_AND_PROCESS": StageInfo(
        label="Cargar reporte SIHOS",
        description=(
            "Lee el Excel exportado de SIHOS, registra pares admin/contrato "
            "desconocidos y carga las facturas en la base de datos."
        ),
    ),
    "DOWNLOAD_DRIVE": StageInfo(
        label="Descargar carpetas faltantes desde Drive",
        description=(
            "Descarga desde Google Drive las carpetas marcadas como FALTANTE "
            "en la BD y las deposita en STAGE. Actualiza el estado a PRESENTE."
        ),
    ),
    "RUN_STAGING": StageInfo(
        label="Mover carpetas de BASE a STAGE",
        description=(
            "Busca carpetas hoja en BASE (ZIP extraído manualmente de Drive) "
            "y las mueve a STAGE para su procesamiento."
        ),
    ),
    "DOWNLOAD_MISSING_DOCS": StageInfo(
        label="Descargar documentos faltantes desde Drive",
        description=(
            "Lee los hallazgos de la BD, construye el nombre esperado de cada archivo "
            "({PREFIJO}_{NIT}_{FACTURA}.pdf) con todos los prefijos posibles y lo busca "
            "en Google Drive. Los archivos encontrados se guardan en la subcarpeta "
            "correspondiente de cada factura."
        ),
    ),
    "DOWNLOAD_INVOICES_FROM_SIHOS": StageInfo(
        label="Descargar facturas del portal SIHOS",
        description=(
            "Accede al portal SIHOS mediante Playwright y descarga los PDFs "
            "de las facturas indicadas en la lista."
        ),
    ),
    "ORGANIZE": StageInfo(
        label="Organizar carpetas de facturas",
        description=(
            "Crea la estructura final en AUDIT y mueve las facturas a sus "
            "carpetas según el reporte cargado."
        ),
    ),
    "REMOVE_NON_PDF": StageInfo(
        label="Eliminar archivos que no son PDF",
        description=(
            "Borra del directorio de trabajo todos los archivos con extensión "
            "diferente a .pdf."
        ),
    ),
    "NORMALIZE_FILES": StageInfo(
        label="Renombrar archivos con nombre inválido",
        description=(
            "Detecta archivos que no cumplen el estándar NIT/prefijo y los "
            "renombra según el esquema configurado."
        ),
    ),
    "CHECK_FOLDERS_WITH_EXTRA_TEXT": StageInfo(
        label="Detectar carpetas con texto extra",
        description=(
            "Identifica carpetas cuyo nombre contiene texto adicional además "
            "del ID de factura (p. ej. 'FE12345 COPIA')."
        ),
    ),
    "NORMALIZE_DIR_NAMES": StageInfo(
        label="Renombrar carpetas con nombre malformado",
        description=(
            "Renombra las carpetas con texto extra, dejando solo el "
            "identificador canónico de factura."
        ),
    ),
    "LIST_UNREADABLE_PDFS": StageInfo(
        label="Listar facturas sin texto extraíble",
        description=(
            "Detecta los PDFs de factura que no tienen capa de texto y "
            "necesitan OCR. Solo lista, no elimina."
        ),
    ),
    "DELETE_UNREADABLE_PDFS": StageInfo(
        label="Eliminar facturas sin texto extraíble",
        description=(
            "Elimina del sistema los PDFs de factura que no tienen texto "
            "extraíble (imágenes escaneadas sin OCR)."
        ),
    ),
    "CATEGORIZE_INVOICES": StageInfo(
        label="Categorizar facturas por tipo",
        description=(
            "Lee el contenido de cada PDF y busca las palabras clave definidas "
            "en Configuración → Tipos de factura. Asigna uno o varios tipos por carpeta."
        ),
    ),
    "VERIFY_INVOICE_CODE": StageInfo(
        label="Verificar número de factura en PDF",
        description=(
            "Comprueba que el número de factura (ej. FE12345) aparezca en el "
            "texto del PDF. Reporta las facturas donde no se encontró."
        ),
    ),
    "VERIFY_CUFE": StageInfo(
        label="Verificar CUFE en facturas",
        description=(
            "Comprueba que cada PDF de factura contenga un CUFE válido "
            "(código único de factura electrónica colombiana)."
        ),
    ),
    "TAG_MISSING_CUFE": StageInfo(
        label="Marcar carpetas sin CUFE",
        description=(
            "Agrega el sufijo ' CUFE' al nombre de la carpeta de las facturas "
            "que no tienen CUFE, para identificarlas visualmente."
        ),
    ),
    "CHECK_INVOICE_NUMBER_ON_FILES": StageInfo(
        label="Verificar número de factura en archivos",
        description=(
            "Verifica que los archivos dentro de cada carpeta tengan el mismo "
            "ID de factura que el nombre de la carpeta."
        ),
    ),
    "CHECK_INVOICES": StageInfo(
        label="Aplicar OCR a facturas",
        description=(
            "Ejecuta ocrmypdf sobre los PDFs de factura sin capa de texto, "
            "generando versiones buscables con texto embebido."
        ),
    ),
    "CHECK_INVALID_FILES": StageInfo(
        label="Detectar PDFs corruptos o ilegibles",
        description=(
            "Escanea todos los PDFs del directorio de trabajo e informa cuáles "
            "no pueden ser leídos o están dañados."
        ),
    ),
    "CHECK_DIRS": StageInfo(
        label="Detectar directorios faltantes",
        description=(
            "Compara los IDs de factura en la BD con las carpetas en disco y "
            "marca como FALTANTE las que no existen en el sistema de archivos."
        ),
    ),
    "CHECK_REQUIRED_DOCS": StageInfo(
        label="Verificar documentos requeridos por tipo",
        description=(
            "Para cada factura, determina los documentos requeridos según su tipo "
            "(definidos en Configuración) y registra hallazgos en la BD por cada uno faltante."
        ),
    ),
}

STAGE_GROUPS: list[tuple[str, list[str]]] = [
    ("Ingesta", [
        "LOAD_AND_PROCESS",
    ]),
    ("Descarga", [
        "DOWNLOAD_DRIVE",
        "DOWNLOAD_MISSING_DOCS",
        "RUN_STAGING",
    ]),
    ("Organización", [
        "ORGANIZE",
    ]),
    ("Normalización", [
        "REMOVE_NON_PDF",
        "NORMALIZE_FILES",
        "CHECK_FOLDERS_WITH_EXTRA_TEXT",
        "NORMALIZE_DIR_NAMES",
    ]),
    ("Facturas", [
        "LIST_UNREADABLE_PDFS",
        "DELETE_UNREADABLE_PDFS",
        "CATEGORIZE_INVOICES",
        "DOWNLOAD_INVOICES_FROM_SIHOS",
        "VERIFY_INVOICE_CODE",
        "VERIFY_CUFE",
        "TAG_MISSING_CUFE",
        "CHECK_INVOICE_NUMBER_ON_FILES",
    ]),
    ("Verificación", [
        "CHECK_INVOICES",
        "CHECK_INVALID_FILES",
        "CHECK_REQUIRED_DOCS",
        "CHECK_DIRS",
    ]),
]
