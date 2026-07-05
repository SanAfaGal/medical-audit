# Modelo de dominio y pipeline de auditoría

> Ver el [README](../README.md) para la visión general del proyecto y el setup local. Ver [docs/API.md](API.md) para la referencia de endpoints.

---

## Pipeline de auditoría

El pipeline está compuesto por etapas secuenciales. Cada etapa se activa individualmente vía `GET /api/pipeline/run/{NOMBRE_ETAPA}` y retorna líneas de log como Server-Sent Events (`[INFO]`, `[WARN]`, `[ERROR]`).

| # | Nombre de etapa | Descripción |
|---|---|---|
| 1 | `LOAD_AND_PROCESS` | Ingestar Excel SIHOS → upsert de facturas en DB. Para facturas con múltiples servicios, se aplica el tipo de servicio con **mayor prioridad configurada**. |
| 2 | `RECATEGORIZE_SERVICES` | Re-aplicar los mapeos de servicio actuales sin re-importar el Excel. |
| 3 | `RUN_STAGING` | Copiar carpetas hoja (que contienen archivos directamente) desde DRIVE a STAGE. |
| 4 | `CHECK_NESTED_FOLDERS` | Detectar carpetas en STAGE que contienen subcarpetas anidadas — requieren aplanamiento manual. |
| 5 | `REMOVE_NON_PDF` | Eliminar archivos no-PDF y PDFs corruptos de STAGE. |
| 6 | `NORMALIZE_FILES` | Aplicar reglas de `PrefixCorrection` + estandarización genérica de nombres de archivo. |
| 7 | `LIST_UNREADABLE_PDFS` | Identificar PDFs de factura sin capa de texto (escaneados). |
| 8 | `DELETE_UNREADABLE_PDFS` | Mover a cuarentena los PDFs de factura que no se pueden leer. |
| 9 | `DOWNLOAD_INVOICES_FROM_SIHOS` | Descargar facturas faltantes desde SIHOS vía automatización Playwright. |
| 10 | `DOWNLOAD_MEDICATION_SHEETS` | Descargar hojas de medicamentos/servicios específicos desde SIHOS. |
| 11 | `VERIFY_INVOICE_CODE` | Confirmar que cada PDF de factura contiene su propio número de factura en el texto extraído. |
| 12 | `CHECK_INVOICE_NUMBER_ON_FILES` | Verificar que los archivos dentro de cada carpeta corresponden al número de factura de esa carpeta. |
| 13 | `CHECK_FOLDERS_WITH_EXTRA_TEXT` | Detectar carpetas con texto adicional pegado al nombre canónico. |
| 14 | `NORMALIZE_DIR_NAMES` | Renombrar carpetas malformadas al ID canónico de factura. |
| 15 | `CHECK_DIRS` | Reconciliar facturas en DB vs. carpetas en disco; marcar faltantes como `FALTANTE`. |
| 16 | `MARK_UNKNOWN_DIRS` | Validar documentos requeridos por tipo de servicio; registrar hallazgos; marcar `PENDIENTE`. |
| 17 | `REVISAR_SOBRANTES` | Revisar archivos cuyos nombres no coinciden con ningún prefijo requerido. Para cada sobrante, sugiere el tipo de documento faltante más probable (1:1 → alta confianza; N:M vía similitud `difflib` → baja confianza). El panel interactivo permite confirmar o corregir la sugerencia para renombrar el archivo en disco y resolver el hallazgo, o eliminar el archivo. |
| 18 | `VERIFY_CUFE` | Verificar el código CUFE (64+ caracteres) de factura electrónica colombiana en los PDFs. |
| 19 | `ORGANIZE` | Mover facturas elegibles (`PRESENTE`, sin hallazgos) al destino de auditoría; marcar `AUDITADA`. |
| 20 | `DOWNLOAD_DRIVE` | Sincronizar carpetas `FALTANTE` desde Google Drive; actualizar estado a `PRESENTE`. |
| 21 | `DOWNLOAD_MISSING_DOCS` | Descargar documentos faltantes específicos desde Drive para facturas con hallazgos abiertos. |
| 22 | `COMPRESS_AUDIT` | Comprimir el directorio de auditoría en un archivo ZIP. |

---

## Modelo de dominio

### Estados de carpeta de factura

| Estado | Significado |
|---|---|
| `PRESENTE` | La carpeta existe en disco |
| `FALTANTE` | Carpeta no encontrada en disco ni en Drive |
| `AUDITADA` | Completamente validada y movida al destino de auditoría |
| `PENDIENTE` | Presente pero con hallazgos de documentos abiertos |
| `REVISAR` | Marcada para revisión manual |
| `ANULAR` | Marcada para anulación |

### Entidades principales

| Entidad | Descripción |
|---|---|
| **Institution** | Hospital/clínica con NIT, credenciales SIHOS (cifradas), credenciales Drive (cifradas), logo (`BYTEA`) |
| **AuditPeriod** | Periodo de facturación que agrupa un conjunto de facturas por institución |
| **Invoice** | Registro de factura importado desde SIHOS; vinculado a Admin, Contrato, ServiceType y FolderStatus |
| **Administrator** | Mapeo de string raw de SIHOS → nombre canónico de administrador por institución |
| **Contract** | Mapeo de string raw de SIHOS → nombre canónico de contrato por institución |
| **Agreement** | Vincula un par (Administrador, Contrato) a una institución |
| **Service** | Mapeo de servicio raw de SIHOS → ServiceType por institución |
| **ServiceType** | Categoría de servicio médico (hospitalización, ambulatorio, etc.); define qué documentos son requeridos vía `ServiceTypeDocument` |
| **DocType** | Tipo de documento requerido con prefijo canónico de nombre de archivo |
| **ServiceTypeDocument** | Entidad de unión que vincula un ServiceType con un DocType requerido por institución |
| **MissingFile** | Hallazgo: documento requerido faltante para una factura específica |
| **PrefixCorrection** | Mapeo de prefijo incorrecto → forma canónica correcta (ej. `OPD_` → `OPF_`) |
| **SystemSettings** | Configuración global del sistema (ruta raíz de auditoría, etc.) |
