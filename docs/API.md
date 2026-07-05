# API Reference

> Ver el [README](../README.md) para la visión general del proyecto y el setup local.

Todos los endpoints tienen el prefijo `/api`. Swagger UI disponible en `/docs` cuando `DOCS_ENABLED=true`.

---

### Instituciones — `/api/institutions`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/institutions` | Listar todas las instituciones |
| `POST` | `/api/institutions` | Crear institución |
| `GET` | `/api/institutions/{id}` | Obtener institución por ID |
| `PATCH` | `/api/institutions/{id}` | Actualizar institución |
| `DELETE` | `/api/institutions/{id}` | Eliminar institución |
| `GET` | `/api/institutions/{id}/logo` | Servir logo (desde DB) |
| `POST` | `/api/institutions/{id}/logo` | Subir logo (PNG/JPEG/WebP/AVIF/GIF) |
| `POST` | `/api/institutions/{id}/drive-credentials` | Subir cuenta de servicio Google Drive (JSON) |
| `POST` | `/api/institutions/{id}/sihos-password` | Guardar contraseña SIHOS (cifrada en DB) |
| `GET` | `/api/institutions/{id}/admins` | Listar admins (`?pending_only=true` para sin mapear) |
| `POST` | `/api/institutions/{id}/admins` | Crear mapeo de admin |
| `PATCH` | `/api/institutions/admins/{admin_id}` | Actualizar mapeo de admin |
| `DELETE` | `/api/institutions/admins/{admin_id}` | Eliminar mapeo de admin |
| `GET` | `/api/institutions/{id}/contracts` | Listar contratos |
| `POST` | `/api/institutions/{id}/contracts` | Crear mapeo de contrato |
| `PATCH` | `/api/institutions/contracts/{contract_id}` | Actualizar mapeo de contrato |
| `DELETE` | `/api/institutions/contracts/{contract_id}` | Eliminar mapeo de contrato |
| `GET` | `/api/institutions/{id}/services` | Listar mapeos de servicio |
| `POST` | `/api/institutions/{id}/services` | Crear mapeo de servicio |
| `PATCH` | `/api/institutions/services/{service_id}` | Actualizar mapeo de servicio |
| `DELETE` | `/api/institutions/services/{service_id}` | Eliminar mapeo de servicio |
| `GET` | `/api/institutions/{id}/service-type-documents` | Listar documentos requeridos por tipo de servicio |
| `POST` | `/api/institutions/{id}/service-type-documents` | Agregar documento requerido a tipo de servicio |
| `DELETE` | `/api/institutions/{id}/service-type-documents/{st_id}/{dt_id}` | Quitar documento requerido |

### Periodos de auditoría — `/api/institutions/{id}/periods`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/institutions/{id}/periods` | Listar periodos de la institución |
| `POST` | `/api/institutions/{id}/periods` | Crear periodo |
| `DELETE` | `/api/periods/{id}` | Eliminar periodo |

### Facturas — `/api/invoices`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/invoices` | Listar facturas (filtros: periodo, estado, admin, contrato, servicio, búsqueda) |
| `GET` | `/api/invoices/ids` | IDs de facturas que coinciden con los filtros actuales |
| `GET` | `/api/invoices/stats` | Conteos por estado y total de hallazgos |
| `GET` | `/api/invoices/findings-summary` | Conteo de hallazgos sin resolver por tipo de documento |
| `GET` | `/api/invoices/export` | Exportar facturas del periodo a Excel (.xlsx) |
| `GET` | `/api/invoices/{id}` | Detalle de una factura |
| `POST` | `/api/invoices` | Crear factura |
| `PATCH` | `/api/invoices/{id}` | Actualizar factura (estado, tipo de servicio) |
| `POST` | `/api/invoices/ingest` | Ingestar Excel SIHOS (multipart) |
| `POST` | `/api/invoices/batch-status` | Actualización masiva de estados |
| `DELETE` | `/api/invoices/{id}` | Eliminar factura |
| `POST` | `/api/invoices/{id}/rename-surplus` | Renombrar archivo sobrante al prefijo correcto y resolver hallazgo |
| `POST` | `/api/invoices/{id}/delete-surplus` | Eliminar archivo sobrante del disco |

### Hallazgos — `/api/missing-files`

Registros de documentos requeridos faltantes por factura.

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/missing-files/{invoice_id}` | Obtener hallazgos de una factura |
| `POST` | `/api/missing-files` | Registrar hallazgo |
| `PATCH` | `/api/missing-files/{invoice_id}/{doc_type_id}/resolve` | Marcar hallazgo como resuelto |
| `DELETE` | `/api/missing-files/{invoice_id}/{doc_type_id}` | Eliminar hallazgo |
| `DELETE` | `/api/missing-files/{invoice_id}` | Eliminar todos los hallazgos de una factura |
| `POST` | `/api/missing-files/batch-delete` | Eliminación masiva de hallazgos |

### Pipeline — `/api/pipeline`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/pipeline/run/{stage}` | Ejecutar etapa; retorna stream **Server-Sent Events** |
| `POST` | `/api/pipeline/run/{stage}` | Iniciar etapa en background (retorna `task_id`) |
| `GET` | `/api/pipeline/task/{task_id}` | Consultar estado de tarea en background |
| `GET` | `/api/pipeline/stream/{task_id}` | Stream de logs de tarea en background (SSE) |

Ver [docs/DOMAIN.md](DOMAIN.md#pipeline-de-auditoría) para el detalle de las 22 etapas del pipeline.

### Configuración — `/api/settings`

| Método | Ruta | Descripción |
|---|---|---|
| `GET/POST` | `/api/settings/service-types` | Listar / crear tipos de servicio |
| `PATCH/DELETE` | `/api/settings/service-types/{id}` | Actualizar / eliminar tipo de servicio |
| `GET/POST` | `/api/settings/doc-types` | Listar / crear tipos de documento |
| `PATCH/DELETE` | `/api/settings/doc-types/{id}` | Actualizar / eliminar tipo de documento |
| `GET/POST` | `/api/settings/folder-statuses` | Listar / crear estados de carpeta |
| `PATCH/DELETE` | `/api/settings/folder-statuses/{id}` | Actualizar / eliminar estado de carpeta |
| `GET/POST` | `/api/settings/prefix-corrections` | Listar / crear reglas de corrección de prefijos |
| `PATCH/DELETE` | `/api/settings/prefix-corrections/{id}` | Actualizar / eliminar regla |
| `GET` | `/api/settings/system` | Obtener configuración global del sistema |
| `PATCH` | `/api/settings/system` | Actualizar ruta base de auditoría y otras opciones globales |

### Explorador de archivos — `/api/explorer`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/explorer/list` | Listar archivos y carpetas en el sandbox |
| `POST` | `/api/explorer/mkdir` | Crear carpeta |
| `POST` | `/api/explorer/upload` | Subir archivo |
| `POST` | `/api/explorer/delete` | Eliminar archivo o carpeta |
| `POST` | `/api/explorer/rename` | Renombrar archivo o carpeta |
| `POST` | `/api/explorer/move` | Mover a otra carpeta |
| `POST` | `/api/explorer/copy` | Copiar archivo o carpeta |
| `POST` | `/api/explorer/merge` | Fusionar carpetas de factura |
| `POST` | `/api/explorer/split` | Dividir carpeta de factura |
| `POST` | `/api/explorer/reorder` | Reordenar archivos dentro de una carpeta |
| `POST` | `/api/explorer/batch-delete` | Eliminar múltiples ítems |
| `GET` | `/api/explorer/download` | Descargar archivos/carpetas como ZIP |
