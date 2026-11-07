# TODO

Pendientes que quedaron de la auditoría del proyecto y del trabajo de multi-hospital / progreso de etapas.
Marca cada punto al terminarlo. Lo ya hecho está en el historial de git (`git log`).

## Seguridad

- [ ] **Firewall del equipo que despliega** (el acceso desde la LAN es intencional): crear la regla solo para la subred local y perfil Privado, y confirmar que la red del equipo está marcada como «Privada». Los comandos están en `docs/DEPLOY.md` («Acceso desde la red LAN»). Pendiente de hacerlo en el equipo real; sin login, quien llegue al puerto lo ve todo.
- [ ] **Probar la extracción de 7z y RAR con archivos reales.** `app/services/archive.py` se probó con archivos simulados y con un 7z generado en memoria, pero no con un archivo hostil real (enlaces simbólicos, cabeceras que mienten sobre el tamaño). El RAR necesita `unrar`/`bsdtar` instalado en la imagen.
- [ ] **DNS rebinding.** El guardia de redirecciones del navegador (`app/core/urlcheck.py::is_blocked_request`) resuelve el host por su cuenta; Chromium resuelve de nuevo al conectar. Un DNS hostil podría cambiar la respuesta entre ambos. Cierre completo: pasar Chromium por un proxy de salida que valide la IP real.
- [ ] **Rotación de la clave de cifrado.** `app/crypto.py` usa Fernet con una sal fija y no hay forma de rotar `SECRET_KEY` sin perder las credenciales guardadas. Evaluar `MultiFernet`.
- [ ] **Dependencias.** `cryptography` está fijada en `>=42,<44`. Añadir `pip-audit` (o Dependabot) al CI para revisar CVEs de forma automática.
- [ ] **Valores por defecto.** `docs_enabled=True` en `app/config.py` deja Swagger abierto salvo que se apague (producción ya lo apaga). El override de desarrollo publica Postgres (5432) en toda la LAN.
- [ ] Si algún día la app sale de un equipo/red de confianza: volver a poner autenticación y valorar topes de subida (`MAX_*` en `.env`, hoy en 0 = sin límite).

## Frontend

- [ ] **Probar la UI en un navegador con clics reales** lo que ya se verificó solo llamando a los métodos de Alpine (`tests/e2e/`): barra de progreso N/total, aviso de «sin líneas nuevas», banner de tarea de otro hospital, reconexión al cerrar y reabrir la pestaña, dos hospitales en dos pestañas, ingest/anular carpetas/sobrantes.
- [x] **Migrar las llamadas `fetch` directas a `api.*`** (`audit`, `base`, `pipeline`, `settings`): todos los errores (422 en lista, 204, caída de red) se muestran igual. Quedan a propósito `fetch` solo en la descarga de ZIP (blob) y los `EventSource` de SSE. Verificado con Chromium headless contra el servidor de dev (carga, crear/borrar, errores 404/409/500 y red caída).
- [x] **JS fuera de las plantillas.** Cada página carga su script desde `app/static/js/` (`audit.js`, `pipeline.js`, `settings.js`, `help.js`, `explorer.js`, `stores.js`, `tailwind-config.js`) con `asset()` (versión por mtime). Un test impide volver a poner `<script>` en línea. Pendiente: eslint y partir `settings.js` (624 líneas) y `explorer.js` (788) en módulos por pestaña.
- [x] Test del JS: `tests/app/test_template_scripts.py` (`node --check` de `static/js`, sin scripts en línea ni `fetch` directos) y `tests/e2e/` (humo en navegador, opt-in). Falta un lint (eslint).

## Pipeline y progreso

- [x] **Barra N/total en las etapas que procesan archivos locales.** Hecho con `run_with_progress` (`stage_events.py`) y callbacks `on_progress`/`cancel_event` en `app/core` (validator, reader, standardizer, inspector). Falta probarla en el navegador (ver Frontend).
- [x] **`verification.py` partido** en `invoice_checks.py`, `folder_checks.py` y `findings.py`; la lógica de sobrantes y de documentos requeridos salió a `services/pipeline/surplus.py` y `required_docs.py` (puras y con tests).
- [ ] **Duplicación entre las etapas SIHOS** (`DOWNLOAD_INVOICES_FROM_SIHOS` y `DOWNLOAD_MEDICATION_SHEETS`): extraer la construcción del `SihosDownloader` y las comprobaciones previas.
- [ ] Estado de tareas en memoria: un reinicio del backend pierde las tareas en curso (la UI ya lo detecta con `/api/pipeline/status`). Persistirlas solo si se necesita sobrevivir a reinicios.

## Mantenibilidad y modularización

- [ ] **Routers gordos.** `explorer.py` ya quedó delgado (809 → 324 líneas; la lógica vive en `services/explorer/` y los servicios lanzan `ExplorerError`). Faltan `pipeline.py` y `invoices.py`, que aún mezclan HTTP con lógica de archivos (anular carpetas, sobrantes, exportar a Excel, ZIP de AUDIT).
- [x] **Consultas directas en routers** movidas a `InstitutionRepo`/`InvoiceRepo` (`institutions.py`, `invoices.py`, `periods.py`); quedan solo los `repo.delete`/`update` ya encapsulados.
- [x] **Unificar la validación de rutas.** Un único contrato `app/paths.py::safe_resolve(base, target)` (relativo o absoluto); `safe_join` eliminado.
- [x] **Tipado.** `mypy --strict` en toda la app (solo se relajan `disallow_untyped_defs`/`disallow_incomplete_defs` en `app/routers/*`, y `disallow_untyped_calls` en `core/drive.py` por google-auth). `EventFn`, `ProgressFn` y `ThreadRunner` son Protocols; `type: ignore` 11 → 1 y `noqa: E402` 24 → 9 (los que quedan son los imports diferidos de `models/` contra ciclos).
- [ ] **Estado global al importar.** `settings`, `audit_data_root` y `crypto._fernet` se crean al importar el módulo; hace que los tests necesiten variables de entorno falsas.
- [x] Reglas de `ruff` pendientes: `B904`, `B905`, `BLE001`, `PLW2901` corregidas y activadas en `pyproject.toml` (los 7 `except Exception` legítimos llevan `noqa` con motivo).
- [x] **Docker.** El target `api` del `Dockerfile` no lo usa ningún compose, duplica ~500 MB con `chown -R` y usa otro uid (1000) que `full` (10001). Quitarlo o alinearlo.

## Pruebas y CI

- [x] **Cobertura.** Suite completa en 60,2 % (umbral 60 % cumplido) con tests HTTP de instituciones, ajustes, hallazgos, facturas, períodos y tareas del pipeline (`tests/app/routers/`). Sigue baja en etapas (`verification.py` 26 %, `packaging.py` 20 %, `downloads.py` 27 %), `routers/api/pipeline.py` (29 %) y `core/ops.py`/`core/drive.py` (~45 %): subir el umbral cuando se cubran.
- [x] **Fixture `-m db` en Windows.** Arreglado con `NullPool` en `tests/app/repos/conftest.py`; los 37 tests `-m db` pasan en local. Usar una BD aparte con `TEST_DATABASE_URL` (el fixture hace `drop_all`; en dev existe `medical_audit_test`).
- [ ] Test de integración de extremo a extremo para dos hospitales en paralelo (hoy solo se comprobó a mano con el SIHOS simulado).

## Rendimiento

Medido con 1000 facturas / 6000 PDFs sintéticos en SSD local de 18 núcleos (el PDF dominó el tiempo; recorrer el disco costó < 0,5 s). Ya hecho: `find_unopenable` en una pasada, caché compartida entre VERIFY_INVOICE_CODE y VERIFY_CUFE, y pool de procesos para leer PDFs (`PDF_WORKERS`).

- [ ] **Medir en el servidor real**: núcleos y memoria de producción (cada proceso del pool ocupa ~70 MB; con 8 son ~560 MB). Si el contenedor tiene poca RAM, bajar `PDF_WORKERS` o ponerle un `mem_limit`.
- [ ] **Calentar el pool al arrancar** (hoy el primer escaneo paga ~1 s de arranque de procesos en Windows).
- [ ] **No medido todavía**: disco de red (el recorrido de carpetas sí pesaría), PDFs escaneados reales (imágenes grandes), `COMPRESS_AUDIT` (Ghostscript), las consultas SQL con muchas facturas (`filter_invoices`, `get_stats`) y las descargas SIHOS/Drive.
- [ ] `/api/invoices/ids` y la exportación a Excel cargan todo sin paginar: revisar con períodos grandes.

## Documentación

- [x] `docs/DOMAIN.md` dice que hay 22 etapas y `CLAUDE.md`/código tienen 24; además describe `COMPRESS_AUDIT` como «comprimir en un ZIP», cuando comprime PDFs con Ghostscript (el ZIP es `EXPORTAR_AUDITADOS`).
- [x] `README.md` y `docs/DEPLOY.md`: reflejar que no hay login y que producción corre con un solo worker.
- [ ] Actualizar `.secrets.baseline` si `detect-secrets` marca cambios de línea tras los últimos commits.

## Datos y entorno de desarrollo

- [ ] Limpiar la base de dev: hospitales de prueba (`Hospital Prueba …`, `SIHOS_FAKE_A`, `SIHOS_FAKE_B`) y los tipos de servicio/documento aleatorios creados para pruebas.
- [ ] Reconstruir la imagen de producción (`docker compose -f docker-compose.yml up --build -d`) para recoger el `Dockerfile` con 1 worker y las etapas paralelas del build.
- [ ] Documentar el SIHOS simulado y el script de siembra usados para probar la descarga (hoy viven solo en el directorio temporal de la sesión).
