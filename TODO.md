# TODO

Pendientes que quedaron de la auditoría del proyecto y del trabajo de multi-hospital / progreso de etapas.
Marca cada punto al terminarlo. Lo ya hecho está en el historial de git (`git log`).

## Seguridad

- [ ] **Limitar el puerto de producción.** `docker-compose.yml` publica `"80:8000"` en todas las interfaces y la app no tiene autenticación. Publicarlo solo en una IP interna (`"192.168.x.x:80:8000"`) o en `127.0.0.1` detrás de un proxy, o cerrar el puerto con firewall. Falta definir desde qué red se accede.
- [ ] **Probar la extracción de 7z y RAR con archivos reales.** `app/services/archive.py` se probó con archivos simulados y con un 7z generado en memoria, pero no con un archivo hostil real (enlaces simbólicos, cabeceras que mienten sobre el tamaño). El RAR necesita `unrar`/`bsdtar` instalado en la imagen.
- [ ] **DNS rebinding.** El guardia de redirecciones del navegador (`app/core/urlcheck.py::is_blocked_request`) resuelve el host por su cuenta; Chromium resuelve de nuevo al conectar. Un DNS hostil podría cambiar la respuesta entre ambos. Cierre completo: pasar Chromium por un proxy de salida que valide la IP real.
- [ ] **Rotación de la clave de cifrado.** `app/crypto.py` usa Fernet con una sal fija y no hay forma de rotar `SECRET_KEY` sin perder las credenciales guardadas. Evaluar `MultiFernet`.
- [ ] **Dependencias.** `cryptography` está fijada en `>=42,<44`. Añadir `pip-audit` (o Dependabot) al CI para revisar CVEs de forma automática.
- [ ] **Valores por defecto.** `docs_enabled=True` en `app/config.py` deja Swagger abierto salvo que se apague (producción ya lo apaga). El override de desarrollo publica Postgres (5432) en toda la LAN.
- [ ] Si algún día la app sale de un equipo/red de confianza: volver a poner autenticación y valorar topes de subida (`MAX_*` en `.env`, hoy en 0 = sin límite).

## Frontend

- [ ] **Probar la UI en un navegador con clics reales** lo que ya se verificó solo llamando a los métodos de Alpine (`tests/e2e/`): barra de progreso N/total, aviso de «sin líneas nuevas», banner de tarea de otro hospital, reconexión al cerrar y reabrir la pestaña, dos hospitales en dos pestañas, ingest/anular carpetas/sobrantes.
- [x] **Migrar las llamadas `fetch` directas a `api.*`** (`audit`, `base`, `pipeline`, `settings`): todos los errores (422 en lista, 204, caída de red) se muestran igual. Quedan a propósito `fetch` solo en la descarga de ZIP (blob) y los `EventSource` de SSE. Verificado con Chromium headless contra el servidor de dev (carga, crear/borrar, errores 404/409/500 y red caída).
- [ ] **Sacar el JS embebido de las plantillas** (`settings` ~2 200 líneas, `pipeline` ~1 800, `base`, `audit`) a `app/static/js/*.js` para poder lintearlo y probarlo.
- [x] Test del JS de las plantillas: `tests/app/test_template_scripts.py` (`node --check`) y `tests/e2e/` (humo en navegador, opt-in). Falta un lint (eslint) cuando el JS salga a `static/js/`.

## Pipeline y progreso

- [x] **Barra N/total en las etapas que procesan archivos locales.** Hecho con `run_with_progress` (`stage_events.py`) y callbacks `on_progress`/`cancel_event` en `app/core` (validator, reader, standardizer, inspector). Falta probarla en el navegador (ver Frontend).
- [ ] **Partir `verification.py`** (569 líneas) y `_revisar_sobrantes` (complejidad 17, 69 sentencias) en `app/services/pipeline/stages/`.
- [ ] **Duplicación entre las etapas SIHOS** (`DOWNLOAD_INVOICES_FROM_SIHOS` y `DOWNLOAD_MEDICATION_SHEETS`): extraer la construcción del `SihosDownloader` y las comprobaciones previas.
- [ ] Estado de tareas en memoria: un reinicio del backend pierde las tareas en curso (la UI ya lo detecta con `/api/pipeline/status`). Persistirlas solo si se necesita sobrevivir a reinicios.

## Mantenibilidad y modularización

- [ ] **Routers gordos.** `explorer.py` (~800 líneas), `pipeline.py` y `invoices.py` mezclan HTTP con lógica de archivos y consultas. Mover a servicios (`services/explorer.py`, etc.). `load_drive_zip` ya salió a `services/archive.py`.
- [x] **Consultas directas en routers** movidas a `InstitutionRepo`/`InvoiceRepo` (`institutions.py`, `invoices.py`, `periods.py`); quedan solo los `repo.delete`/`update` ya encapsulados.
- [x] **Unificar la validación de rutas.** Un único contrato `app/paths.py::safe_resolve(base, target)` (relativo o absoluto); `safe_join` eliminado.
- [ ] **Tipado.** Poner un `Protocol` a `event_fn` en `app/core`, reducir los `type: ignore` (9) y los `noqa` (32), y valorar `mypy --strict` por módulos.
- [ ] **Estado global al importar.** `settings`, `audit_data_root` y `crypto._fernet` se crean al importar el módulo; hace que los tests necesiten variables de entorno falsas.
- [x] Reglas de `ruff` pendientes: `B904`, `B905`, `BLE001`, `PLW2901` corregidas y activadas en `pyproject.toml` (los 7 `except Exception` legítimos llevan `noqa` con motivo).
- [x] **Docker.** El target `api` del `Dockerfile` no lo usa ningún compose, duplica ~500 MB con `chown -R` y usa otro uid (1000) que `full` (10001). Quitarlo o alinearlo.

## Pruebas y CI

- [x] **Cobertura.** Suite completa en 60,2 % (umbral 60 % cumplido) con tests HTTP de instituciones, ajustes, hallazgos, facturas, períodos y tareas del pipeline (`tests/app/routers/`). Sigue baja en etapas (`verification.py` 26 %, `packaging.py` 20 %, `downloads.py` 27 %), `routers/api/pipeline.py` (29 %) y `core/ops.py`/`core/drive.py` (~45 %): subir el umbral cuando se cubran.
- [x] **Fixture `-m db` en Windows.** Arreglado con `NullPool` en `tests/app/repos/conftest.py`; los 37 tests `-m db` pasan en local. Usar una BD aparte con `TEST_DATABASE_URL` (el fixture hace `drop_all`; en dev existe `medical_audit_test`).
- [ ] Test de integración de extremo a extremo para dos hospitales en paralelo (hoy solo se comprobó a mano con el SIHOS simulado).

## Documentación

- [x] `docs/DOMAIN.md` dice que hay 22 etapas y `CLAUDE.md`/código tienen 24; además describe `COMPRESS_AUDIT` como «comprimir en un ZIP», cuando comprime PDFs con Ghostscript (el ZIP es `EXPORTAR_AUDITADOS`).
- [x] `README.md` y `docs/DEPLOY.md`: reflejar que no hay login y que producción corre con un solo worker.
- [ ] Actualizar `.secrets.baseline` si `detect-secrets` marca cambios de línea tras los últimos commits.

## Datos y entorno de desarrollo

- [ ] Limpiar la base de dev: hospitales de prueba (`Hospital Prueba …`, `SIHOS_FAKE_A`, `SIHOS_FAKE_B`) y los tipos de servicio/documento aleatorios creados para pruebas.
- [ ] Reconstruir la imagen de producción (`docker compose -f docker-compose.yml up --build -d`) para recoger el `Dockerfile` con 1 worker y las etapas paralelas del build.
- [ ] Documentar el SIHOS simulado y el script de siembra usados para probar la descarga (hoy viven solo en el directorio temporal de la sesión).
