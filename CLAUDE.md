# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git commits y pushing
Nunca crees un commit ni hagas push a remoto a menos que el usuario lo solicite explícitamente en el mensaje actual. No hagas auto-commit tras completar tareas. Siempre espera a que el usuario revise los cambios y lo pida.

## Documentación del proyecto

El README.md es solo orientación rápida (stack, arquitectura, setup). El detalle vive en:
- `docs/DEVELOPMENT.md` — comandos `dev.sh`, migraciones, backups locales, testing, variables de entorno
- `docs/DEPLOY.md` — runbook completo de producción (`deploy.sh`, rollback, backups, firewall)
- `docs/MIGRATION.md` — migrar el stack completo (DB + volumen de PDFs) a otra máquina
- `docs/API.md` — referencia completa de endpoints REST
- `docs/DOMAIN.md` — modelo de dominio (entidades, estados) y las 22 etapas del pipeline

## Comandos

Entorno gestionado con `uv`. Backend nativo corre en Windows; solo Postgres está en Docker por defecto (ver `./dev.sh docker-up` para correr también el backend en Docker).

```bash
./dev.sh db                              # Levantar Postgres (Docker)
./dev.sh migrate                         # Aplicar migraciones Alembic pendientes
./dev.sh serve                           # Uvicorn con hot-reload en 0.0.0.0:8000
./dev.sh test                            # pytest completo
./dev.sh test -k test_invoice -v         # Un test específico por nombre
./dev.sh test -m "not db and not slow"   # Solo tests unitarios rápidos (sin Postgres)
./dev.sh test tests/core/test_scanner.py # Un archivo específico
./dev.sh lint                            # ruff check + format check
./dev.sh format                          # Auto-formatear con ruff
uv run mypy app/                         # Type checking (sin wrapper en dev.sh)
```

Pytest markers (`pyproject.toml`): `db` (requiere Postgres vivo), `slow` (archivos grandes), `pdf` (requiere fixtures PDF reales). CI corre `-m db` con `--cov-fail-under=60` sobre `core/` + `app/`.

**Gotcha conocido:** el fixture compartido de `-m db` (`tests/app/repos/conftest.py::test_engine`, session-scoped) falla en algunos entornos locales de Windows con `InterfaceError: cannot perform operation: another operation is in progress` — un problema de versión asyncpg/event-loop, no del código. Si un test `-m db` nuevo falla ahí durante desarrollo, verificar con un script standalone (un solo event loop de principio a fin, sin el fixture compartido) antes de asumir que el fix está mal; el test en sí es válido y corre bien en CI.

## Arquitectura

Capas con dirección de dependencia estricta: `routers → services → repositories → models`. Los routers instancian repos directamente vía `Depends(get_db)`; los repos reciben `AsyncSession` y no hacen commit salvo que el método sea explícitamente transaccional — el commit final es responsabilidad del router.

**`core/` es una librería framework-agnostic** — no importa nada de `app/` (ni FastAPI ni SQLAlchemy). Contiene el procesamiento de documentos puro: escaneo (`scanner.py`), extracción de texto (`reader.py`), compresión Ghostscript (`processor.py`), validación (`validator.py`, `inspector.py`), normalización de nombres (`standardizer.py`, `organizer.py`, `ops.py`), y las integraciones externas (`downloader.py` vía Playwright para SIHOS, `drive.py` para Google Drive). Solo `app/services/pipeline_runner.py` importa `core/` — si necesitás tocar lógica de dominio, primero mirá si ya vive ahí antes de agregarla en `app/`.

**Pipeline de auditoría** (`app/services/pipeline_runner.py`): registro de 23 etapas vía decorador `@_stage("NOMBRE")`, cada una un async generator que hace `yield` de líneas de log con formato `[INFO]`/`[WARN]`/`[ERROR]` (y `[DATA] {json}` para payloads interactivos que consume el frontend). Se disparan individualmente desde `app/routers/api/pipeline.py`, ya sea vía SSE directo (`GET /run/{stage}`) o como tarea en background (`POST /run/{stage}` + `PipelineTaskManager` en `app/services/task_manager.py`, que gestiona streaming con cursor/offset para reconexión SSE y evicción de logs viejos). Ver `docs/DOMAIN.md` para la lista completa de etapas.

**Autenticación**: toda la app (páginas, API, static) está protegida por HTTP Basic Auth de contraseña única compartida vía middleware ASGI en `app/security.py` (`ApiKeyAuthMiddleware`, registrado en `app/main.py`), excepto `/health` y `/health/db`. El username es fijo (`admin`, hardcodeado en `app/security.py`) y la contraseña viene de la variable `APP_PASSWORD`. No hay usuarios ni roles.

**Seguridad de rutas de archivo**: cualquier endpoint que reciba una ruta relativa del cliente (explorer, pipeline, invoices) debe validarla con `app/paths.py::safe_resolve`/`safe_join` antes de tocar el filesystem — resuelven la ruta y verifican que quede dentro del directorio base, lanzando `ValueError` si escapa (los routers la traducen a `HTTPException(400, ...)`). No reimplementar esta validación inline.

**Modo dev vs producción** (dos stacks Docker independientes, pueden correr en paralelo sin conflicto — nombres de proyecto y puertos distintos): `docker-compose.yml` + `override.yml` (+ `dev-backend.yml` opcional) para desarrollo vía `dev.sh`; `docker-compose.prod.yml` (db + backend + nginx) vía `deploy.sh`. Nunca mezclar los scripts entre entornos.

**Alembic**: los modelos en `app/models/__init__.py` deben importarse todos ahí para que `alembic revision --autogenerate` los detecte. Además de constraints normales, hay triggers de Postgres (creados vía `op.execute` en migraciones, no vía SQLAlchemy) que sincronizan `Invoice.folder_status_id` según los hallazgos (`missing_files`) abiertos — ver la migración `initial_schema` (la única en `alembic/versions/` — el historial se consolidó en una sola). Si vas a tocar transiciones de estado de factura, revisá esos triggers primero; un `UPDATE` en Python puede ser silenciosamente corregido por ellos (los repos que lo hacen ya loguean un warning cuando eso pasa, ver `app/repositories/invoice_repo.py`).
