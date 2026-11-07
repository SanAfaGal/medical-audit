# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git commits y pushing
Nunca crees un commit ni hagas push a remoto a menos que el usuario lo solicite explícitamente en el mensaje actual. No hagas auto-commit tras completar tareas. Siempre espera a que el usuario revise los cambios y lo pida.

## Documentación del proyecto

El README.md es solo orientación rápida (stack, arquitectura, setup). El detalle vive en:
- `docs/DEVELOPMENT.md` — comandos de desarrollo, migraciones, backups locales, testing, variables de entorno
- `docs/DEPLOY.md` — runbook completo de producción (comandos `docker compose -f docker-compose.yml`, rollback, backups, firewall)
- `docs/MIGRATION.md` — migrar el stack completo (DB + volumen de PDFs) a otra máquina
- `docs/API.md` — referencia completa de endpoints REST
- `docs/DOMAIN.md` — modelo de dominio (entidades, estados) y las 24 etapas del pipeline

## Comandos

Entorno gestionado con `uv`. Backend nativo corre en Windows; solo Postgres está en Docker por defecto (ver `docs/DEVELOPMENT.md` para correr también el backend en Docker).

```bash
docker compose up -d db adminer   # Levantar Postgres (Docker)
uv run alembic upgrade head   # Aplicar migraciones Alembic pendientes
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload   # Uvicorn con hot-reload en 0.0.0.0:8000
uv run pytest   # pytest completo
uv run pytest -k test_invoice -v   # Un test específico por nombre
uv run pytest -m "not db and not slow"   # Solo tests unitarios rápidos (sin Postgres)
uv run pytest tests/app/core/test_scanner.py   # Un archivo específico
uv run ruff check . && uv run ruff format --check .   # ruff check + format check
uv run ruff format .   # Auto-formatear con ruff
uv run mypy app/                         # Type checking
```

Pytest markers (`pyproject.toml`): `db` (requiere Postgres vivo), `slow` (archivos grandes), `pdf` (requiere fixtures PDF reales). CI corre `-m db` con `--cov-fail-under=60` sobre `app/`.

**Gotcha conocido:** el fixture compartido de `-m db` (`tests/app/repos/conftest.py::test_engine`, session-scoped) falla en algunos entornos locales de Windows con `InterfaceError: cannot perform operation: another operation is in progress` — un problema de versión asyncpg/event-loop, no del código. Si un test `-m db` nuevo falla ahí durante desarrollo, verificar con un script standalone (un solo event loop de principio a fin, sin el fixture compartido) antes de asumir que el fix está mal; el test en sí es válido y corre bien en CI.

## Arquitectura

Capas con dirección de dependencia estricta: `routers → services → repositories → models`. Los routers instancian repos directamente vía `Depends(get_db)`; los repos reciben `AsyncSession` y no hacen commit salvo que el método sea explícitamente transaccional — el commit final es responsabilidad del router.

**`app/core/` es una librería framework-agnostic** — no importa nada del resto de `app/` (ni FastAPI ni SQLAlchemy); ruff lo hace cumplir con `TID251` (`banned-api` en `pyproject.toml`). Contiene el procesamiento de documentos puro: escaneo (`scanner.py`), extracción de texto (`reader.py`), compresión Ghostscript (`processor.py`), validación (`validator.py`, `inspector.py`), normalización de nombres (`standardizer.py`, `organizer.py`, `ops.py`), y las integraciones externas (`downloader.py` vía Playwright para SIHOS, `drive.py` para Google Drive). Solo `app/services/pipeline/`, `app/services/archive.py` y `app/routers/api/pipeline.py` importan `app/core/` — si necesitás tocar lógica de dominio, primero mirá si ya vive ahí antes de agregarla fuera de él.

**Pipeline de auditoría** (paquete `app/services/pipeline/`): registro de 24 etapas vía decorador `@stage("NOMBRE")` (`registry.py`), repartidas por fase en `stages/` (`ingest`, `staging`, `downloads`, `verification`, `packaging`); cada handler recibe un `StageContext` tipado (`context.py`) y `runner.execute` lo despacha, cada una un async generator que hace `yield` de líneas de log con formato `[INFO]`/`[WARN]`/`[ERROR]` (y `[DATA] {json}` para payloads interactivos que consume el frontend). Se disparan individualmente desde `app/routers/api/pipeline.py`, ya sea vía SSE directo (`GET /run/{stage}`) o como tarea en background (`POST /run/{stage}` + `PipelineTaskManager` en `app/services/task_manager.py`, que gestiona streaming con cursor/offset para reconexión SSE y evicción de logs viejos). Ver `docs/DOMAIN.md` para la lista completa de etapas. El reporte de progreso (líneas, barra N/total, latido, cancelación) está centralizado en `app/services/stage_events.py`: no lo reimplementes por etapa.

**Frontend compartido**: `app/static/js/api.js` (`apiErrorMessage`, `api.*`) y `app/static/js/toast.js` (store único `Alpine.store('toast')`, markup en `templates/partials/toasts.html`). Los errores de la API pueden venir como lista (422): pasa siempre `err.detail` por `apiErrorMessage`. Tests JS: `node --test tests/js/shared.test.mjs`.

**Autenticación**: la app no tiene autenticación (se eliminó el HTTP Basic Auth compartido). Cualquiera que alcance el puerto accede a todo, por lo que debe desplegarse solo en una red de confianza. No asumir usuarios, roles ni sesión en ningún endpoint.

**Seguridad de rutas de archivo**: cualquier endpoint que reciba una ruta relativa del cliente (explorer, pipeline, invoices) debe validarla con `app/paths.py::safe_resolve(base, target)` antes de tocar el filesystem — resuelven la ruta y verifican que quede dentro del directorio base, lanzando `ValueError` si escapa (los routers la traducen a `HTTPException(400, ...)`). No reimplementar esta validación inline.

**Modo dev vs producción** (dos stacks Docker independientes, pueden correr en paralelo sin conflicto — nombres de proyecto y puertos distintos): `docker-compose.yml` (base de producción: db + backend) con `-f` explícito (siempre en producción); `docker-compose.override.yml` (sobreescrituras dev: puerto de db, Adminer, backend con hot-reload) se aplica automáticamente solo con `docker compose` sin `-f`. Nunca correr `docker compose` sin `-f` en producción.

**Alembic**: los modelos en `app/models/__init__.py` deben importarse todos ahí para que `alembic revision --autogenerate` los detecte. Además de constraints normales, hay triggers de Postgres (creados vía `op.execute` en migraciones, no vía SQLAlchemy) que sincronizan `Invoice.folder_status_id` según los hallazgos (`missing_files`) abiertos — ver la migración `initial_schema` (la única en `alembic/versions/` — el historial se consolidó en una sola). Si vas a tocar transiciones de estado de factura, revisá esos triggers primero; un `UPDATE` en Python puede ser silenciosamente corregido por ellos (los repos que lo hacen ya loguean un warning cuando eso pasa, ver `app/repositories/invoice_repo.py`).
