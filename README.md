# Medical Audit

Aplicación web de producción para automatizar la auditoría de documentos de facturación médica en instituciones de salud colombianas. Ingesta facturas desde SIHOS, valida carpetas físicas de documentos contra las reglas de documentación requerida por tipo de servicio, y lleva el estado de auditoría por periodo de facturación.

---

## Tabla de contenidos

- [Stack tecnológico](#stack-tecnológico)
- [Arquitectura](#arquitectura)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Desarrollo local](#desarrollo-local)
- [Despliegue a producción](#despliegue-a-producción)
- [Documentación adicional](#documentación-adicional)
- [Seguridad](#seguridad)
- [Monitoreo](#monitoreo)

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.11+ |
| Framework web | FastAPI (async) |
| Base de datos | PostgreSQL 16 |
| ORM / Migraciones | SQLAlchemy 2.0 (asyncio) + Alembic |
| Frontend | Jinja2 (templates server-side) |
| Servidor de app | Uvicorn (2 workers, nativo en host) |
| Contenedores | Docker + Docker Compose |
| PDF | PyMuPDF, pdfplumber, Ghostscript |
| Automatización de browser | Playwright (descarga de facturas SIHOS) |
| Almacenamiento en nube | Google Drive API |
| Procesamiento de datos | pandas, openpyxl |
| Logs | structlog (estructurado) |
| Seguridad | Sin autenticación (red de confianza) + cryptography para credenciales almacenadas |
| Gestor de paquetes | uv |
| Testing | pytest + pytest-asyncio |

---

## Arquitectura

```
  Dispositivos LAN ──── :8000 ──► Uvicorn / FastAPI  (nativo en Windows)
                                          │
                          ┌───────────────┼───────────────┐
                          │               │               │
                   sistema de         Google         localhost:5432
                   archivos           Drive API           │
                   Windows                         ┌─────────────┐
                   (DRIVE/STAGE/AUDIT)              │   Docker    │
                                                    │ PostgreSQL  │
                                                    └─────────────┘
```

- **Backend** (FastAPI + Uvicorn) corre de forma nativa en el host Windows. Accede a los archivos de auditoría directamente desde el sistema de archivos local sin overhead de virtualización.
- **Base de datos** (PostgreSQL 16) corre en Docker con el puerto 5432 expuesto a `localhost`. Solo la base de datos está contenedorizada.
- **Logos de instituciones** se almacenan en la base de datos como `BYTEA` y se sirven vía `GET /api/institutions/{id}/logo` — sin volúmenes compartidos.
- El backend escucha en `0.0.0.0:8000`, por lo que cualquier dispositivo en la misma red LAN puede acceder a la app (**la app no tiene autenticación** — ver [Seguridad](#seguridad)).

---

## Estructura del proyecto

```
medical-audit-v2/
├── app/
│   ├── main.py                    # Fábrica de FastAPI, lifespan, middleware, registro de routers
│   ├── config.py                  # Settings (pydantic-settings, lee desde .env)
│   ├── database.py                # Session factory async de SQLAlchemy
│   ├── crypto.py                  # Cifrado de credenciales almacenadas (SIHOS, Drive)
│   ├── paths.py                   # Conversión de rutas Windows ↔ contenedor + helpers de sandbox
│   ├── models/
│   │   ├── base.py
│   │   ├── institution.py         # Institution, Administrator, Contract, Agreement, Service
│   │   ├── invoice.py             # Invoice
│   │   ├── period.py              # AuditPeriod
│   │   ├── finding.py             # MissingFile (hallazgos de auditoría)
│   │   ├── rules.py               # ServiceType, DocType, FolderStatus, PrefixCorrection, SystemSettings
│   │   └── __init__.py            # Importa todos los modelos para autogenerate de Alembic
│   ├── repositories/              # Capa de acceso a datos
│   │   ├── institution_repo.py
│   │   ├── invoice_repo.py
│   │   ├── finding_repo.py
│   │   └── rules_repo.py
│   ├── routers/
│   │   ├── pages.py               # Rutas Jinja2 (/, /audit, /settings)
│   │   └── api/
│   │       ├── institutions.py    # /api/institutions
│   │       ├── periods.py         # /api/institutions/{id}/periods
│   │       ├── invoices.py        # /api/invoices
│   │       ├── findings.py        # /api/missing-files
│   │       ├── pipeline.py        # /api/pipeline (SSE streaming + task manager)
│   │       ├── settings.py        # /api/settings
│   │       └── explorer.py        # /api/explorer (explorador de archivos)
│   ├── services/
│   │   ├── billing.py             # Ingesta de Excel SIHOS + normalización
│   │   ├── pipeline_runner.py     # Pipeline de 22 etapas (async generator)
│   │   └── task_manager.py        # Gestión de tareas pipeline en background
│   ├── core/                      # Lógica de dominio y procesamiento de documentos (sin dependencias de app/)
│   │   ├── scanner.py                 # Descubrimiento de archivos (glob/regex)
│   │   ├── reader.py                  # Extracción de texto PDF (PyMuPDF + pdfplumber)
│   │   ├── processor.py               # Compresión de PDFs con Ghostscript
│   │   ├── validator.py                # Validación de facturas (CUFE, código de factura)
│   │   ├── inspector.py                # Validación de estructura de carpetas
│   │   ├── organizer.py                # Operaciones de mover y renombrar archivos/carpetas
│   │   ├── standardizer.py             # Normalización de nombres de archivo
│   │   ├── downloader.py                # Descarga de facturas SIHOS vía Playwright
│   │   ├── drive.py                    # Sincronización con Google Drive
│   │   └── ops.py                      # Utilidades de operaciones de archivo
│   ├── schemas/                   # Modelos Pydantic de request/response
│   ├── static/                    # CSS y assets estáticos
│   └── templates/                 # Templates Jinja2 HTML
├── alembic/
│   └── versions/                  # Migraciones auto-generadas desde los modelos
├── seeds/
│   └── seed_data.py               # Script de datos iniciales
├── tests/
│   ├── app/                       # Incluye app/core/
│   ├── load/
│   └── conftest.py                # Fixtures y markers de pytest
├── docs/
│   ├── DEPLOY.md                  # Guía de despliegue a producción
│   ├── DEVELOPMENT.md             # Comandos, migraciones, testing, variables de entorno
│   ├── API.md                     # Referencia de endpoints
│   └── DOMAIN.md                  # Modelo de dominio + etapas del pipeline
├── backups/                       # Snapshots de tablas de configuración (no en git)
├── docker-compose.yml             # Base de producción: db + backend
├── docker-compose.override.yml    # Dev: puerto db, Adminer, backend con hot-reload
├── Dockerfile                     # Multi-stage: builder + runtime
├── pyproject.toml                 # Dependencias, pytest, ruff, mypy
├── uv.lock                        # Dependencias congeladas (reproducible)
└── .env.example                   # Template de configuración
```

---

## Desarrollo local

### Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose v2
- [uv](https://github.com/astral-sh/uv) para el entorno Python local

### Setup local

**1. Clonar y configurar el entorno**

```bash
git clone <repo-url>
cd medical-audit-v2
cp .env.example .env
```

Edita `.env` con tus valores locales. El `SECRET_KEY` se genera así:

```bash
python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

**2. Iniciar la base de datos**

```bash
docker compose up -d db adminer
```

El `docker-compose.override.yml` se aplica automáticamente y levanta también Adminer en `http://localhost:8080`.

**3. Aplicar migraciones**

```bash
uv run alembic upgrade head
```

**4. Iniciar el backend**

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Uvicorn inicia con hot-reload en `0.0.0.0:8000`.

**5. Configurar la carpeta de auditoría**

La ruta base de auditoría se configura desde la interfaz: **Configuración → Sistema**.

**6. URLs de acceso**

| URL | Descripción |
|---|---|
| `http://localhost:8000` | Aplicación principal (sin login) |
| `http://<ip-lan>:8000` | Acceso desde otros dispositivos en la red |
| `http://localhost:8000/docs` | Swagger UI (solo si `DOCS_ENABLED=true`) |
| `http://localhost:8080` | Adminer — UI de base de datos (solo dev) |

### Backend en Docker (alternativa)

Por defecto el backend corre nativo (paso 4 arriba). Si prefieres correrlo también en Docker — para probar el pipeline con Playwright igual que en producción, o si no tienes `uv`/Python instalado — usa en su lugar:

```bash
docker compose up -d --build db adminer backend
docker compose exec -T backend uv run alembic upgrade head
docker compose exec -T backend uv run python seeds/seed_data.py
```

Levanta `db` + `backend` (con hot-reload vía bind mount de `app/` y `alembic/`) + Adminer, aplica migraciones y siembra datos iniciales. Reutiliza el mismo contenedor y volumen de PostgreSQL que el modo nativo — puedes alternar entre el backend nativo y el de Docker sin perder datos.

> Los datos de auditoría viven en el volumen Docker `audit_data` (`AUDIT_DATA_ROOT=/audit_data`, definido en el compose); no hay que configurarlo en `.env`.

```bash
docker compose logs -f backend   # ver logs / confirmar reload
docker compose exec -T backend uv run alembic upgrade head   # migrar sin uv local
docker compose exec -T backend uv run python seeds/seed_data.py   # sembrar datos sin uv local
docker compose down   # detener todo
```

> Comandos completos de desarrollo (testing, migraciones, backups) en **[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)**.

---

## Despliegue a producción

Stack separado (`docker-compose.yml`): dos contenedores (db + backend), aislado del entorno de desarrollo. Todos los comandos usan `-f docker-compose.yml` explícito (sin él, Compose aplicaría el override de desarrollo) — ver [docs/DEPLOY.md](docs/DEPLOY.md).

```bash
git clone <repo-url> && cd medical-audit-v2
cp .env.example .env   # editar con valores de producción
docker compose -f docker-compose.yml up --build -d   # build + up -d + migraciones + seed, en un solo paso
docker compose -f docker-compose.yml exec -T backend uv run alembic upgrade head
docker compose -f docker-compose.yml exec -T backend uv run python seeds/seed_data.py
```

### Dev vs. producción

| | Desarrollo | Producción |
|---|---|---|
| Compose file | `docker-compose.yml` + `docker-compose.override.yml` (auto) | `docker-compose.yml` (con `-f`, sin override) |
| Proyecto Docker | `medical-audit` (fijo, `name:` en el override) | `medical-audit-prod` (fijo, `name:` en el compose) |
| Qué corre en contenedores | PostgreSQL + Adminer (+ backend si lo corres en Docker) | db + backend |
| Backend | Nativo en el host por defecto, hot-reload (o en Docker con `docker compose up -d --build db adminer backend`) | Contenedorizado |
| Puerto de la app | `8000` (directo a Uvicorn) | `80` (mapeado a Uvicorn `8000`) |
| Puerto DB expuesto al host | `5432` | Ninguno (solo red interna Docker) |
| Volumen DB | `medical-audit_pgdata` | `medical-audit-prod_pgdata` |

Ambos stacks pueden correr al mismo tiempo sin conflicto (nombres de proyecto y puertos distintos).

> Guía completa de despliegue (requisitos de servidor, rollback, backups, firewall, automatización) en **[docs/DEPLOY.md](docs/DEPLOY.md)**.

---

## Documentación adicional

| Documento | Contenido |
|---|---|
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Variables de entorno, comandos de desarrollo, migraciones, backups locales, testing |
| [docs/DEPLOY.md](docs/DEPLOY.md) | Runbook de producción: requisitos, rollback, backups, firewall, automatización |
| [docs/MIGRATION.md](docs/MIGRATION.md) | Migrar el stack completo (DB + PDFs) a otra máquina |
| [docs/API.md](docs/API.md) | Referencia completa de endpoints REST |
| [docs/DOMAIN.md](docs/DOMAIN.md) | Modelo de dominio (entidades, estados) y las 22 etapas del pipeline |

---

## Seguridad

- **Sin autenticación** — la app no pide usuario ni contraseña: cualquiera que alcance el puerto accede a todo (datos de auditoría y administración de credenciales SIHOS/Drive). Exponerla solo en una red de confianza (LAN cerrada, VPN o firewall); no publicarla a internet.
- **`.env` nunca se commitea** — está en `.gitignore`. Usar `.env.example` como plantilla.
- **Variables requeridas son obligatorias** — `docker-compose.yml` usa sintaxis `:?` para `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `SECRET_KEY`. Si alguna falta, Docker Compose aborta con error explícito.
- **URL de SIHOS validada (anti-SSRF)** — `sihos_base_url` solo admite `http(s)` y rechaza loopback, enlaces locales y endpoints de metadatos (`app/core/urlcheck.py`); se revalida resolviendo DNS justo antes de abrir el navegador, y el navegador bloquea redirecciones a esas direcciones. Las IP privadas (LAN) siguen permitidas.
- **Archivos comprimidos acotados** — `load-drive-zip` limita el tamaño subido (1 GB) y descomprimido (5 GB), el número de elementos, rechaza rutas inseguras y enlaces simbólicos (`app/services/archive.py`).
- **Trabajo pesado fuera del event loop** — el backend corre con un solo worker, así que escaneos de disco, extracción y lectura de Excel se ejecutan con `asyncio.to_thread`; no bloquear el loop en endpoints `async`.
- **Secretos cifrados en reposo** — contraseñas SIHOS y credenciales de Google Drive se almacenan en DB cifradas vía `app/crypto.py`. La clave proviene de `SECRET_KEY`.
- **Logos almacenados en DB** — los logos de instituciones se guardan como `BYTEA` en PostgreSQL y se sirven vía API, sin volúmenes compartidos.
- **Contenedor sin root** — la imagen Docker corre como `appuser`, no como `root`.
- **Sin exposición directa de la DB** — PostgreSQL solo es accesible dentro de la red Docker.
- **Swagger deshabilitado por defecto** — `DOCS_ENABLED=false` en producción evita exponer la documentación de la API.
- **Sin rate limiting** — el backend se publica directo en el puerto 80 sin proxy; no exponer fuera de la red local.
- **Historial git limpio** — ninguna credencial ni secreto ha sido commiteado al repositorio.

---

## Monitoreo

### Logs estructurados (structlog)

Todos los logs incluyen campos de contexto (`method`, `path`, `status`, `latency_ms`). Nivel configurable vía `LOG_LEVEL`. Las rutas `/health`, `/health/db` y `/static` están excluidas del logging de requests.

### Health checks

| Endpoint | Descripción |
|---|---|
| `GET /health` | Health básico (siempre 200 si el proceso está corriendo) |
| `GET /health/db` | Health con verificación de conexión a DB (503 si la DB no está disponible) |

> Para el despliegue a producción, ver **[docs/DEPLOY.md](docs/DEPLOY.md)**.
